#include "verneuil.h"
#include "vfs.h"

#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

SQLITE_EXTENSION_INIT1

/**
 * The Verneuil VFS layer is based on the Linux VFS, with Rust hooks
 * around file read, write, and locking operations.
 *
 * Unlike the Linux VFS, this VFS accidentally passes
 * `unixexcl-1.2.4.singleproc`, but fails `unixexcl-1.1.4.multiproc`:
 * the `sqlite3PendingByte` offset is only available when built with
 * the amalgamated source, so we can't use the test-only lock range.
 */

/* Temporary directories must have some room left for filenames. */
#define LINUX_VFS_TEMPPATH_MAX (PATH_MAX - 200)

#ifndef SQLITE_DEFAULT_FILE_PERMISSIONS
# define SQLITE_DEFAULT_FILE_PERMISSIONS 0644
#endif

/*
 * Avoid using file descriptors lower than this value: we don't want
 * writes to what should be stdout or stderr to stomp over our data.
 */
#ifndef SQLITE_MINIMUM_FILE_DESCRIPTOR
# define SQLITE_MINIMUM_FILE_DESCRIPTOR 3
#endif

#ifndef SQLITE_TEMP_FILE_PREFIX
# define SQLITE_TEMP_FILE_PREFIX "etilqs_"
#endif

/**
 * Some older distributions fail to expose these constants.  Hardcode
 * them if necessary: open file description locks have been around
 * since Linux 3.15, and we don't try to support anything that old
 * (getrandom was introduced in 3.17, and we also use that).
 */
#ifndef F_OFD_GETLK
#define F_OFD_GETLK     36
#define F_OFD_SETLK     37
#define F_OFD_SETLKW    38
#endif

#if (F_OFD_GETLK != 36) || (F_OFD_SETLK != 37) || (F_OFD_SETLKW != 38)
# error "Mismatch in fallback OFD fcntl constants."
#endif

#ifdef SQLITE_TEST
/*
 * Define these test-only counters in BSS; when the sqlite test driver
 * also has a definition for these, they will be merged at link-time.
 */
int sqlite3_sync_count;
int sqlite3_fullsync_count;
int sqlite3_open_file_count;
int sqlite3_current_time;
#endif

struct linux_file {
        sqlite3_file base;
        /*
         * The descriptor for this file object.  The underlying kernel
         * file object must be unique owned by this file struct (i.e.,
         * can't dup this).
         */
        int fd;

        /*
         * Current lock level for this file object on the underlying
         * inode.  Matches the level parameters of xLock / xUnlock:
         *
         * SQLITE_LOCK_NONE       0
         * SQLITE_LOCK_SHARED     1
         * SQLITE_LOCK_RESERVED   2
         * SQLITE_LOCK_PENDING    3
         * SQLITE_LOCK_EXCLUSIVE  4
         */
        int lock_level;

        /*
         * NULLable.  Only NULL for temporary files, otherwise this is
         * an absolute path to the file.
         */
        const char *path;

        /*
         * This metadata identifies the inode that `fd` refers to.
         */
        dev_t device;
        ino_t inode;

        /*
         * Wait up to this many milliseconds for each lock
         * acquisition.
         */
        uint32_t lock_timeout_ms;

        /*
         * sqlite expects us to fsync the parent directory along with
         * this (journal) file, in order to guarantee its visibility.
         *
         * We don't actually do that, but we still want to fake it for
         * tests.
         */
        bool dirsync_pending;
};

static_assert(sizeof(struct sqlite3_file) == sizeof(void *),
    "vfs_ops.rs assumes sqlite3_file consists of one vtable pointer.");

static_assert(sizeof(dev_t) == sizeof(uint64_t),
    "vfs_ops.rs assumes dev_t as a u64.");

static_assert(sizeof(ino_t) == sizeof(uint64_t),
    "vfs_ops.rs assumes ino_t as a u64.");

static_assert(SQLITE_LOCK_NONE == 0, "vfs_ops.rs assumes NONE == 0");
static_assert(SQLITE_LOCK_SHARED == 1, "vfs_ops.rs assumes SHARED == 1");
static_assert(SQLITE_LOCK_RESERVED == 2, "vfs_ops.rs assumes RESERVED == 2");
static_assert(SQLITE_LOCK_PENDING == 3, "vfs_ops.rs assumes PENDING == 3");
static_assert(SQLITE_LOCK_EXCLUSIVE == 4, "vfs_ops.rs assumes EXCLUSIVE == 4");

typedef void dlfun_t(void);

static int linux_open(sqlite3_vfs *, const char *name, sqlite3_file *,
    int flags, int *OUT_flags);
static int linux_delete(sqlite3_vfs *, const char *name, int syncDir);
static int linux_access(sqlite3_vfs *, const char *name, int flags, int *OUT_res);
static int linux_full_pathname(sqlite3_vfs *, const char *name, int n, char *dst);

static void *linux_dlopen(sqlite3_vfs *, const char *name);
static void linux_dlerror(sqlite3_vfs *, int n, char *OUT_error);
static dlfun_t *linux_dlsym(sqlite3_vfs *, void *, const char *symbol);
static void linux_dlclose(sqlite3_vfs *, void *);

static int linux_randomness(sqlite3_vfs *, int n, char *dst);

static int linux_sleep(sqlite3_vfs *, int microseconds);

static int linux_get_last_error(sqlite3_vfs *, int n, char *OUT_error);

static int linux_current_time_int64(sqlite3_vfs *, sqlite3_int64 *);

static int linux_set_syscall(sqlite3_vfs *, const char *, sqlite3_syscall_ptr);
static sqlite3_syscall_ptr linux_get_syscall(sqlite3_vfs *, const char *);
static const char *linux_next_syscall(sqlite3_vfs *, const char *);

static int linux_file_check_reserved_lock(sqlite3_file *, int *OUT_result);

static int linux_file_control(sqlite3_file *, int op, void *arg);
static int linux_file_sector_size(sqlite3_file *);
static int linux_file_device_characteristics(sqlite3_file *);

/*
 * The directory for temporary files.  It is lazily computed once,
 * and then cached.  The first value published to this variable
 * is sticky: all writes must compare-and-swap with NULL.
 */
static const char *_Atomic linux_vfs_tempdir = NULL;

/*
 * We use this vtable of IO methods for files that do not require
 * write tracking for replication, i.e., everything but main DB files.
 */
static const struct sqlite3_io_methods verneuil_io_methods = {
        .iVersion = 1,  /* No WAL or mmap method */
        .xClose = verneuil__file_close_impl,

        .xRead = verneuil__file_read_impl,
        .xWrite = verneuil__file_write_impl,
        .xTruncate = verneuil__file_truncate_impl,
        .xSync = verneuil__file_sync_impl,

        .xFileSize = verneuil__file_size_impl,

        .xLock = verneuil__file_lock_impl,
        .xUnlock = verneuil__file_unlock_impl,
        .xCheckReservedLock = linux_file_check_reserved_lock,

        .xFileControl = linux_file_control,
        .xSectorSize = linux_file_sector_size,
        .xDeviceCharacteristics = linux_file_device_characteristics,
};

/*
 * We use this vtable for main DB files, to let Rust code intercept
 * I/O.
 */
static const struct sqlite3_io_methods verneuil_intercept_io_methods = {
        .iVersion = 1,  /* No WAL or mmap method */
        .xClose = verneuil__file_close,

        .xRead = verneuil__file_read,
        .xWrite = verneuil__file_write,
        .xTruncate = verneuil__file_truncate,
        .xSync = verneuil__file_sync,

        .xFileSize = verneuil__file_size,

        .xLock = verneuil__file_lock,
        .xUnlock = verneuil__file_unlock,
        .xCheckReservedLock = linux_file_check_reserved_lock,

        .xFileControl = linux_file_control,
        .xSectorSize = linux_file_sector_size,
        .xDeviceCharacteristics = linux_file_device_characteristics,
};

static sqlite3_vfs verneuil_vfs = {
        .iVersion = 3,
        .szOsFile = sizeof(struct linux_file),
        .mxPathname = PATH_MAX,
        .zName = "verneuil",
        .xOpen = linux_open,
        .xDelete = linux_delete,
        .xAccess = linux_access,

        .xFullPathname = linux_full_pathname,

        .xDlOpen = linux_dlopen,
        .xDlError = linux_dlerror,
        .xDlSym = linux_dlsym,
        .xDlClose = linux_dlclose,

        .xRandomness = linux_randomness,

        .xSleep = linux_sleep,
        /* CurrentTime isn't used when CurrentTimeInt64 is available. */

        .xGetLastError = linux_get_last_error,

        .xCurrentTimeInt64 = linux_current_time_int64,

        /*
         * Parts of the test suite requires these methods to exist,
         * although they don't have to actually do anything.
         */
        .xSetSystemCall = linux_set_syscall,
        .xGetSystemCall = linux_get_syscall,
        .xNextSystemCall = linux_next_syscall,
};

/**
 * Is `dir` currently a valid temporary directory?
 */
static bool
is_valid_tempdir(const char *dir)
{
        struct stat sb;
        int r;

        /* Is the path too long? */
        if (strnlen(dir, LINUX_VFS_TEMPPATH_MAX) >= LINUX_VFS_TEMPPATH_MAX)
                return false;

        /* Is it a directory? */
        do {
                r = stat(dir, &sb);
        } while (r != 0 && errno == EINTR);

        if (r != 0 || S_ISDIR(sb.st_mode) == 0)
                return false;

        /* Do we have read and write access to it? */
        do {
                r = access(dir, W_OK | X_OK);
        } while (r != 0 && errno == EINTR);

        return r == 0;
}

static int
verneuil_set_tempdir(const char *dir)
{
        static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        /* Reserve room for the file name itself. */
        static char tempdir_copy[LINUX_VFS_TEMPPATH_MAX];
        const char *expected;
        int r;
        bool success = false;

        if (is_valid_tempdir(dir) == false)
                return SQLITE_MISUSE;

        r = pthread_mutex_lock(&lock);
        if (r != 0)
                return SQLITE_INTERNAL;

        /* tempdir already computed. */
        if (atomic_load(&linux_vfs_tempdir) != NULL)
                goto out;

        /*
         * This strncpy is safe because `tempdir_copy` must be empty:
         * no other thread may be in the current critical section, and
         * we ensure `linux_vfs_tempdir` is non-NULL before releasing
         * the lock.  If we get here, it's the first call to
         * `sqlite3_linux_vfs_set_tempdir`.
         */
        strncpy(tempdir_copy, dir, sizeof(tempdir_copy));
        tempdir_copy[sizeof(tempdir_copy) - 1] = '\0';

        expected = NULL;
        success = atomic_compare_exchange_strong(&linux_vfs_tempdir,
            &expected, tempdir_copy);

out:
        r = pthread_mutex_unlock(&lock);
        assert(r == 0 && "pthread_mutex_unlock failed");
        return (success == true) ? SQLITE_OK : SQLITE_LOCKED;
}

static const char *
compute_tempdir(void)
{
        const char *dirs[] = {
                /* Use the same priority as the Unix VFS. */
                sqlite3_temp_directory,
                /*
                 * We call `getenv` the first time we compute the
                 * temporary directory, and then cache the result.
                 * That's not safe in there are calls to `setenv`, but
                 * there's essentially nothing we can do to fix that;
                 * the environment should be considered static.
                 *
                 * POSIX also allows the implementation to reuse a
                 * mutable buffer for the return value of `getenv`.
                 * We assume no libc is that broken.
                 */
                NULL,
                NULL,
                "/var/tmp",
                "/usr/tmp",
                "/tmp",
                ".",
        };

#ifdef _GNU_SOURCE
        /*
         * When GNU extensions are available, use secure_getenv to
         * ignore the environment in suid binaries: we don't want
         * untrusted user to control where our writes go.
         */
        dirs[1] = secure_getenv("SQLITE_TMPDIR");
        dirs[2] = secure_getenv("TMPDIR");
#else
        dirs[1] = getenv("SQLITE_TMPDIR");
        dirs[2] = getenv("TMPDIR");
#endif

        for (size_t i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
                const char *dir = dirs[i];

                if (dir != NULL && is_valid_tempdir(dir))
                        return dir;
        }

        /* If nothing works, default to "/tmp" */
        return "/tmp";
}

static const char *
get_tempdir_base(void)
{
        const char *copy;
        const char *computed;

        copy = atomic_load_explicit(&linux_vfs_tempdir, memory_order_relaxed);
        if (copy != NULL)
                return copy;

        computed = compute_tempdir();
        if (atomic_compare_exchange_strong(&linux_vfs_tempdir, &copy,
            computed) == true)
                return computed;

        return copy;
}

/**
 * Wraps open(2) to retry on EINTR and avoid returning file
 * descriptors below `SQLITE_MINIMUM_FILE_DESCRIPTOR` (usually 3,
 * to avoid stdin/stdout/stderr).
 */
static int
linux_safe_open(const char *path, int flags, mode_t mode)
{
        int dummy, err, fd, r;

        do {
                fd = open(path, flags, mode);
        } while (fd < 0 && errno == EINTR);

        if (fd < 0)
                return fd;

        if (fd >= SQLITE_MINIMUM_FILE_DESCRIPTOR)
                return fd;

        /*
         * This FD is too low.  Open `/dev/null` and dup3 in place.
         */
        do {
                dummy = open("/dev/null", O_RDONLY | O_CLOEXEC);
        } while (dummy < 0 && errno == EINTR);

        if (dummy < 0)
                return dummy;

        /* Copy `dummy` to `0 <= fd < SQLITE_MINIMUM_FILE_DESCRIPTOR`. */
        do {
                r = dup3(dummy, fd, O_CLOEXEC);
        } while (r < 0 && errno == EINTR);

        /*
         * Regardless of what happened to `dup3`, we want to get rid
         * of `dummy` if it's a file descriptor number we can use.
         */
        if (dummy >= SQLITE_MINIMUM_FILE_DESCRIPTOR) {
                err = errno;
                close(dummy);
                errno = err;
        }

        if (r < 0)
                return r;

        /*
         * We made some progress and populated at least one fd below
         * SQLITE_MINIMUM_FILE_DESCRIPTOR.  Try again.
         */
        return linux_safe_open(path, flags, mode);
}

static bool
linux_path_exists(const char *path)
{
        int r;

        do {
                r = access(path, F_OK);
        } while (r != 0 && errno == EINTR);

        return r == 0;
}

static int
linux_open(sqlite3_vfs *vfs, const char *name, sqlite3_file *vfile,
    int flags, int *OUT_flags)
{
        struct linux_file *file = (void *)vfile;
        const struct sqlite3_io_methods *io_methods;
        const int journal_mask =
            SQLITE_OPEN_SUPER_JOURNAL | SQLITE_OPEN_MAIN_JOURNAL | SQLITE_OPEN_WAL;
        int open_flags = O_CLOEXEC | O_LARGEFILE | O_NOFOLLOW;
        int fd, rc;
        const bool is_uri = (flags & SQLITE_OPEN_URI) != 0;

        (void)vfs;
        /*
         * Intercept IO calls on main DB files.  These DB files should
         * have a name and be persistent, but if we are ever asked to
         * open such temporary main DBs, don't intercept their IO.
         *
         * When locking is disabled we can't tell what's a good time
         * to snapshot; there's also no change to track for immutable
         * files.  In both cases, we don't want to intercept IO.
         */
        if ((flags & SQLITE_OPEN_MAIN_DB) != 0 &&
            name != NULL &&
            (flags & SQLITE_OPEN_DELETEONCLOSE) == 0 &&
            sqlite3_uri_boolean(is_uri ? name : NULL, "nolock", 0) == 0 &&
            sqlite3_uri_boolean(is_uri ? name : NULL, "immutable", 0) == 0) {
                io_methods = &verneuil_intercept_io_methods;
        } else {
                io_methods = &verneuil_io_methods;
        }

        /*
         * It is safe to borrow `name` for the lifetime of the `file`.
         * https://www.sqlite.org/c3ref/vfs.html says
         *
         *   SQLite further guarantees that the string will be valid
         *   and unchanged until xClose() is called. Because of the
         *   previous sentence, the sqlite3_file can safely store a
         *   pointer to the filename if it needs to remember the
         *   filename for some reason.
         */
        *file = (struct linux_file) {
                .base.pMethods = io_methods,
                .fd = -1,
                .path = name,
                /*
                 * If we're creating a new journal file, test code
                 * expects the parent directory to be fsynced the
                 * first time the journal itself is synced.  We don't
                 * do that, but we do want to increment the sync
                 * counter in tests.
                 */
                .dirsync_pending = (flags & SQLITE_OPEN_CREATE) != 0 &&
                    (flags & journal_mask) != 0,
        };

        if ((flags & SQLITE_OPEN_READONLY) != 0)
                open_flags |= O_RDONLY;

        if ((flags & SQLITE_OPEN_READWRITE) != 0)
                open_flags |= O_RDWR;

        /*
         * name == NULL means a temporary file, and implies
         * delete-on-close. The default unix VFS implements that flag
         * latter as open + unlink; let's skip that intermediate step
         * and get an `O_TMPFILE` in `get_tempdir_base()`.
         */
        if (name == NULL || (flags & SQLITE_OPEN_DELETEONCLOSE) != 0) {
                file->path = NULL;
                fd = linux_safe_open(get_tempdir_base(),
                     O_TMPFILE | O_EXCL | open_flags, 0600);
                if (fd < 0 && errno == EACCES) {
                        rc = SQLITE_READONLY_DIRECTORY;
                        goto fail;
                }
        } else {
                if ((flags & SQLITE_OPEN_CREATE) != 0)
                        open_flags |= O_CREAT;

                if ((flags & SQLITE_OPEN_EXCLUSIVE) != 0)
                        open_flags |= O_EXCL;

                /*
                 * TODO: we should ideally inherit the permission and
                 * ownership of the main DB when opening journals.
                 *
                 * In most cases however, it's fine to just use the
                 * the default (0644), and let umask do its thing.
                 */
                fd = linux_safe_open(name, open_flags,
                    SQLITE_DEFAULT_FILE_PERMISSIONS);

                /* Bail early if we found a directory. */
                if (fd < 0 && errno == EISDIR) {
                        rc = SQLITE_CANTOPEN;
                        goto fail;
                }

                /*
                 * If we failed to create a new file that does not
                 * already exist, report a read-only parent directory.
                 */
                if (fd < 0 &&
                    (flags & SQLITE_OPEN_CREATE) != 0 &&
                    errno == EACCES &&
                    linux_path_exists(name) == false) {
                        rc = SQLITE_READONLY_DIRECTORY;
                        goto fail;
                }

                /*
                 * Try again in read-only mode.
                 */
                if (fd < 0 && (flags & SQLITE_OPEN_READWRITE) != 0) {
                        flags &= ~(SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
                        flags |= SQLITE_OPEN_READONLY;

                        open_flags &= ~(O_RDWR | O_CREAT);
                        open_flags |= O_RDONLY;
                        fd = linux_safe_open(name, open_flags, 0);
                }
        }

        if (fd < 0) {
                rc = SQLITE_CANTOPEN;
                goto fail;
        }

#ifdef SQLITE_TEST
        extern int sqlite3_open_file_count;

        sqlite3_open_file_count++;
#endif

        file->fd = fd;

        /*
         * fstat the file to remember its current identity.
         */
        if (name != NULL) {
                struct stat sb;
                int r;

                do {
                        r = fstat(fd, &sb);
                } while (r != 0 && errno == EINTR);

                if (r != 0) {
                        verneuil__file_close_impl(vfile);
                        return SQLITE_IOERR_FSTAT;
                }

                file->device = sb.st_dev;
                file->inode = sb.st_ino;
        }

        if (OUT_flags != NULL)
                *OUT_flags = flags;
        return SQLITE_OK;

fail:
        *file = (struct linux_file) { .fd = -1 };
        return rc;
}

static int
linux_delete(sqlite3_vfs *vfs, const char *name, int sync_dir)
{
        int r;

        (void)vfs;
        /*
         * We never fsync the parent directory (i.e., assume
         * `SQLITE_DISABLE_DIRSYNC` is always set).  In the worst
         * case, this means a rollback journal could remain visible if
         * the OS crashes soon after a transaction commit.  If that
         * happens, that last transaction may be lost.  However, while
         * we take a hit on durability, the db will always be valid.
         *
         * We're willing to take that risk, for simpler code.
         */
        (void)sync_dir;

#ifndef SQLITE_DISABLE_DIRSYNC
# warning "The Linux VFS assumes DIRSYNC is disabled."
#endif

        do {
                r = unlink(name);
        } while (r != 0 && errno == EINTR);

        if (r != 0) {
                if (errno == ENOENT)
                        return SQLITE_IOERR_DELETE_NOENT;

                return SQLITE_IOERR_DELETE;
        }

#ifdef SQLITE_TEST
        /*
         * Some tests assert on the number of sync calls.  Update that
         * counter as expected, even if we don't actually fsync.
         */
        if ((sync_dir & 1) != 0) {
                extern int sqlite3_sync_count;

                sqlite3_sync_count++;
        }
#endif

        return SQLITE_OK;
}

static int
linux_access(sqlite3_vfs *vfs, const char *name, int flags, int *OUT_res)
{
        int access_mode = F_OK;
        int r;

        (void)vfs;

        /*
         * The Unix VFS says a file exists if it's not a regular file,
         * or it's non-empty... Replicate that logic here.
         */
        if (flags == SQLITE_ACCESS_EXISTS) {
                struct stat sb;

                do {
                        r = stat(name, &sb);
                } while (r != 0 && errno == EINTR);

                /* If we can't stat the file, assume it doesn't exist. */
                if (r != 0) {
                        *OUT_res = 0;
                        return SQLITE_OK;
                }

                if (S_ISREG(sb.st_mode) != 0) {
                        *OUT_res = 1;
                        return SQLITE_OK;
                }

                *OUT_res = (sb.st_size > 0) ? 1 : 0;
                return SQLITE_OK;
        }

        if (flags == SQLITE_ACCESS_READWRITE) {
                access_mode = W_OK | R_OK;
        } else  if (flags == SQLITE_ACCESS_READ) {
                access_mode = R_OK;
        }

        do {
                r = access(name, access_mode);
        } while (r != 0 && errno == EINTR);

        *OUT_res = (r == 0) ? 1 : 0;
        return SQLITE_OK;
}

/**
 * Determines whether `name` is a symlink.
 *
 * @returns true on success, false on failure;
 */
static bool
check_if_symlink(const char *name, bool *OUT_is_symlink)
{
        struct stat sb;
        int r;

        *OUT_is_symlink = false;

        do {
                r = lstat(name, &sb);
        } while (r != 0 && errno == EINTR);

        if (r != 0) {
                /*
                 * If the path doesn't exist yet, it's clearly not a
                 * symlink.
                 */
                return errno == ENOENT;
        }

        *OUT_is_symlink = S_ISLNK(sb.st_mode) != 0;
        return true;
}

/**
 * Attempts to copy the result of `realpath` to dst.
 *
 * @returns true on success, false on failure.
 */
static bool
resolve_path(const char *name, int n, char *dst)
{
        char buf[PATH_MAX];
        const char *resolved;
        size_t resolved_size;

        resolved = realpath(name, buf);
        if (resolved == NULL)
                return false;

        resolved_size = strlen(resolved) + 1;
        if ((int)resolved_size > n)
                return false;

        memcpy(dst, resolved, resolved_size);
        return true;
}

/**
 * Iteratively constructs the symlinked path for `link` in `dst`,
 * until `dst` is not a symlink.
 *
 * @returns true on success, false on failure.
 */
static bool
walk_symlink(const char *link, char *dst, size_t n)
{
#ifdef SQLITE_MAX_SYMLINKS
        const size_t limit = SQLITE_MAX_SYMLINKS;
#else
        const size_t limit = 100;
#endif

        /* Copy `link` to `dst` */
        {
                size_t link_len;

                link_len = strnlen(link, n);
                if (link_len >= n)
                        return false;

                memcpy(dst, link, link_len + 1);
        }

        /*
         * Expand the last component of `dst` in place.
         */
        for (size_t i = 0; i <= limit; i++) {
                char target[PATH_MAX];
                char *last_slash;
                ssize_t r;
                bool is_symlink;

                /*
                 * If we don't have a symlink, there's nothing else to
                 * do.  Also ignore all errors: either `resolve_parent_path`
                 * will handle it, or `open(2)` will fail.
                 */
                if (check_if_symlink(dst, &is_symlink) == false ||
                    is_symlink == false)
                        return true;

                do {
                        r = readlink(dst, target, sizeof(target));
                } while (r < 0 && errno == EINTR);

                if ((size_t)r >= sizeof(target))
                        return false;

                target[r] = '\0';

                last_slash = strrchr(dst, '/');
                if (last_slash == NULL) {
                        if ((size_t)r >= n)
                                return false;

                        memcpy(dst, target, r + 1);
                } else {
                        size_t remaining = (dst + n) - (last_slash + 1);

                        if ((size_t)r > remaining)
                                return false;

                        memcpy(last_slash + 1, target, r + 1);
                }
        }

        /*
         * If we hit the iteration limit, that was too many symlinks,
         * and we should fail.
         */
        return false;
}

/**
 * Find the real path for a file that might not exist yet, by
 * resolving only the parent directory, and appending the file to it.
 * The `name` string will be modified in place.
 *
 * This will fail to do something useful if the parent directory also
 * does not exist, but that's fine: that means there can't be funny
 * symlink shenanigans, and the open call will fail anyway.
 *
 * @return true on success, false on failure;
 */
static bool
resolve_parent_path(char *name, int n_dst, char *dst)
{
        char resolved_dir[PATH_MAX];
        const char *parent;
        const char *filename;
        const char *resolved;
        int r;

        /*
         * Split `name` in a parent directory and a trailing filename.
         */
        {
                char *last_slash;

                last_slash = strrchr(name, '/');
                if (last_slash == NULL) {
                        parent = ".";
                        filename = name;
                } else {
                        filename = last_slash + 1;

                        /* While the last two chars are "//", go back one. */
                        while (last_slash > name + 1 && last_slash[-1] == '/')
                                last_slash--;

                        *last_slash = '\0';
                        parent = name;
                }
        }

        resolved = realpath(parent, resolved_dir);
        if (resolved == NULL) {
                /*
                 * If some component is missing, we want to use the
                 * parent directory as is, and let the open(2) call deal
                 * with any problem.  Similarly for EACCES, which
                 * realpath can also return if we lack read
                 * permissions (that aren't needed for open(2)).

                 */
                if (errno != ENOENT && errno != EACCES)
                        return false;

                resolved = parent;
        }

        if (strcmp(resolved, "/") == 0) {
                r = snprintf(dst, n_dst, "/%s", filename);
        } else {
                r = snprintf(dst, n_dst, "%s/%s", resolved, filename);
        }

        return r >= 0 && r < n_dst;
}

/**
 * sqlite uses xFullPathName to figure out where a database file
 * actually lives: journal files are created in the same directory.
 *
 * This name-based scheme also means that opening DB files through
 * symlinks can be a bad idea; that's why we must return
 * SQLITE_OK_SYMLINK when we successfully construct the real path for
 * `name`, but `name` is actually a symlink.
 */
static int
linux_full_pathname(sqlite3_vfs *vfs, const char *name, int n, char *dst)
{
        char target[PATH_MAX];
        int ok;
        bool is_symlink;

        (void)vfs;

        if (check_if_symlink(name, &is_symlink) == false)
                return SQLITE_CANTOPEN;

        ok = (is_symlink == true) ? SQLITE_OK_SYMLINK : SQLITE_OK;
        if (resolve_path(name, n, dst) == true)
                return ok;

        /*
         * We now have to handle paths that don't exist yet, and
         * compute something that matches the file that would be
         * created if we were to open(2) through that path.
         */

        /* First, iteratively resolve when the direct `name` is a symlink. */
        if (walk_symlink(name, target, sizeof(target)) == false)
                return SQLITE_CANTOPEN;

        /* Next, try to resolve the parent directory. */
        if (resolve_parent_path(target, n, dst) == false)
                return SQLITE_CANTOPEN;

        return ok;
}

static void *
linux_dlopen(sqlite3_vfs *vfs, const char *name)
{

        (void)vfs;
        return dlopen(name, RTLD_NOW | RTLD_GLOBAL);
}

static void
linux_dlerror(sqlite3_vfs *vfs, int n, char *OUT_error)
{
        const char *err;

        (void)vfs;
        /*
         * The whole dlerror interface is thread-hostile.  Let's hope
         * this is good enough for sqlite.
         */
        err = dlerror();
        if (err != NULL)
                snprintf(OUT_error, n, "%s", err);
        return;
}

static dlfun_t *
linux_dlsym(sqlite3_vfs *vfs, void *handle, const char *symbol)
{

        (void)vfs;
        return (dlfun_t *)dlsym(handle, symbol);
}

static void
linux_dlclose(sqlite3_vfs *vfs, void *handle)
{

        (void)vfs;
        dlclose(handle);
        return;
}

/*
 * Linux added the getrandom syscall in 3.17, but glibc only recently
 * added a wrapper.  Define our own getrandom.
 */
static ssize_t
getrandom_compat(void *buf, size_t buflen, unsigned int flags)
{

        return syscall(SYS_getrandom, buf, buflen, flags);
}

static int
linux_randomness(sqlite3_vfs *vfs, int n, char *dst)
{

        (void)vfs;
        memset(dst, 0, n);

#if !defined(SQLITE_TEST) && !defined(SQLITE_OMIT_RANDOMNESS)
        ssize_t r;

        do {
                /*
                 * This is only called to initialise a small
                 * (256-byte) seed, so we don't have to worry
                 * about short reads.
                 */
                r = getrandom_compat(dst, n, /*flags=*/0);
        } while (r < 0 && errno == EINTR);
        /*
         * It's ok to fail silently: we zero-filled, so no UB,
         * and the output is only used for a non-crypto PRNG.
         */
#endif

        return n;
}

static int
linux_sleep(sqlite3_vfs *vfs, int microseconds)
{
        struct timespec to_sleep = {
                .tv_sec = microseconds / 1000000,
                .tv_nsec = (microseconds % 1000000) * 1000,
        };

        (void)vfs;

        nanosleep(&to_sleep, NULL);
        return microseconds;
}

static int
linux_get_last_error(sqlite3_vfs *vfs, int n, char *OUT_error)
{

        (void)vfs;
        (void)n;
        (void)OUT_error;
        /* As of sqlite 3.35.5, only the return code is used. */
        return errno;
}

static int
linux_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out)
{
        /* Offset copied from os_unix.c */
        static const int64_t epoch = 24405875 * (int64_t)8640000;
        struct timespec now;

        (void)vfs;

        clock_gettime(CLOCK_REALTIME, &now);
        *out = epoch + (int64_t)1000 * now.tv_sec + now.tv_nsec / 1000000;

#ifdef SQLITE_TEST
        extern int sqlite3_current_time;

        if (sqlite3_current_time != 0)
                *out = epoch + (int64_t)1000 * sqlite3_current_time;
#endif

        return SQLITE_OK;
}

/**
 * We don't actually implement any syscall fault injection logic.
 * However, some test code fails if we don't implement the interface
 * at all.  Expose no-op implementations to improve test coverage.
 */
static int
linux_set_syscall(sqlite3_vfs *vfs, const char *name, sqlite3_syscall_ptr ptr)
{

        (void)vfs;
        (void)ptr;
        /* No name -> reset. */
        if (name == NULL)
                return SQLITE_OK;

        return SQLITE_NOTFOUND;
}

static sqlite3_syscall_ptr
linux_get_syscall(sqlite3_vfs *vfs, const char *name)
{

        (void)vfs;
        (void)name;
        return NULL;
}

static const char *
linux_next_syscall(sqlite3_vfs *vfs, const char *name)
{

        (void)vfs;
        (void)name;
        return NULL;
}

int
verneuil__file_close_impl(sqlite3_file *vfile)
{
        struct linux_file *file = (void *)vfile;

        if (file->fd >= 0) {
                /*
                 * *never* retry close: it might fail after recycling
                 * the file descriptor id.
                 */
                close(file->fd);

#ifdef SQLITE_TEST
                extern int sqlite3_open_file_count;

                sqlite3_open_file_count--;
#endif
        }

        *file = (struct linux_file) { .fd = -1 };
        return SQLITE_OK;
}

int
verneuil__file_read_impl(sqlite3_file *vfile, void *dst, int n,
    sqlite3_int64 off)
{
        struct linux_file *file = (void *)vfile;

        while (n > 0) {
                ssize_t r;

                r = pread(file->fd, dst, n, off);
                if (r == 0)
                        break;

                if (r > 0) {
                        assert(r <= n);
                        dst = (char *)dst + r;
                        n -= r;
                        off += r;
                } else if (errno != EINTR) {
                        switch (errno) {
                        /* Upstream converts these to CORRUPTFS. */
                        case ERANGE:
                        case EIO:
                        case ENXIO:
#ifdef EDEVERR
                        case EDEVERR:
#endif
                                return SQLITE_IOERR_CORRUPTFS;

                        default:
                                return SQLITE_IOERR_READ;
                        }
                }
        }

        if (n == 0)
                return SQLITE_OK;

        /*
         * We don't return the actual read length; short reads must
         * instead zero-fill the remainder of the destination buffer.
         */
        memset(dst, 0, n);
        return SQLITE_IOERR_SHORT_READ;
}

int
verneuil__file_write_impl(sqlite3_file *vfile, const void *src, int n,
    sqlite3_int64 off)
{
        struct linux_file *file = (void *)vfile;

        while (n > 0) {
                ssize_t r;

                r = pwrite(file->fd, src, n, off);

                /* r == 0 shouldn't happen... */
                if (r >= 0) {
                        assert(r <= n);
                        src = (const char *)src + r;
                        n -= r;
                        off += r;
                } else if (errno != EINTR) {
                        switch (errno) {
                        case EDQUOT:
                        case ENOSPC:
                                return SQLITE_FULL;

                        default:
                                return SQLITE_IOERR_WRITE;
                        }
                }
        }

        return SQLITE_OK;
}

int
verneuil__file_truncate_impl(sqlite3_file *vfile, sqlite3_int64 size)
{
        struct linux_file *file = (void *)vfile;
        int r;

#ifdef __ANDROID__
        /*
         * Sqlite says ftruncate() always uses 32-bit offsets on
         * android, and it's safe to just ignore any requests
         * for more than 2GB!?
         */
        if (size > INT32_MAX)
                return SQLITE_OK;
#endif

        do {
                r = ftruncate(file->fd, size);
        } while (r != 0 && errno == EINTR);

        if (r != 0)
                return SQLITE_IOERR_TRUNCATE;

        return SQLITE_OK;
}

int
verneuil__file_sync_impl(sqlite3_file *vfile, int flags)
{
        struct linux_file *file = (void *)vfile;
        int r;

        (void)flags;

#ifdef SQLITE_TEST
        extern int sqlite3_fullsync_count;
        extern int sqlite3_sync_count;

        if ((flags & 0x0F) == SQLITE_SYNC_FULL)
                sqlite3_fullsync_count++;
        sqlite3_sync_count++;

        /* Fake fsync-ing the parent directory, if needed.*/
        if (file->dirsync_pending == true)
                sqlite3_sync_count++;
#endif

        /*
         * If we did implement dirsync, we would only have to do so
         * once after file creation.
         */
        file->dirsync_pending = false;

        /*
         * If the file doesn't exist in a directory, it won't be
         * visible after a crash, so there's nothing to sync.
         */
        if (file->path == NULL)
                return SQLITE_OK;

        do {
                r = fdatasync(file->fd);
        } while (r != 0 && errno == EINTR);

        if (r != 0) {
                switch (errno) {
                case ENOSPC:
                case EDQUOT:
                        return SQLITE_FULL;

                default:
                        return SQLITE_IOERR_FSYNC;
                }
        }

        return SQLITE_OK;
}

int
verneuil__file_size_impl(sqlite3_file *vfile, sqlite3_int64 *OUT_size)
{
        struct stat sb;
        struct linux_file *file = (void *)vfile;
        int r;

        do {
                r = fstat(file->fd, &sb);
        } while (r != 0 && errno == EINTR);

        if (r != 0)
                return SQLITE_IOERR_FSTAT;

        *OUT_size = sb.st_size;
        return SQLITE_OK;
}

/**
 * The Linux VFS replicates the default Unix VFS's locking scheme.
 *
 * The lock bytes start at `sqlite3PendingByte` or `0x40000000`.
 *
 * The first lock byte is the "PENDING" lock: when a writer has
 * acquired this lock, new read locks may not be acquired.
 *
 * The next one is the "RESERVED" lock: writers race for this lock to
 * determine which one can enter the write state machine.
 *
 * Finally, the next 510 bytes are for "SHARED" (read) locks.  Sqlite
 * uses this range to allow concurrent readers on systems that do not
 * support shared file locks.  We always acquire the whole range.
 */

static inline off_t
linux_file_pending_lock_offset(void)
{
        return 0x40000000;
}

static inline off_t
linux_file_reserved_lock_offset(void)
{

        return linux_file_pending_lock_offset() + 1;
}

static inline off_t
linux_file_shared_lock_offset(void)
{

        return linux_file_pending_lock_offset() + 2;
}

static const size_t linux_file_shared_lock_size = 510;

/*
 * The locks all live in a 512-byte region starting at
 * `linux_file_pending_lock_offset()`.
 */
static const size_t linux_file_all_lock_size = 512;

static uint64_t
now_ms_boottime(void)
{
        struct timespec now;

        clock_gettime(CLOCK_BOOTTIME, &now);
        return 1000 * (uint64_t)now.tv_sec + (now.tv_nsec / 1000000);
}

/**
 * Sleeps for a few milliseconds, or until `end_ms`.
 *
 * @param num_attempts the number of times we have already slept
 *   while trying to acquire the same lock.
 * Returns false if the current time is past the `end_ms` deadline.
 */
static bool
sleep_until_at_most(uint64_t num_attempts, uint64_t end_ms)
{
        struct timespec to_sleep = { .tv_sec = 0 };
        uint64_t now;
        double random_value;
        double max_sleep_ms;
        double sleep_ms;

        now = now_ms_boottime();
        if (now >= end_ms)
                return false;

        {
                uint64_t random_bits;

                sqlite3_randomness(sizeof(random_bits), &random_bits);
                random_value = (1.0 / UINT64_MAX) * random_bits;
        }

        /*
         * Use exponential backoff to set the maximum sleep value,
         * from an initial value 0.1 milliseconds up to 10
         * milliseconds.
         *
         * We use `num_attempts / 4` as our backoff exponent to
         * approximate a smoother base factor of ~1.2x instead of 2x
         * per attempt.  This asymptotically guarantees that, if the
         * lock is held continuously until we acquire it, we will
         * sleep at most ~50% (modulo jitter) longer than the time
         * the lock was actually unavailable to us.
         */
        if (num_attempts < 7 * 4) {
                max_sleep_ms = 0.1 * (1UL << (num_attempts / 4));
        } else {
                max_sleep_ms = 10.0;
        }

        if (max_sleep_ms > end_ms - now)
                max_sleep_ms = end_ms - now;

        /* And jitter uniformly within that limit. */
        sleep_ms = random_value * max_sleep_ms;
        to_sleep.tv_nsec = 1e6 * sleep_ms + 0.5;
        nanosleep(&to_sleep, NULL);
        return true;
}

static int
linux_flock_op(const struct linux_file *file, int op, struct flock *fl,
    int default_error)
{
        uint64_t begin, end;
        uint64_t num_sleep = 0;
        uint32_t timeout = file->lock_timeout_ms;
        int r;

        begin = now_ms_boottime();
        end = begin + timeout;

        do {
                r = fcntl(file->fd, op, fl);

                /*
                 * If we failed to acquire the lock, and there's a
                 * non-zero timeout, sleep for a bit at most 20 times
                 * per timeout millisecond: this limit guarantees that
                 * we won't sleep forever in case the clock isn't
                 * monotonic.
                 *
                 * The initial sleeps are expected to be ~0.05 ms on
                 * average and start ramping up after the 4th, so
                 * allowing 20 sleeps per millisecond of timeout means
                 * that we will definitely not stop due to this sleep
                 * *count* limit unless time goes wonky or our calls
                 * to nanosleep are interrupted very frequently.
                 */
                if (r < 0 &&
                    num_sleep / 20 < timeout &&
                    (errno == EACCES || errno == EAGAIN)) {
                        if (sleep_until_at_most(num_sleep, end) == false) {
                                errno = EACCES;
                                break;
                        }

                        num_sleep++;
                        /*
                         * Pretend we were just interrupted after
                         * sleeping for a bit, to let the do / while
                         * loop try again.
                         */
                        errno = EINTR;
                }
        } while (r < 0 && errno == EINTR);

        if (r != 0) {
                switch (errno) {
                case EACCES:
                case EAGAIN:
                case ETIMEDOUT:
                case EBUSY:
                case EINTR:
                case ENOLCK:
                        return SQLITE_BUSY;

                case EPERM:
                        return SQLITE_PERM;

                default:
                        return default_error;
                }
        }

        return SQLITE_OK;
}

static int
acquire_shared_lock(struct linux_file *file)
{
        struct flock fl = {
                .l_type = F_RDLCK,
                .l_whence = SEEK_SET,
                .l_start = linux_file_pending_lock_offset(),
                .l_len = 1,
        };
        int r;

        /*
         * Before acquiring a shared lock, we must make sure that the
         * PENDING byte is free.  It's OK if the byte is then taken
         * while we try to grab a read lock on the shared lock range:
         * the PENDING byte only exists to protect against starvation,
         * when new readers keep preventing the writer from acquiring
         * the shared range for writes.
         *
         * Once the writer has acquired the PENDING byte, only a
         * bounded number of readers may have already observed it as
         * free, but not acquired the shared range for reads yet.
         * A race here cannot starve the writer forever.
         */
        r = linux_flock_op(file, F_OFD_GETLK, &fl, SQLITE_IOERR_LOCK);
        if (r != SQLITE_OK)
                return r;

        if (fl.l_type != F_UNLCK)
                return SQLITE_BUSY;

        /* Now, we can try to acquire the shared lock range for reads. */
        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_RDLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_shared_lock_offset(),
                    .l_len = linux_file_shared_lock_size,
            }, SQLITE_IOERR_LOCK);
        if (r != SQLITE_OK)
                return r;

        file->lock_level = SQLITE_LOCK_SHARED;
        return SQLITE_OK;
}

static int
acquire_reserved_lock(struct linux_file *file)
{
        int r;

        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_WRLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_reserved_lock_offset(),
                    .l_len = 1,
            }, SQLITE_IOERR_LOCK);
        if (r != SQLITE_OK)
                return r;

        file->lock_level = SQLITE_LOCK_RESERVED;
        return SQLITE_OK;
}

static int
acquire_exclusive_lock(struct linux_file *file)
{
        int r;

        /*
         * Acquire the "intent to write" lock if necessary.
         *
         * I don't think this can happen with sqlite, but let's make
         * the locking code obviously correct.
         */
        if (file->lock_level < SQLITE_LOCK_RESERVED) {
                r = acquire_reserved_lock(file);
                if (r != SQLITE_OK)
                        return r;
        }

        /*
         * Before acquiring an exclusive lock, we must first acquire
         * the pending lock byte, to tell readers to drain out.
         *
         * This should not fail with SQLITE_BUSY, now that we own the
         * RESERVED byte.
         */
        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_WRLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_pending_lock_offset(),
                    .l_len = 1,
            }, SQLITE_IOERR_LOCK);
        if (r != SQLITE_OK)
                return r;

        file->lock_level = SQLITE_LOCK_PENDING;

        /* Now, we can try to acquire the shared lock range exclusively. */
        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_WRLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_shared_lock_offset(),
                    .l_len = linux_file_shared_lock_size,
            }, SQLITE_IOERR_LOCK);
        if (r != SQLITE_OK)
                return r;

        file->lock_level = SQLITE_LOCK_EXCLUSIVE;
        return SQLITE_OK;
}

int
verneuil__file_lock_impl(sqlite3_file *vfile, int level)
{
        struct linux_file *file = (void *)vfile;

        /* xLock never downgrades, and instead no-ops. */
        if (file->lock_level >= level)
                return SQLITE_OK;

        switch (level) {
        case SQLITE_LOCK_SHARED:
                return acquire_shared_lock(file);

        case SQLITE_LOCK_RESERVED:
                /* We're not supposed to go from NONE to RESERVED. */
                assert(file->lock_level == SQLITE_LOCK_SHARED);
                return acquire_reserved_lock(file);

        case SQLITE_LOCK_EXCLUSIVE:
                return acquire_exclusive_lock(file);

        case SQLITE_LOCK_NONE:
        case SQLITE_LOCK_PENDING:  /* PENDING is an internal state. */
        default:
                /* Shouldn't happen. */
                return SQLITE_ERROR;
        }
}

static int
release_all_locks(struct linux_file *file)
{
        int r;

        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_UNLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_pending_lock_offset(),
                    .l_len = linux_file_all_lock_size,
            }, SQLITE_IOERR_UNLOCK);
        if (r != SQLITE_OK)
                return r;

        file->lock_level = SQLITE_LOCK_NONE;
        return SQLITE_OK;
}

static int
downgrade_write_lock_to_shared(struct linux_file *file)
{
        int r;

        /*
         * Start by converting all our locks to shared.
         */
        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_RDLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_pending_lock_offset(),
                    .l_len = linux_file_all_lock_size,
            }, SQLITE_IOERR_UNLOCK);
        /*
         * Downgrades should not fail with SQLITE_BUSY (that can only
         * happen if another process locks the file with an
         * incompatible scheme).  If they do, the unix VFS says we
         * should instead return IOERR_RDLOCK to avoid asserts.
         */
        if (r == SQLITE_BUSY)
                return SQLITE_IOERR_RDLOCK;
        if (r != SQLITE_OK)
                return r;

        /*
         * At this point, we definitely don't have an exclusive lock,
         * and new read locks can be acquired.
         *
         * If the next step fails, other writers will not be able to
         * make progress until we release all our locks, but they
         * would have been blocked when upgrading from RESERVED to
         * EXCLUSIVE anyway.
         */
        file->lock_level = SQLITE_LOCK_SHARED;

        /*
         * And now release the (shared) reserved and pending lock: we
         * may not actually own the pending lock, but, given that we
         * do hold the reserved lock, no one can.
         */
        r = linux_flock_op(file, F_OFD_SETLK,
            &(struct flock) {
                    .l_type = F_UNLCK,
                    .l_whence = SEEK_SET,
                    .l_start = linux_file_pending_lock_offset(),
                    .l_len = 2,
            }, SQLITE_IOERR_UNLOCK);
        if (r != SQLITE_OK)
                return SQLITE_IOERR_RDLOCK;

        return SQLITE_OK;
}

int
verneuil__file_unlock_impl(sqlite3_file *vfile, int level)
{
        struct linux_file *file = (void *)vfile;

        /* xUnlock never upgrades, and instead no-ops. */
        if (file->lock_level <= level)
                return SQLITE_OK;

        switch (level) {
        case SQLITE_LOCK_NONE:
                return release_all_locks(file);
        case SQLITE_LOCK_SHARED:
                return downgrade_write_lock_to_shared(file);

        case SQLITE_LOCK_RESERVED:
        case SQLITE_LOCK_EXCLUSIVE:
        case SQLITE_LOCK_PENDING:  /* PENDING is an internal state. */
        default:
                /* Shouldn't happen. */
                return SQLITE_ERROR;
        }
}

static int
linux_file_check_reserved_lock(sqlite3_file *vfile, int *OUT_result)
{
        struct flock fl = {
                /* The reserved byte is only really acquired for writes. */
                .l_type = F_RDLCK,
                .l_whence = SEEK_SET,
                .l_start = linux_file_reserved_lock_offset(),
                .l_len = 1,
        };
        struct linux_file *file = (void *)vfile;
        int r;

        *OUT_result = 0;
        /*
         * This fcntl should say "yes the RESERVED byte lock is taken"
         * even when it's owned by the current `file`.  OFD locks
         * can't tell us that (a file description can always override
         * its own locks), so we must first look at the file's current
         * lock level.
         */
        if (file->lock_level >= SQLITE_LOCK_RESERVED) {
                *OUT_result = 1;
                return SQLITE_OK;
        }

        r = linux_flock_op(file, F_OFD_GETLK, &fl,
            SQLITE_IOERR_CHECKRESERVEDLOCK);
        if (r != SQLITE_OK)
                return r;

        *OUT_result = (fl.l_type == F_UNLCK) ? 0 : 1;
        return SQLITE_OK;
}

static bool
linux_file_has_moved(const struct linux_file *file)
{
        struct stat sb;
        const char *path = file->path;
        int r;

        /*
         * Assume temporary files don't move: we ideally don't even
         * want them to have a name.
         */
        if (path == NULL)
                return false;

        do {
                r = stat(path, &sb);
        } while (r != 0 && errno == EINTR);

        /* Assume this means trouble. */
        if (r != 0)
                return true;

        return sb.st_dev != file->device || sb.st_ino != file->inode;
}

static int
linux_tempfilename(char **dst)
{
        static _Atomic uint64_t counter = 0;
        struct timespec now = { 0 };
        uint64_t noise[2] = { 0 };
        uint64_t unique;
        ssize_t r;
        int pid;

        clock_gettime(CLOCK_REALTIME, &now);
        do {
                r = getrandom_compat(noise, sizeof(noise), 0);
        } while (r <= 0 && errno == EINTR);

        unique = atomic_fetch_add(&counter, 1);
        pid = getpid();

        static_assert(
            LINUX_VFS_TEMPPATH_MAX + sizeof(SQLITE_TEMP_FILE_PREFIX)
            /* the harcoded string pattern is < 100 chars. */
            + 100
            /* pid fits in 10 chars. */
            + 10
            /* time and counter fit in 20 chars each. */
            + 2 * 20
            /* finally, the two random u64 are 16 chars each. */
            + 2 * 16
            < PATH_MAX,
            "TEMPPATH_MAX + suffix must fit in PATH_MAX");

        *dst = sqlite3_mprintf(
            "%s/"SQLITE_TEMP_FILE_PREFIX"linux_vfs.pid=%i"
            ".time=%lld.counter=%"PRIu64".rand=%016"PRIx64"%016"PRIx64".tmp",
            get_tempdir_base(), pid,
            (long long)now.tv_sec, unique, noise[0], noise[1]);
        if (*dst == NULL)
                return SQLITE_NOMEM;

        return SQLITE_OK;
}

static int
linux_file_control(sqlite3_file *vfile, int op, void *arg)
{
        struct linux_file *file = (void *)vfile;

        switch (op) {
        /* Advisory fcntl used in tests. */
        case SQLITE_FCNTL_CHUNK_SIZE:
                return SQLITE_OK;

        case SQLITE_FCNTL_VFSNAME: {
#ifdef TEST_VFS
                /*
                 * Pretend we're "unix" in tests, to avoid
                 * accidentally losing coverage.
                 */
                const char *vfsname = "unix";
#else
                const char *vfsname = "linux";
#endif
                *(char**)arg = sqlite3_mprintf("%s", vfsname);
                return SQLITE_OK;
        }

        case SQLITE_FCNTL_HAS_MOVED:
                *(int *)arg = linux_file_has_moved(file) ? 1 : 0;
                return SQLITE_OK;

        case SQLITE_FCNTL_TEMPFILENAME:
                return linux_tempfilename(arg);

        /* These are used in tests, and should be implemented. */
        case SQLITE_FCNTL_LOCKSTATE:
                *(int *)arg = file->lock_level;
                return SQLITE_OK;

        case SQLITE_FCNTL_LOCK_TIMEOUT: {
                uint32_t old = file->lock_timeout_ms;

                file->lock_timeout_ms = *(uint32_t *)arg;
                *(uint32_t *)arg = old;
                return SQLITE_OK;
        }

        default:
                return SQLITE_NOTFOUND;
        }
}

static int
linux_file_sector_size(sqlite3_file *vfile)
{
        enum {
                /*
                 * sqlite assumes the sector size is a multiple of its
                 * min value, 512 bytes.
                 */
                MIN_SECTOR_SIZE = 512,
#ifdef PAGE_SIZE
                DEFAULT_SECTOR_SIZE = PAGE_SIZE,
#else
                DEFAULT_SECTOR_SIZE = 4096,
#endif
        };
        static atomic_int cached_sector_size = 0;
        long sector_size;

        (void)vfile;

        /*
         * Our sector size value is independent of the underlying
         * filesystem, so we can use a process-global cache.
         *
         * Any non-zero value must be valid: the sector size
         * computation should be deterministic, so redundant writes
         * will always store the same value.
         */
        sector_size = atomic_load_explicit(&cached_sector_size,
            memory_order_relaxed);
        if (sector_size != 0)
                return sector_size;

        /*
         * Let the sector size match the OS page size, if we can find
         * it.  That's the granularity at which Linux's buffered IO
         * works, so should be safe.  It also happens to match the
         * sqlite default on x86 and x86-64.
         */
        sector_size = sysconf(_SC_PAGESIZE);
        if (sector_size <= 0 || sector_size > INT_MAX)
                sector_size = DEFAULT_SECTOR_SIZE;

        if ((sector_size % MIN_SECTOR_SIZE) != 0)
                sector_size = MIN_SECTOR_SIZE;

        atomic_store_explicit(&cached_sector_size, sector_size,
            memory_order_relaxed);
        return sector_size;
}

static int
linux_file_device_characteristics(sqlite3_file *vfile)
{
        /*
         * We don't have any kind of failure-atomic write guarantee on
         * Linux, so SQLITE_IOCAP_ATOMIC* must be off.
         *
         * We have seen what looks like files being extended with zero
         * bytes before storing the new data, when VMs froze while
         * writing to ext4 / Ceph, so `SQLITE_IOCAP_SAFE_APPEND` isn't.
         *
         * Linux will reorder buffered I/O, so
         * `SQLITE_IOCAP_SEQUENTIAL` is not a thing.
         *
         * Open files can be unlinked under POSIX, so
         * `SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN` is also not a thing.
         *
         * We know data is overwritten a full OS page at a time, and
         * maybe worse with SSDs, so `SQLITE_IOCAP_POWERSAFE_OVERWRITE`
         * doesn't seem true at all... however, stock sqlite turns it
         * on by default because otherwise the journaling I/O is
         * really sucky.  See https://sqlite.org/psow.html for
         * details.
         *
         * Although some filesystems (e.g., xfs) support fully
         * immutable files, we don't check for it, so let's assume
         * `SQLITE_IOCAP_IMMUTABLE` is false.
         *
         * Finally, we also don't implement F2FS-style batch atomic
         * commits, so `SQLITE_IOCAP_BATCH_ATOMIC` is false.
         */
        (void)vfile;

        return SQLITE_IOCAP_POWERSAFE_OVERWRITE;
}

int
verneuil_configure_impl(const struct verneuil_options *options)
{
        static const struct verneuil_options default_options;
        int rc;

        if (options == NULL)
                options = &default_options;

        rc = sqlite3_vfs_register(&verneuil_vfs, options->make_default ? 1 : 0);
        if (rc != SQLITE_OK)
                return rc;

        if (options->tempdir != NULL)
                return verneuil_set_tempdir(options->tempdir);

        return SQLITE_OK;
}

int
verneuil_init_impl(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi)
{
#ifdef TEST_VFS
        int make_default = 1;
#else
        int make_default = 0;
#endif
        int rc;

        (void)db;
        (void)pzErrMsg;
        SQLITE_EXTENSION_INIT2(pApi);

        /*
         * When building in test mode, also shadow the "unix" vfs:
         * some tests instantiate it directly, and we want that
         * coverage.
         */
#ifdef TEST_VFS
        {
                static sqlite3_vfs verneuil_fake_unix_vfs;

                if (verneuil_fake_unix_vfs.zName == NULL) {
                        verneuil_fake_unix_vfs = verneuil_vfs;
                        verneuil_fake_unix_vfs.zName = "unix";
                }

                rc = sqlite3_vfs_register(&verneuil_fake_unix_vfs, /*makeDflt=*/0);
                if (rc != SQLITE_OK)
                        return rc;
        }
#endif

        rc = sqlite3_vfs_register(&verneuil_vfs, make_default);
        if (rc != SQLITE_OK)
                return rc;

        return SQLITE_OK_LOAD_PERMANENTLY;
}

#if defined(TEST_VFS) && defined(SQLITE_CORE)
int
verneuil_test_only_register(void)
{
        char *error = NULL;
        int rc;

        rc = sqlite3_verneuil_init(NULL, &error, NULL);
        sqlite3_free(error);

        if (rc == SQLITE_OK_LOAD_PERMANENTLY)
                rc = SQLITE_OK;
        return rc;
}
#endif
