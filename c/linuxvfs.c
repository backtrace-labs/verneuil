#include "linuxvfs.h"

#include <stddef.h>
#include <stdlib.h>

SQLITE_EXTENSION_INIT1

/**
 * This shim VFS wraps the default VFS and exposes only a subset of IO
 * methods.  Running SQLite's tests with this dummy VFS gives us a
 * baseline for VFSes that do not implement mmap or WAL methods, nor
 * any of the test syscall override logic.
 */

struct shim_file {
        sqlite3_file base;
        sqlite3_file *original;
};

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

static int shim_file_close(sqlite3_file *);
static int shim_file_read(sqlite3_file *, void *dst, int n, sqlite3_int64 off);
static int shim_file_write(sqlite3_file *, const void *src, int n, sqlite3_int64 off);
static int shim_file_truncate(sqlite3_file *, sqlite3_int64 size);
static int shim_file_sync(sqlite3_file *, int flags);
static int shim_file_size(sqlite3_file *, sqlite3_int64 *OUT_size);

static int shim_file_lock(sqlite3_file *, int level);
static int shim_file_unlock(sqlite3_file *, int level);
static int shim_file_check_reserved_lock(sqlite3_file *, int *OUT_result);

static int shim_file_control(sqlite3_file *, int op, void *arg);
static int shim_file_sector_size(sqlite3_file *);
static int shim_file_device_characteristics(sqlite3_file *);

static sqlite3_vfs *base_vfs;

static const struct sqlite3_io_methods shim_io_methods = {
        .iVersion = 1,  /* No WAL or mmap method */
        .xClose = shim_file_close,

        .xRead = shim_file_read,
        .xWrite = shim_file_write,
        .xTruncate = shim_file_truncate,
        .xSync = shim_file_sync,

        .xFileSize = shim_file_size,

        .xLock = shim_file_lock,
        .xUnlock = shim_file_unlock,
        .xCheckReservedLock = shim_file_check_reserved_lock,

        .xFileControl = shim_file_control,
        .xSectorSize = shim_file_sector_size,
        .xDeviceCharacteristics = shim_file_device_characteristics,
};

static sqlite3_vfs linux_vfs = {
        .iVersion = 3,
        .szOsFile = sizeof(struct shim_file),
        .mxPathname = 512,  /* default limit for the default VFS */
        .zName = "linux",
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

static int
linux_open(sqlite3_vfs *vfs, const char *name, sqlite3_file *vfile,
    int flags, int *OUT_flags)
{
        struct shim_file *file = (void *)vfile;
        int rc;

        (void)vfs;
        *file = (struct shim_file) {
                .base.pMethods = &shim_io_methods,
                .original = calloc(1, base_vfs->szOsFile),
        };

        if (file->original == NULL) {
                *file = (struct shim_file) { 0 };
                return SQLITE_NOMEM;
        }

        rc = base_vfs->xOpen(base_vfs, name, file->original, flags, OUT_flags);
        if (file->original->pMethods == NULL) {
                free(file->original);
                *file = (struct shim_file) { 0 };
        }

        return rc;
}

static int
linux_delete(sqlite3_vfs *vfs, const char *name, int syncDir)
{

        (void)vfs;
        return base_vfs->xDelete(base_vfs, name, syncDir);
}

static int
linux_access(sqlite3_vfs *vfs, const char *name, int flags, int *OUT_res)
{

        (void)vfs;
        return base_vfs->xAccess(base_vfs, name, flags, OUT_res);
}

static int
linux_full_pathname(sqlite3_vfs *vfs, const char *name, int n, char *dst)
{

        (void)vfs;
        return base_vfs->xFullPathname(vfs, name, n, dst);
}

static void *
linux_dlopen(sqlite3_vfs *vfs, const char *name)
{

        (void)vfs;
        return base_vfs->xDlOpen(base_vfs, name);
}

static void
linux_dlerror(sqlite3_vfs *vfs, int n, char *OUT_error)
{

        (void)vfs;
        base_vfs->xDlError(base_vfs, n, OUT_error);
        return;
}

static dlfun_t *
linux_dlsym(sqlite3_vfs *vfs, void *handle, const char *symbol)
{

        (void)vfs;
        return base_vfs->xDlSym(base_vfs, handle, symbol);
}

static void
linux_dlclose(sqlite3_vfs *vfs, void *handle)
{

        (void)vfs;
        base_vfs->xDlClose(base_vfs, handle);
        return;
}

static int
linux_randomness(sqlite3_vfs *vfs, int n, char *dst)
{

        (void)vfs;
        return base_vfs->xRandomness(base_vfs, n, dst);
}

static int
linux_sleep(sqlite3_vfs *vfs, int microseconds)
{

        (void)vfs;
        return base_vfs->xSleep(base_vfs, microseconds);
}

static int
linux_get_last_error(sqlite3_vfs *vfs, int n, char *OUT_error)
{

        (void)vfs;
        return base_vfs->xGetLastError(base_vfs, n, OUT_error);
}

static int
linux_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out)
{

        (void)vfs;
        return base_vfs->xCurrentTimeInt64(base_vfs, out);
}

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

static int
shim_file_close(sqlite3_file *vfile)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;
        int rc;

        if (original == NULL) {
                *file = (struct shim_file) { 0 };
                return SQLITE_OK;
        }

        if (original->pMethods != NULL) {
                rc = original->pMethods->xClose(original);
        } else {
                rc = SQLITE_OK;
        }

        free(original);
        *file = (struct shim_file) { 0 };
        return rc;
}

static int
shim_file_read(sqlite3_file *vfile, void *dst, int n, sqlite3_int64 off)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xRead(original, dst, n, off);
}

static int
shim_file_write(sqlite3_file *vfile, const void *src, int n, sqlite3_int64 off)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xWrite(original, src, n, off);
}

static int
shim_file_truncate(sqlite3_file *vfile, sqlite3_int64 size)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xTruncate(original, size);
}

static int
shim_file_sync(sqlite3_file *vfile, int flags)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xSync(original, flags);
}

static int
shim_file_size(sqlite3_file *vfile, sqlite3_int64 *OUT_size)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xFileSize(original, OUT_size);
}

static int
shim_file_lock(sqlite3_file *vfile, int level)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xLock(original, level);
}

static int
shim_file_unlock(sqlite3_file *vfile, int level)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xUnlock(original, level);
}

static int
shim_file_check_reserved_lock(sqlite3_file *vfile, int *OUT_result)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xCheckReservedLock(original, OUT_result);
}

static int
shim_file_control(sqlite3_file *vfile, int op, void *arg)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        switch (op) {
        /* Advisory fcntl used in tests. */
        case SQLITE_FCNTL_CHUNK_SIZE:
                return SQLITE_OK;

        case SQLITE_FCNTL_VFSNAME:
                *(char**)arg = sqlite3_mprintf("%s", base_vfs->zName);
                return SQLITE_OK;

        /* These are used in tests, and should be implemented. */
        case SQLITE_FCNTL_LOCKSTATE:
        case SQLITE_FCNTL_TEMPFILENAME:
        case SQLITE_FCNTL_HAS_MOVED:
                return original->pMethods->xFileControl(original, op, arg);

        default:
                return SQLITE_NOTFOUND;
        }
}

static int
shim_file_sector_size(sqlite3_file *vfile)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xSectorSize(original);
}

static int
shim_file_device_characteristics(sqlite3_file *vfile)
{
        struct shim_file *file = (void *)vfile;
        sqlite3_file *original = file->original;

        return original->pMethods->xDeviceCharacteristics(original);
}

int
sqlite3_linuxvfs_init(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi)
{
        sqlite3_vfs *default_vfs;
        int rc;

        (void)db;
        SQLITE_EXTENSION_INIT2(pApi);

        default_vfs = sqlite3_vfs_find(0);
        if (default_vfs == NULL) {
                *pzErrMsg = sqlite3_mprintf("unable to find default vfs");
                goto error;
        }

        if (default_vfs->iVersion < linux_vfs.iVersion) {
                *pzErrMsg = sqlite3_mprintf("default vfs has version %i < %i",
                    default_vfs->iVersion, linux_vfs.iVersion);
                goto error;
        }

        if (default_vfs != &linux_vfs)
                base_vfs = default_vfs;

        /*
         * When building in test mode, also shadow the "unix" vfs:
         * some tests instantiate it directly, and we want that
         * coverage.
         */
#ifdef TEST_VFS
        {
                static sqlite3_vfs linux_fake_unix_vfs;

                if (linux_fake_unix_vfs.zName == NULL) {
                        linux_fake_unix_vfs = linux_vfs;
                        linux_fake_unix_vfs.zName = "unix";
                }

                rc = sqlite3_vfs_register(&linux_fake_unix_vfs, /*makeDflt=*/0);
                if (rc != SQLITE_OK)
                        return rc;
        }
#endif

        rc = sqlite3_vfs_register(&linux_vfs, /*makeDflt=*/1);
        if (rc != SQLITE_OK)
                return rc;

        return SQLITE_OK_LOAD_PERMANENTLY;

error:
        if (*pzErrMsg == NULL)
                return SQLITE_NOMEM;

        return SQLITE_INTERNAL;
}

#ifdef SQLITE_CORE
/*
 * Do not define this entry point if we expect to be loaded
 * dynamically by sqlite: when built that way, our call to
 * `sqlite3_linuxvfs_init` will crash in
 * `SQLITE_EXTENSION_INIT2(pApi);`.
 */
int
sqlite3_linuxvfs_register(const char *unused)
{
        char *error = NULL;
        int rc;

        (void)unused;
        rc = sqlite3_linuxvfs_init(NULL, &error, NULL);
        sqlite3_free(error);

        if (rc == SQLITE_OK_LOAD_PERMANENTLY)
                rc = SQLITE_OK;
        return rc;
}
#endif
