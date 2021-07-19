#include "file_ops.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/xattr.h>
#include <unistd.h>

/**
 * Some older distributions fail to expose OFD constants.  Hardcode
 * them if necessary: open file description locks have been around
 * since Linux 3.15, and we don't try to support anything that old
 * (getrandom was introduced in 3.17, and we also use that).
 */
#ifndef F_OFD_SETLK
#define F_OFD_SETLK     37
#endif

#if F_OFD_SETLK != 37
# error "Mismatch in fallback OFD fcntl constant."
#endif

int
verneuil__open_temp_file(const char *directory, int mode)
{
        int r;

        do {
                r = open(directory, O_RDWR | O_CLOEXEC | O_TMPFILE, mode);
        } while (r < 0 && errno == EINTR);

        return r;
}

int
verneuil__link_temp_file(int fd, const char *target)
{
        char buf[200];
        int r;

        r = snprintf(buf, sizeof(buf), "/proc/self/fd/%i", fd);
        assert((size_t)r < sizeof(buf));

        do {
                r = linkat(AT_FDCWD, buf, AT_FDCWD, target, AT_SYMLINK_FOLLOW);
        } while (r < 0 && errno == EINTR);

        return r;
}

int
verneuil__open_directory(const char *path)
{
        int r;

        do {
                r = open(path, O_CLOEXEC | O_DIRECTORY | O_PATH);
        } while (r < 0 && errno == EINTR);

        return r;
}

ssize_t
verneuil__getxattr(int fd, const char *name, void *buf, size_t bufsz)
{
        ssize_t ret;

        do {
                ret = fgetxattr(fd, name, buf, bufsz);
        } while (ret < 0 && errno == EINTR);

        if (ret >= 0)
                return ret;

        if (errno == ENOTSUP)
                return -1;

        return 0;
}

int
verneuil__setxattr(int fd, const char *name, const void *buf, size_t bufsz)
{
        ssize_t ret;

        do {
                ret = fsetxattr(fd, name, buf, bufsz, /*flags=*/0);
        } while (ret < 0 && errno == EINTR);

        if (ret == 0)
                return 0;

        if (errno == ENOTSUP)
                return 1;

        return -1;
}

int
verneuil__ofd_lock_exclusive(int fd)
{
        struct flock fl = {
                .l_type = F_WRLCK,
                .l_whence = SEEK_SET,
                .l_start = 0,
                .l_len = 1,
        };
        int r;

        do {
                r = fcntl(fd, F_OFD_SETLK, &fl);
        } while (r < 0 && errno == EINTR);

        if (r == 0)
                return 0;

        if (errno == EAGAIN)
                return 1;

        return -1;
}
