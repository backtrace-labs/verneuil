#include "file_ops.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/xattr.h>
#include <unistd.h>

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
verneuil__touch(int fd)
{
        int r;

        do {
                r = futimens(fd, NULL);
        } while (r < 0 && errno == EINTR);

        return r;
}

int
verneuil__ofd_lock_exclusive(int fd)
{
        int r;

        /*
         * We always acquire a lock on the whole file, so regular BSD
         * flock does what we want.
         */
        do {
                r = flock(fd, LOCK_EX | LOCK_NB);
        } while (r < 0 && errno == EINTR);

        if (r == 0)
                return 0;

        if (errno == EWOULDBLOCK)
                return 1;

        return -1;
}

int
verneuil__ofd_lock_release(int fd)
{
        int r;

        do {
                r = flock(fd, LOCK_UN);
        } while (r < 0 && errno == EINTR);

        return r;
}
