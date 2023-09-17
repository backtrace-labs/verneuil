#include "file_ops.h"

#include <assert.h>
#include <errno.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

ssize_t
verneuil__getxattr(int fd, const char *name, void *buf, size_t bufsz)
{
        ssize_t ret;

        do {
#ifdef __APPLE__
                ret = fgetxattr(fd, name, buf, bufsz, /*position=*/0, /*options=*/0);
#else
                /* Assume this is linux-compatible. */
                ret = fgetxattr(fd, name, buf, bufsz);
#endif
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
#ifdef __APPLE__
                ret = fsetxattr(fd, name, buf, bufsz, /*position=*/0, /*options=*/0);
#else
                ret = fsetxattr(fd, name, buf, bufsz, /*flags=*/0);
#endif
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
