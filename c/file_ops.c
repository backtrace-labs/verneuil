#include "file_ops.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef RENAME_EXCHANGE
# define RENAME_EXCHANGE (1 << 1)
#elif RENAME_EXCHANGE != (1 << 1)
# error "Mismatch in RENAME_EXCHANGE constant value."
#endif

static inline int
renameat2_compat(int olddirfd, const char *oldpath,
    int newdirfd, const char *newpath, unsigned int flags)
{

        return syscall(SYS_renameat2, olddirfd, oldpath, newdirfd, newpath, flags);
}

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
verneuil__exchange_paths(const char *x, const char *y)
{
        int r;

        do {
                r = renameat2_compat(AT_FDCWD, x, AT_FDCWD, y, RENAME_EXCHANGE);
        } while (r < 0 && errno == EINTR);

        return r;
}
