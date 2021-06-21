#include "file_ops.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
