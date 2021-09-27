#pragma once
#include <sys/types.h>

/**
 * replication_buffer.rs, directory_schema.rs, and ofd_lock.rs call
 * into these functions for low-level file operations.
 */

/**
 * Attempts to create a new hardlink to `fd` at `target`.
 */
int verneuil__link_temp_file(int fd, const char *target);

/**
 * Attempts to get xattr `name` from `fd`.
 *
 * Returns the xattr size on success, and -1 on error.
 *
 * This function only errors out if the filesystem does not support
 * xattrs; everything else is mapped to an empty value.
 */
ssize_t verneuil__getxattr(int fd, const char *name, void *buf, size_t bufsz);

/**
 * Attempts to set xattr `name` on `fd` to `buf[0..bufsz - 1]`.
 *
 * Returns 0 on success, 1 if ENOTSUP, and -1 on all other errors.
 */
int verneuil__setxattr(int fd, const char *name, const void *buf, size_t bufsz);

/**
 * Attempts to update the atime, mtime, and ctime of `fd` (to now).
 */
int verneuil__touch(int fd);

/**
 * Attempts to acquire an exclusive open file description (OFD) lock
 * on `fd`.
 *
 * Returns 0 on success, 1 if the lock is taken, -1 on failure.
 */
int verneuil__ofd_lock_exclusive(int fd);

/**
 * Release any open file description (OFD) lock on `fd`.
 *
 * Returns 0 on success, -1 on failure.
 */
int verneuil__ofd_lock_release(int fd);
