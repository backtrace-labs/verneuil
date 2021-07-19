#pragma once
#include <sys/types.h>

/**
 * replication_buffer.rs and directory_schema.rs call into these
 * functions for low-level file operations.
 */

/**
 * Opens a new unlinked temporary file in `directory`.
 */
int verneuil__open_temp_file(const char *directory, int mode);

/**
 * Attempts to create a new hardlink to `fd` at `target`.
 */
int verneuil__link_temp_file(int fd, const char *target);

/**
 * Opens a directory as O_PATH.
 */
int verneuil__open_directory(const char *);

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
 * Attempts to acquire an exclusive file-private (OFD) lock on bytes
 * [0, 1) of `fd`.
 *
 * Returns 0 on success, 1 if the lock is taken, -1 on failure.
 */
int verneuil__ofd_lock_exclusive(int fd);
