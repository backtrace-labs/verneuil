#pragma once

/**
 * replication_buffer.rs calls into these functions for low-level file
 * operations.
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
 * Attempts to exchange the files or directories at `x` and `y`.
 */
int verneuil__exchange_paths(const char *x, const char *y);

/**
 * Opens a directory as O_PATH.
 */
int verneuil__open_directory(const char *);
