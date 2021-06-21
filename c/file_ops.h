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
