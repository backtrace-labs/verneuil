#pragma once
#include "verneuil.h"

#include <assert.h>
#include <stdint.h>

struct linux_file;
struct snapshot_file;

/*
 * The functions defined in this header are exported by the C side for
 * Rust to call (with corresponding FFI definitions in `lib.rs` and
 * `vfs_ops.rs`).
 *
 * All functions in the public `include/verneuil.h` header *must* be
 * defined in Rust: cargo likes to play with linker visibility tricks.
 */
static_assert(sizeof(sqlite3_int64) == sizeof(int64_t),
    "vfs_ops.rs uses i64 for sqlite3_int64");

/**
 * Configures the C half of the Verneuil VFS.
 */
int verneuil_configure_impl(const struct verneuil_options *options);

/**
 * Implements the registration hook for Sqlite's extension loading
 * mechanism.
 */
int verneuil_init_impl(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi);

/**
 * Test-only: registers the Verneuil VFS as the new default, and
 * shadows the Unix VFS with a copy of the Verneuil VFS.
 */
int verneuil_test_only_register(void);

/**
 * Performs additional Rust-side initialisation on a fully-initialised
 * linux_file for a main db.
 *
 * Returns 0 on success, non-zero on error.
 */
int verneuil__file_post_open(struct linux_file *);

/**
 * Attempts to open a snapshot file for the directory proto at `path`.
 */
int verneuil__snapshot_open(struct snapshot_file *, const char *path);

/**
 * The base implementations for these methods (`verneuil__file_..._impl`)
 * are defined in vfs.c, and perform the actual work.  They're
 * directly called for all files except main database files.
 *
 * The Rust implementations (`verneuil__file_*`, without the `_impl`
 * suffix) are defined in `vfs_ops.rs`, and end up calling the base
 * `_impl` functions.  These delegation functions are only called for
 * main database files.
 */

/**
 * Implementation for xClose.
 */
int verneuil__file_close_impl(struct sqlite3_file *);

/**
 * Rust implementation for xClose.  Cleans up any state initialised by
 * `verneuil__file_post_open` before calling `verneuil__file_close_impl`.
 */
int verneuil__file_close(struct sqlite3_file *);
int verneuil__snapshot_close(struct sqlite3_file *);

/**
 * Implementation for xRead.
 */
int verneuil__file_read_impl(struct sqlite3_file *, void *, int, sqlite3_int64);

/**
 * Rust implementation for xRead.
 */
int verneuil__file_read(struct sqlite3_file *, void *, int, sqlite3_int64);
int verneuil__snapshot_read(struct sqlite3_file *, void *, int, sqlite3_int64);

/**
 * Implementation for xWrite.
 */
int verneuil__file_write_impl(sqlite3_file *, const void *, int, sqlite3_int64);

/**
 * Rust implementation for xWrite.
 */
int verneuil__file_write(sqlite3_file *, const void *, int, sqlite3_int64);
int verneuil__snapshot_write(sqlite3_file *, const void *, int, sqlite3_int64);

/**
 * Implementation for xTruncate.
 */
int verneuil__file_truncate_impl(sqlite3_file *, sqlite3_int64);

/**
 * Rust implementation for xTruncate.
 */
int verneuil__file_truncate(sqlite3_file *, sqlite3_int64);
int verneuil__snapshot_truncate(sqlite3_file *, sqlite3_int64);

/**
 * Implementation for xSync.
 */
int verneuil__file_sync_impl(sqlite3_file *, int);

/**
 * Rust implementation for xSync.
 */
int verneuil__file_sync(sqlite3_file *, int);
int verneuil__snapshot_sync(sqlite3_file *, int);

/**
 * Implementation for xFileSize.
 */
int verneuil__file_size_impl(sqlite3_file *, sqlite3_int64 *OUT_size);

/**
 * Rust implementation for xFileSize.
 */
int verneuil__file_size(sqlite3_file *, sqlite3_int64 *OUT_size);
int verneuil__snapshot_size(sqlite3_file *, sqlite3_int64 *OUT_size);

/**
 * Implementation for xLock.
 */
int verneuil__file_lock_impl(sqlite3_file *, int level);

/**
 * Rust implementation for xLock.
 */
int verneuil__file_lock(sqlite3_file *, int level);
int verneuil__snapshot_lock(sqlite3_file *, int level);

/**
 * Implementation for xUnlock.
 */
int verneuil__file_unlock_impl(sqlite3_file *, int level);

/**
 * Rust implementation for xUnlock.
 */
int verneuil__file_unlock(sqlite3_file *, int level);
int verneuil__snapshot_unlock(sqlite3_file *, int level);
