#pragma once
#include "verneuil.h"

/*
 * The functions defined in this header are exported by the C side for
 * Rust to call (with corresponding FFI definitions in `lib.rs`).
 *
 * All functions in the public `include/verneuil.h` header *must* be
 * defined in Rust: cargo likes to play with linker visibility tricks.
 */

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
 * Implementation for xClose.
 */
int verneuil__file_close_impl(struct sqlite3_file *);

/**
 * Implementation for xRead.
 */
int verneuil__file_read_impl(struct sqlite3_file *, void *, int, sqlite3_int64);

/**
 * Implementation for xWrite.
 */
int verneuil__file_write_impl(sqlite3_file *, const void *, int, sqlite3_int64);

/**
 * Implementation for xTruncate.
 */
int verneuil__file_truncate_impl(sqlite3_file *, sqlite3_int64);

/**
 * Implementation for xSync.
 */
int verneuil__file_sync_impl(sqlite3_file *, int);

/**
 * Implementation for xFileSize.
 */
int verneuil__file_size_impl(sqlite3_file *, sqlite3_int64 *OUT_size);

/**
 * Implementation for xLock.
 */
int verneuil__file_lock_impl(sqlite3_file *, int level);

/**
 * Implementation for xUnlock.
 */
int verneuil__file_unlock_impl(sqlite3_file *, int level);
