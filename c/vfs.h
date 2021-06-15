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
