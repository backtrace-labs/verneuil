#pragma once

#include <sqlite3ext.h>
#include <stdbool.h>

/**
 * This initialisation function will be called by sqlite when the VFS
 * is loaded as a runtime extension.
 */
int sqlite3_linuxvfs_init(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi);

/**
 * Registers the Linux VFS as the new default VFS.
 *
 * Returns SQLITE_OK on success, and a sqlite error code on failure.
 *
 * Only implemented when built with `-DSQLITE_CORE`, which must only
 * be set when the application will link directly with the VFS and
 * sqlite.  Regular builds, without `-DSQLITE_CORE` should instead
 * be loaded via sqlite's runtime extension mechanism.
 */
int sqlite3_linuxvfs_register(const char *unused);
