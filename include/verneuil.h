#pragma once
#include <sqlite3ext.h>
#include <stdbool.h>

struct verneuil_options {
        bool make_default;
        /*
         * NULL leaves the temporary directory to its default
         * value.
         */
        const char *tempdir;
};

/**
 * Initializes the Verneuil VFS and registers it with sqlite.
 *
 * @returns 0 on success, a sqlite error code on failure.
 *
 * When built without `-DSQLITE_CORE`, this function should only be called
 * after sqlite3 has loaded the verneuil module dynamically, in order to
 * apply configuration options.
 *
 * If `tempdir` could not be overridden (it was already constructed
 * before the call to `verneuil_configure`), returns SQLITE_LOCKED.
 */
int verneuil_configure(const struct verneuil_options *options);

/**
 * This initialisation function will be called by sqlite when the VFS
 * is loaded as a runtime extension.
 */
int sqlite3_verneuil_init(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi);

/**
 * Registers the Verneuil VFS as the new default VFS.
 *
 * Returns SQLITE_OK on success, and a sqlite error code on failure.
 *
 * Only implemented when built with `-DSQLITE_CORE`, which must only
 * be set when the application will link directly with the VFS and
 * sqlite.  Regular builds, without `-DSQLITE_CORE` should instead
 * be loaded via sqlite's runtime extension mechanism.
 */
int sqlite3_verneuil_register(const char *unused);
