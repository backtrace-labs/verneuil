#pragma once
#include <sqlite3ext.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct verneuil_options {
        bool make_default;

        /*
         * NULL leaves the temporary directory to its default
         * value.
         */
        const char *tempdir;

        /*
         * NULL disables replication by default. A non-NULL value
         * enables it by default; all replication buffers will live in
         * subdirectories of that staging directory.
         */
        const char *replication_spooling_dir;

        /*
         * If non-zero, the permission mask for the replication
         * directory if it must be created.
         *
         * Zero defaults to 0700.
         */
        uint32_t replication_spooling_dir_permissions;

        /*
         * Optional JSON-encoded options.  The fields directly in
         * `verneuil_options` always take priority over the JSON.
         *
         * If the string is of the form "@/path/to/file", the
         * contents of that file are instead parsed as JSON-encoded
         * options.
         *
         * In particular, `make_default` is a no-op in the JSON.
         *
         * Fields (see `struct Options` in lib.rs and `struct
         * ReplicationTarget` in replication_target.rs for more
         * details):
         *
         *  - make_default: ignored
         *
         *  - tempdir: optional, directory for temporary sqlite files.
         *    (the files are never linked in the filesystem, but this
         *    option directs what filesystem backs their storage).
         *
         *  - replication_staging_dir: optional, path prefix for
         *    replication data.  Must be set to enable replication.
         *
         *  - replication_targets: array of sinks for replication
         *    data.  Each entry should look like
         *        {
         *          "s3":
         *          {
         *            "region": "us-east-1",
         *            "chunk_bucket": "verneuil_chunks",
         *            "directory_buckets": "verneuil_directories",
         *            "domain_addressing": false
         *          }
         *        }
         */
        const char *json_options;
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
 * Information about the replication proto for a given sqlite db file.
 *
 * The `blob_name` is a C string and should always be available on
 * success.  All remaining fields may be 0 on failure.
 */
struct verneuil_replication_info {
        /* Name of the corresponding blob in remote storage. */
        char *blob_name;

        /* Metadata from the blob proto. */
        uint64_t header_fprint[2];
        uint64_t contents_fprint[2];
        uint64_t ctime;
        uint32_t ctime_ns;

        /*
         * Actual protobuf-encoded payload in the directory blob, if
         * known.
         */
        size_t num_bytes;
        char *bytes;
};

/**
 * Populates `dst` with the replication information for sqlite file `db`,
 * inside the `spool_prefix` spooling directory (or `NULL` for the
 * default).
 *
 * Returns 0 on success, -1 on failure.
 */
int verneuil_replication_info_for_db(struct verneuil_replication_info *dst,
    const char *db, const char *spool_prefix);

/**
 * Releases resources owned by `info`.
 *
 * Safe to call on zero-initialised or NULL `info`.
 */
void verneuil_replication_info_deinit(struct verneuil_replication_info *info);
