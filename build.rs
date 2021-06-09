fn main() {
    println!("cargo:rerun-if-changed=include/sqlite3.h");
    println!("cargo:rerun-if-changed=include/sqlite3ext.h");
    println!("cargo:rerun-if-changed=c/sqlite3.c");

    cc::Build::new()
        // Apply some recommendations from https://www.sqlite.org/compile.html
        .define("SQLITE_DQS", "0") // Disable double-quoted string literals
        .define("SQLITE_THREADSAFE", "2") // Let the application mutex connections
        .define("SQLITE_DEFAULT_MEMSTATUS", "0") // Do not track memory usage
        .define("SQLITE_DEFAULT_WAL_SYNCHRONOUS", "1") // Sync WAL DBs "NORMAL"ly
        .define("SQLITE_LIKE_DOESNT_MATCH_BLOBS", None) // Don't run LIKE on blobs
        .define("SQLITE_OMIT_DEPRECATED", None)
        .define("SQLITE_OMIT_SHARED_CACHE", None)
        .define("SQLITE_USE_ALLOCA", None)
        .define("SQLITE_OMIT_AUTOINIT", None)
        // We want larger pages for our replication.
        .define("SQLITE_DEFAULT_PAGE_SIZE", "65536")
        .define("SQLITE_MAX_DEFAULT_PAGE_SIZE", "65536")
        // Fast secure delete will zero-fill garbage in pages that
        // would be updated anyway.  The result compresses better.
        .define("SQLITE_FAST_SECURE_DELETE", None)
        // Opening an URI is the easiest way to pipe options to the VFS.
        .define("SQLITE_USE_URI", None)
        // This option is enabled by the default Makefile
        .define("SQLITE_ENABLE_MATH_FUNCTIONS", None)
        // We use a lot of JSON; might be good to push down to sqlite.
        .define("SQLITE_ENABLE_JSON1", None)
        .include("include")
        .flag_if_supported("-Wno-unused-parameter") // We don't tweak sqlite3.c
        .file("c/sqlite3.c")
        .opt_level(2)
        .compile("verneuil_sqlite")
}
