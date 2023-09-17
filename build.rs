fn build_sqlite() {
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
        // We leave the shared cache enabled: verneuil is incompatible with a WAL
        // DB, so applications may want to use the shared cache mode for to allow
        // more query concurrency.
        .define("SQLITE_USE_ALLOCA", None)
        .define("SQLITE_OMIT_AUTOINIT", None)
        // We only run on recent linux, which has these functions
        .define("HAVE_FDATASYNC", None)
        .define("HAVE_GMTIME_R", None)
        .define("HAVE_LOCALTIME_R", None)
        .define("HAVE_STRCHRNUL", None)
        .define("HAVE_USLEEP", None)
        .define("HAVE_UTIME", None)
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
        .define("SQLITE_ENABLE_STAT4", None)
        .define("SQLITE_ENABLE_UPDATE_DELETE_LIMIT", None)
        // Make sqlite use calloc instead of malloc: the pager likes to
        // malloc(3) full pages, but only populate them partially before
        // fully persisting them to disk.  That leaks nondeterministic
        // garbage to disk, which is bad for privacy and when testing
        // replication for correctness.
        .flag_if_supported("-include replace_malloc.h")
        .include("include")
        .flag_if_supported("-Wno-unused-parameter") // We don't tweak sqlite3.c
        .file("c/sqlite3.c")
        .opt_level(2)
        .compile("verneuil_sqlite")
}

fn main() {
    // If we're building a VFS library for sqlite to load, we do not
    // want to vendor in our own copy of sqlite.
    if cfg!(all(
        feature = "vendor_sqlite",
        not(feature = "dynamic_vfs"),
        not(feature = "test_vfs")
    )) {
        build_sqlite();
    }

    println!("cargo:rerun-if-changed=c/file_ops.c");
    println!("cargo:rerun-if-changed=c/file_ops.h");
    println!("cargo:rerun-if-changed=c/vfs.c");
    println!("cargo:rerun-if-changed=c/vfs.h");
    println!("cargo:rerun-if-changed=include/verneuil.h");
    let mut build = cc::Build::new();
    build
        .flag_if_supported("-Wmissing-declarations")
        .flag_if_supported("-Wmissing-prototypes")
        .flag_if_supported("-Wstrict-prototypes")
        .flag_if_supported("-Wundef")
        .include("include");

    if cfg!(feature = "test_vfs") {
        // Enable test-only code.
        build
            .define("TEST_VFS", None)
            .define("SQLITE_TEST", None)
            // -fcommon to avoid collisions between test-only counters
            // like `sqlite3_sync_count` that must be defined redundantly
            // in vfs.c (and are always defined in SQLite's test binary).
            //
            // This lets us build libverneuil *once* and use it in SQLite
            // test binaries that do and don't define the counters.
            .flag_if_supported("-fcommon");
    }

    if cfg!(feature = "dynamic_vfs") {
        if cfg!(feature = "vendor_sqlite") {
            eprintln!(
                "The Verneuil VFS cannot vendor sqlite: it would conflict with the loader process."
            );
        }
    } else {
        // We're linking this extension statically, without going
        // through sqlite's dynamic loading mechanism.
        build.define("SQLITE_CORE", None);
    }

    // We want GNU extensions on Linux.
    #[cfg(target_os = "linux")]
    build.define("_GNU_SOURCE", None);
    build
        // We know the linuxvfs doesn't implement dirsync.
        .define("SQLITE_DISABLE_DIRSYNC", None)
        .file("c/file_ops.c")
        .file("c/vfs.c")
        .opt_level(2)
        .compile("verneuil_vfs")
}
