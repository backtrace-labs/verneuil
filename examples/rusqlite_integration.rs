/// This example shows how to integrate verneuil with a Rust application
/// that interacts with sqlite via rusqlite.
///
/// Run it with
///    $ MAKE_DEFAULT=true DEFAULT_REGION=us-east-2 DEFAULT_CHUNK_BUCKET=... DEFAULT_MANIFEST_BUCKET=... cargo run --example rusqlite_integration -- a b c d
///       Compiling verneuil v0.1.2 (/home/pkhuong/verneuil)
///        Finished dev [unoptimized + debuginfo] target(s) in 10.06s
///         Running `target/debug/examples/rusqlite_integration a b c d`
///    Opening /tmp/foo.db with verneuil default VFS
///    From local sqlite file
///    0 => target/debug/examples/rusqlite_integration
///    1 => a
///    2 => b
///    3 => c
///    4 => d
///    From replica
///    0 => target/debug/examples/rusqlite_integration
///    1 => a
///    2 => b
///    3 => c
///    4 => d
fn main() {
    use tracing_subscriber::EnvFilter;

    // Direct `tracing` output to stderr (configure via the `RUST_LOG`
    // environment variable).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .compact()
        .init();

    // Figure out the config from the `VERNEUIL_CONFIG` environmen
    // variables, or hardcoded values here.
    let options =
        verneuil::load_configuration_from_env(None).unwrap_or_else(|| verneuil::Options {
            make_default: env!("MAKE_DEFAULT") == "true",
            replication_spooling_dir: Some("/tmp/verneuil-spool".into()),
            replication_targets: vec![
                verneuil::replication_target::ReplicationTarget::S3(
                    verneuil::replication_target::S3ReplicationTarget {
                        region: env!("DEFAULT_REGION").into(),
                        endpoint: None,
                        // You will want to rename these two buckets to
                        // something for which you have credentials.
                        chunk_bucket: env!("DEFAULT_CHUNK_BUCKET").into(),
                        manifest_bucket: env!("DEFAULT_MANIFEST_BUCKET").into(),
                        domain_addressing: true,
                        create_buckets_on_demand: false,
                    },
                ),
                // This local replication target is mostly for the
                // read replica's benefit: it's useful to cache chunks
                // instead of always fetching from S3.  However, it
                // doesn't hurt to use the same configuration for the
                // read-write (replicating) Verneuil VFS: writers will
                // directly populate the cache.
                verneuil::replication_target::ReplicationTarget::Local(
                    verneuil::replication_target::LocalReplicationTarget {
                        directory: "/tmp/verneuil-cache".into(),
                        num_shards: 100,
                        capacity: 10000,
                    },
                ),
            ],
            ..Default::default()
        });

    verneuil::configure(options.clone()).unwrap();

    let mut conn = if options.make_default {
        println!("Opening /tmp/foo.db with verneuil default VFS");
        rusqlite::Connection::open("/tmp/foo.db").unwrap()
    } else {
        println!("Opening /tmp/foo.db with explicit verneuil VFS");
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "/tmp/foo.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            "verneuil",
        )
        .unwrap();

        // If we use Verneuil's vendored sqlite, we get 64K pages by
        // default.  Otherwise, we should explicitly bump that up.
        //
        // When the database might already exist, the pragma will only
        // take effect after a `VACUUM` statement.
        conn.pragma(None, "page_size", 65536, |_| Ok(())).unwrap();

        conn
    };

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS test (
           id INTEGER PRIMARY KEY,
           value TEXT NOT NULL
        );
        DELETE FROM test;",
    )
    .unwrap();

    // We can flush synchronously.
    if let Some(path) = options.replication_spooling_dir.as_deref() {
        verneuil::copy_all_spool_paths(
            std::path::Path::new(path).to_owned(),
            /*best_effort=*/ false,
        )
        .unwrap();
    }

    // And now that there is some replicated state, we can open a
    // replica.
    let replica = rusqlite::Connection::open_with_flags_and_vfs(
        // Open the replica for a file on the current machine, at
        // `/tmp/foo.db`.
        "verneuil:///tmp/foo.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        "verneuil_snapshot",
    )
    .unwrap();

    // Populate the local db.
    {
        let tx = conn.transaction().unwrap();

        for (i, value) in std::env::args().enumerate() {
            tx.execute(
                "INSERT INTO test(id, value) VALUES(?, ?)",
                rusqlite::params![i, value],
            )
            .unwrap();
        }

        tx.commit().unwrap();
    }

    // Dump the contents of the db.
    println!("From local sqlite file");
    for (id, value) in conn
        .prepare("SELECT id, value FROM test")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<usize, i64>(0).unwrap(),
                row.get::<usize, String>(1).unwrap(),
            ))
        })
        .unwrap()
        .flatten()
    {
        println!("{} => {}", id, value);
    }

    // There might be a delay (the copier attempts to only
    // copy updated data slighly less than once a second),
    // but the replication data should be updated eventually.
    std::thread::sleep(std::time::Duration::from_secs(2));

    // And now dump the contents of the replica.
    println!("From replica");
    // Force a synchronous refresh.  See `doc/SNAPSHOT_VFS.md` for
    // more details on the pragmas.
    replica
        .pragma(None, "verneuil_snapshot_refresh", 2, |_| Ok(()))
        .unwrap();

    for (id, value) in replica
        .prepare("SELECT id, value FROM test")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<usize, i64>(0).unwrap(),
                row.get::<usize, String>(1).unwrap(),
            ))
        })
        .unwrap()
        .flatten()
    {
        println!("{} => {}", id, value);
    }
}
