//! Exercises the `Tracker`'s staged-manifest cache end to end, against
//! a database large enough that every snapshot publishes an
//! xor-differenced chunk list.
//!
//! `#[ignore]`d because it takes minutes and ~150MB of scratch; run it
//! with
//!    $ cargo test --release --test cache_smoke -- --ignored --nocapture
//!
//! The unit tests build a `Tracker` by hand and cover the cache's
//! population, lookup and invalidation.  What they cannot do is run
//! real sqlite: this driver puts two connections through real write
//! transactions against one database, rebuilds the replica from the
//! staged manifest -- the same thing `Tracker::compare_snapshot` does
//! under `test_validate_writes` -- and byte-compares it with the
//! source file after every one of them.
//!
//! The only replication target is `Local`, for which the copier
//! creates no upload bucket -- so no chunk ever leaves the spool.
//! That is *why* `assert_replica_matches` can rebuild from the spool's
//! own chunk directories with an empty target list.  It also means
//! the copier's upload-and-delete path is never exercised here.
//!
//! The database is deliberately past the base-chunk threshold.  Below
//! it there is no base chunk, the published chunk list is not
//! differenced, and a cache that stored the published list rather than
//! the decoded one would look perfectly correct.
//!
//! What this does *not* catch, so that nobody reads more into a green
//! run than is there:
//!
//! - The interleaved rounds do not, on their own, test the
//!   staged-manifest identity check.  Both connections there write, so
//!   each republish also moves the db file's version xattr, and
//!   `judge_current_manifest`'s version id comparison would reject the
//!   stale base as a backstop even if the identity check were gone.
//!   The last round is what covers it: a republish need not move the
//!   version id, and once the staged chunks are gone with the
//!   manifest, a tracker that trusted its cached base publishes
//!   fingerprints for chunks that no longer exist.  Measured -- with
//!   the identity check removed, rounds 0-14 pass and the last one
//!   fails to rebuild the replica.
//! - In a production build the randomised scrub in `snapshot_chunks`
//!   repairs a poisoned chunk list, so a wrong cached value does not
//!   show up as a diverging replica: it shows up as "forcing
//!   resynchronisation scan", and a full re-stage of the database on
//!   every commit.  That is a `tracing` ERROR, so this driver counts
//!   ERROR events itself and fails on any of them -- the count is
//!   taken before `RUST_LOG` filtering, so it works whatever the
//!   environment says.  A `test_vfs` build turns the same mistake
//!   into an assertion failure instead.
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

/// Counts `tracing` ERROR events, and remembers what they said.
///
/// The signal this driver is really looking for -- "forcing
/// resynchronisation scan", logged by `snapshot_chunks` when a chunk's
/// fingerprint disagrees with the snapshot's idea of it -- is repaired
/// by the scrub, so it never reaches the replica comparison.  Counting
/// it is what turns "watch stderr by hand" into a test.  Deliberately
/// *not* behind the `EnvFilter`: a driver whose only oracle could be
/// switched off by `RUST_LOG` would be worse than useless.
#[derive(Clone, Default)]
struct ErrorCounter {
    count: Arc<AtomicUsize>,
    messages: Arc<Mutex<Vec<String>>>,
}

impl<S: tracing::Subscriber> Layer<S> for ErrorCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::ERROR {
            return;
        }

        self.count.fetch_add(1, Ordering::Relaxed);

        struct Capture(String);
        impl tracing::field::Visit for Capture {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{:?}", value);
                }
            }
        }

        let mut capture = Capture(String::new());
        event.record(&mut capture);
        self.messages
            .lock()
            .expect("lock must be valid")
            .push(capture.0);
    }
}

/// Enough 64KB chunks that `reencode_flattened_chunks` emits a base
/// chunk, so every snapshot publishes a differenced chunk list.
///
/// `BASE_CHUNK_MIN_LENGTH` is 600, but it is compared against the
/// *flattened word* count -- two words per fingerprint -- so today the
/// real threshold is 300 chunks.  Size this past 600 chunks
/// so the driver keeps covering the differenced path whichever way
/// that discrepancy is eventually resolved, and assert on the base
/// chunk itself rather than trusting the arithmetic.
const TARGET_ROWS: i64 = 640;

/// Lists a directory, tolerating its disappearance: the copier
/// renames and removes spool subdirectories on its own thread.
fn read_dir(dir: &Path) -> impl Iterator<Item = std::fs::DirEntry> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
}

/// Collects every "chunks" directory under the spool tree.  Chunks
/// move between "staging", "ready" and "consuming" as the copier runs,
/// and the manifest may reference any of them.
fn chunk_dirs(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in read_dir(root) {
        let path = entry.path();

        if path.is_dir() {
            if path
                .file_name()
                .map(|name| name == "chunks")
                .unwrap_or(false)
            {
                out.push(path.clone());
            }

            chunk_dirs(&path, out);
        }
    }
}

/// Empties every "staging" subtree under the spool, chunks and
/// manifest alike.
///
/// This is the state an operator leaves behind by clearing the spool
/// directory.  (`copier::force_full_snapshot` is *not* this: it empties
/// "staging/meta" and leaves the chunks alone, which is the round
/// above -- and that one does not discriminate, because with no other
/// writer the stale base still describes the current data.)  The next
/// snapshot must notice and rebuild from scratch; one that trusts a
/// cached base instead publishes fingerprints for chunks that are gone.
fn wipe_staging(root: &Path) {
    fn empty(dir: &Path) {
        // `read_dir` can fail here through no fault of ours: the
        // copier renames "ready" to "consuming" and removes them
        // once empty, on its own thread, after every snapshot.
        for entry in read_dir(dir) {
            let path = entry.path();
            if path.is_dir() {
                empty(&path);
            } else {
                std::fs::remove_file(&path).expect("must remove staged file");
            }
        }
    }

    fn walk(dir: &Path) {
        // `read_dir` can fail here through no fault of ours: the
        // copier renames "ready" to "consuming" and removes them
        // once empty, on its own thread, after every snapshot.
        for entry in read_dir(dir) {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            if path
                .file_name()
                .map(|name| name == "staging")
                .unwrap_or(false)
            {
                // Files, not directories: the tracker needs the
                // directories to still be there to stage into, and
                // removing them instead produces a flurry of unrelated
                // "failed to create temporary file" errors.
                empty(&path);
                continue;
            }

            walk(&path);
        }
    }

    walk(root);
}

/// Finds the single staged manifest under the spool tree.
fn staged_manifest(root: &Path) -> PathBuf {
    fn walk(dir: &Path, found: &mut Vec<PathBuf>) {
        // `read_dir` can fail here through no fault of ours: the
        // copier renames "ready" to "consuming" and removes them
        // once empty, on its own thread, after every snapshot.
        for entry in read_dir(dir) {
            let path = entry.path();

            if path.is_dir() {
                walk(&path, found);
                continue;
            }

            let in_dir = |path: &Path, depth: usize, name: &str| -> bool {
                let mut parent = path.parent();
                for _ in 0..depth {
                    parent = parent.and_then(Path::parent);
                }

                parent
                    .and_then(Path::file_name)
                    .map(|found| found == name)
                    .unwrap_or(false)
            };

            if in_dir(&path, 0, "meta") && in_dir(&path, 1, "staging") {
                found.push(path);
            }
        }
    }

    let mut found = Vec::new();
    walk(root, &mut found);
    assert_eq!(found.len(), 1, "expected one staged manifest: {:?}", found);
    found.pop().expect("checked above")
}

/// Rebuilds the database from the currently staged manifest and
/// byte-compares it with the source file.
///
/// Returns whether that manifest carried a base chunk, i.e. whether
/// its chunk list was xor-differenced on disk.  That is the case this
/// driver exists to cover, and it is worth checking rather than
/// inferring from the database's size.
fn assert_replica_matches(spool: &Path, db: &str, context: &str) -> bool {
    let mut dirs = Vec::new();
    chunk_dirs(spool, &mut dirs);

    let builder = || {
        let mut builder = kismet_cache::CacheBuilder::new();
        for dir in &dirs {
            builder.plain_reader(dir);
        }
        builder
    };

    let bytes = std::fs::read(staged_manifest(spool)).expect("must read staged manifest");
    let (manifest, base_chunk) =
        verneuil::Manifest::decode_and_validate(&bytes, builder(), None, "replica")
            .expect("staged manifest must decode");

    // `decode_and_validate` returns the base chunk it had to fetch to
    // undo the xor, so `Some` means the staged manifest really was
    // differenced.  Note it before the chunk is handed to the snapshot.
    let was_differenced = base_chunk.is_some();

    let snapshot = verneuil::Snapshot::new(
        verneuil::SnapshotLoadingPolicy::Default,
        builder(),
        &[],
        &manifest,
        base_chunk,
    )
    .expect("snapshot must load");

    let mut replicated = Vec::new();
    snapshot
        .as_read(0, u64::MAX)
        .expect("must open snapshot reader")
        .read_to_end(&mut replicated)
        .expect("must read snapshot");

    let expected = std::fs::read(db).expect("must read db file");

    assert_eq!(
        replicated.len(),
        expected.len(),
        "{}: replica length differs",
        context
    );
    assert!(
        replicated == expected,
        "{}: replica contents differ",
        context
    );

    was_differenced
}

fn open(db: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_with_flags_and_vfs(
        db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        "verneuil",
    )
    .expect("must open db");

    conn.pragma(None, "page_size", 65536, |_| Ok(()))
        .expect("must set page size");
    // Verneuil's VFS does not support the WAL.
    conn.pragma(None, "journal_mode", "delete", |_| Ok(()))
        .expect("must set journal mode");
    conn
}

/// Rewrites `count` rows, then flushes the replication data so the
/// staged manifest is up to date.
fn write_rows(conn: &mut rusqlite::Connection, round: i64, count: i64) {
    let tx = conn.transaction().expect("must begin");

    for i in 0..count {
        let id = (round * 37 + i * 101) % TARGET_ROWS;
        tx.execute(
            "UPDATE test SET value = ? WHERE id = ?",
            rusqlite::params![vec![(round + i) as u8; 60_000 + round as usize], id],
        )
        .expect("must update");
    }

    tx.commit().expect("must commit");
    conn.pragma(None, "verneuil_flush_replication_data", 2, |_| Ok(()))
        .expect("must flush");
}

#[test]
#[ignore = "takes minutes and ~150MB of scratch; run with -- --ignored"]
fn cache_smoke() {
    let scratch = tempfile::tempdir().expect("must create scratch dir");
    let dir = scratch.path().to_str().expect("utf-8 tempdir").to_owned();
    let db = format!("{}/smoke.db", dir);
    let spool = format!("{}/spool", dir);

    let errors = ErrorCounter::default();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .compact()
                .with_filter(tracing_subscriber::EnvFilter::from_default_env()),
        )
        .with(errors.clone())
        .init();

    let _ = std::fs::remove_file(&db);

    verneuil::configure(verneuil::Options {
        make_default: false,
        cache_staged_manifest: true,
        replication_spooling_dir: Some(spool.clone().into()),
        replication_targets: vec![verneuil::replication_target::ReplicationTarget::Local(
            verneuil::replication_target::LocalReplicationTarget {
                directory: format!("{}/cache", dir).into(),
                num_shards: 16,
                capacity: 1_000_000,
            },
        )],
        ..Default::default()
    })
    .expect("must configure verneuil");

    let mut first = open(&db);
    first
        .execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, value BLOB NOT NULL);")
        .expect("must create table");

    {
        let tx = first.transaction().expect("must begin");
        for i in 0..TARGET_ROWS {
            tx.execute(
                "INSERT INTO test(id, value) VALUES(?, ?)",
                rusqlite::params![i, vec![(i % 251) as u8; 65_000]],
            )
            .expect("must insert");
        }
        tx.commit().expect("must commit");
    }
    first
        .pragma(None, "verneuil_flush_replication_data", 2, |_| Ok(()))
        .expect("must flush");

    let len = std::fs::metadata(&db).expect("must stat db").len();
    println!("db is {} bytes ({} chunks)", len, len / (1 << 16));

    let mut differenced = 0usize;
    let mut snapshots = 0usize;
    let mut check = |context: &str| {
        snapshots += 1;
        if assert_replica_matches(Path::new(&spool), &db, context) {
            differenced += 1;
        }
        println!("{} matches", context);
    };

    check("initial snapshot");

    // Steady state on one connection: every snapshot after the first
    // finds its own manifest staged, and serves the cached value.
    for round in 0..6 {
        write_rows(&mut first, round, 5);
        check(&format!("round {}", round));
    }

    // Now interleave a second connection: the realistic shape, where
    // each connection's cache entry is superseded by the other's
    // publish.  Every one of these rounds is a cache miss, taken by
    // the identity check -- it runs first, at the top of
    // `judge_current_manifest`.  See the note at the top of this file
    // for why a green run still doesn't prove that check works.
    let mut second = open(&db);

    for round in 6..14 {
        let (writer, who) = if round % 2 == 0 {
            (&mut first, "first")
        } else {
            (&mut second, "second")
        };

        write_rows(writer, round, 5);
        check(&format!("round {} ({} connection)", round, who));
    }

    // Finally, the staged manifest disappearing without the db file's
    // version id moving -- what `copier::force_full_snapshot` does when
    // clearing the version xattr fails, and what clearing the spool by
    // hand does.  The cached entry still matches on version id, so the
    // identity check is the only thing that rejects it.
    //
    // Deleting only the manifest does not discriminate: with no other
    // writer the stale base still describes the current data, so the
    // snapshot it produces is correct anyway.  Taking the chunks with
    // it does.  Measured mechanism, with the identity check removed:
    // the reused base's `Reuse` arm republishes `base_chunks_fprint`
    // without re-staging the base chunk this wipe just deleted, so the
    // staged manifest can no longer be decoded at all -- the rebuild
    // fails with "unable to find chunk data".  This is the round that
    // makes a green run mean something about the identity check.
    std::fs::remove_file(staged_manifest(Path::new(&spool))).expect("must remove staged manifest");
    write_rows(&mut first, 14, 5);
    check("round 14 (staged manifest deleted)");

    wipe_staging(Path::new(&spool));
    write_rows(&mut first, 15, 5);
    check("round 15 (staging wiped)");

    // If the database stopped being big enough to warrant a base
    // chunk, every snapshot above would publish an undifferenced chunk
    // list -- the one shape in which caching the published list
    // instead of the decoded one looks perfectly correct -- and this
    // driver would pass while testing nothing it was written for.
    println!(
        "{} of {} snapshots were differenced",
        differenced, snapshots
    );
    assert!(
        differenced > snapshots / 2,
        "most snapshots must carry a base chunk; raise TARGET_ROWS"
    );

    // A snapshot built on a wrong base does not corrupt the replica --
    // the scrub notices and re-stages the whole database -- so this,
    // not the comparison above, is what catches a bad cached value.
    let count = errors.count.load(Ordering::Relaxed);
    assert_eq!(
        count,
        0,
        "expected no tracing ERROR events, saw {}: {:?}",
        count,
        errors.messages.lock().expect("lock must be valid")
    );

    println!("OK");
}
