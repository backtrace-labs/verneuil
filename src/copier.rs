//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
use core::num::NonZeroU32;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::future;
use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use kismet_cache::Cache;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use tracing::info_span;
use tracing::instrument;
use tracing::Level;

use crate::chain_debug;
use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::drop_result;
use crate::executor::call_with_executor;
use crate::filtered_io_error;
use crate::fresh_error;
use crate::fresh_warn;
use crate::manifest_schema::clear_version_id;
use crate::manifest_schema::parse_manifest_chunks;
use crate::manifest_schema::parse_manifest_info;
use crate::ofd_lock::OfdLock;
use crate::racy_time::RacySystemTime;
use crate::recent_work_set::RecentWorkSet;
use crate::recent_work_set::WorkUnit;
use crate::replication_buffer;
use crate::replication_target::apply_local_cache_replication_target;
use crate::replication_target::parse_s3_region_specification;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::ReplicationTargetList;
use crate::replication_target::S3ReplicationTarget;
use crate::result::Result;

const CHUNK_CONTENT_TYPE: &str = "application/octet-stream";

/// Rate limit for individual blob uploads.
///
/// We want to guarantee a low average rate (e.g., 30 per second)
/// to bound our costs, but also avoid slowing down replication in
/// the common case, when write transactions are rare.
const COPY_RATE_QUOTA: governor::Quota =
    governor::Quota::per_second(unsafe { NonZeroU32::new_unchecked(30) })
        .allow_burst(unsafe { NonZeroU32::new_unchecked(100) });

/// Rate limit when copying synchronously.
///
/// We allow a much higher rate because synchronous upload isn't a
/// background task.  If it's user-initiated, it should complete ASAP:
/// someone actively decided they want to that data copied.
const SYNCHRONOUS_COPY_RATE_QUOTA: governor::Quota =
    governor::Quota::per_second(unsafe { NonZeroU32::new_unchecked(1000) })
        .allow_burst(unsafe { NonZeroU32::new_unchecked(1000) });

/// Whenever we are rate-limited, add up to this fraction of the base
/// delay to our sleep duration.
const RATE_LIMIT_SLEEP_JITTER_FRAC: f64 = 1.0;

/// We upload 64KB chunks, so uploads should be fast.  Impose
/// a short timeout after which we should abort the request
/// and try again.
const COPY_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How many times do we retry on transient errors?
const COPY_RETRY_LIMIT: i32 = 3;

/// Initial back-off delay (+/- jitter).
const COPY_RETRY_BASE_WAIT: Duration = Duration::from_millis(100);

/// Grow the back-off delay by `COPY_RETRY_MULTIPLIER` after
/// consecutive failures.
const COPY_RETRY_MULTIPLIER: f64 = 10.0;

/// Remember up to this many recent copy requests for deduplication.
///
/// This value should ideally be proportional to the maximum copy rate
/// (`COPY_RATE_QUOTA`) and `COPY_REQUEST_MIN_AGE`.
const COPY_REQUEST_MEMORY: usize = 30 * 3600;

/// Avoid executing duplicate copy requests that are separated by less
/// than `COPY_REQUEST_MIN_AGE`.
const COPY_REQUEST_MIN_AGE: Duration = Duration::from_secs(3600);

/// When comparing against `COPY_REQUEST_MIN_AGE`, jitter the
/// timestamp by up to this duration.
const COPY_REQUEST_JITTER: Duration = Duration::from_secs(600);

/// Perform background work for one spooling directory approximately
/// once per BACKGROUND_SCAN_PERIOD.
const BACKGROUND_SCAN_PERIOD: Duration = Duration::from_secs(5);

/// Let at least this much time elapse between copies for a given
/// spooling directory (background or otherwise).
const MIN_COPY_PERIOD: Duration = Duration::from_millis(500);

/// Make sure at least this much time elapses between uploads of the
/// same manifest blob: some S3-compatible blob store have trouble
/// with ordering of PUTs that are too close in real time.
///
/// For example,
/// https://www.backblaze.com/b2/docs/s3_compatible_api.html says:
///
/// "When uploading multiple versions of the same file within the same
/// second, the possibility exists that the processing of these
/// versions may not be in order. Backblaze recommends delaying
/// uploads of multiple versions of the same file by at least one
/// second to avoid this situation."
///
/// (Might see some noise on this issue
/// https://github.com/s3fs-fuse/s3fs-fuse/issues/272 when Backblaze
/// fixes this).
///
/// We thus impose a delay slightly greater than one second between
/// the end of the last manifest upload and the start of the next.
const MIN_MANIFEST_COPY_DELAY: Duration = Duration::from_millis(1100);

/// When we fail to find credentials, sleep for at least this long
/// before trying again: it's not particularly useful to log that we
/// couldn't acquire credentials multiple times a second, especially
/// when the fix usually involves manual action.
const FAILED_CREDENTIALS_SLEEP: Duration = Duration::from_secs(60);

/// Wait at least this long (and more for random jitter) when we
/// fail to acquire the copy lock for a spooling directory.
///
/// During testing, the p99 for blob uploads on S3 and GCS is < 1
/// second, and even the worst case latency over a few days was < 1.8
/// seconds on S3.  Waiting a bit should suffice to make it unlikely
/// that a fresh upload will race in front of a slow stale upload.
/// However, even if that were to happen, we should eventually
/// recover, since copiers always eventually upload from staging.
const COPY_LOCK_CONTENTION_WAIT: Duration = Duration::from_secs(2);

/// When we repeatedly fail to acquire the copy lock, try to reset it
/// (delete the lock file) at this rate.  Resetting the copy lock lets
/// most copier processes make use of the lock when only one process
/// is misbehaving.
const COPY_LOCK_RESET_RATE: f64 = 0.01;

/// List this many files in a directory before trying to consume them
/// all.  That's helpful because consuming files is much slower than
/// adding new ones, so it's easy for a copier to get stuck consuming
/// from an apparently neverending directory of files.
const CONSUME_DIRECTORY_BATCH_SIZE: usize = 10;

/// The maximum number of concurrent file consumption tasks allowed
/// for a single call to `consume_directory`.
const CONSUME_DIRECTORY_CONCURRENCY_LIMIT: usize = 100;

/// We aim for ~30 requests / second.  We've seen a p99 of ~300 ms per
/// PUT, so 10 workers should be enough, even with additional sleeping
/// and backoff.
const WORKER_COUNT: usize = 10;

/// How many of the last manifest files we have uploaded must we
/// remember?  This memory lets us avoid repeated uploads of the
/// same "staged" manifest file.
const STAGED_MANIFEST_MEMORY: usize = 2;

/// Try to identify lagging replication roughly at this period; we
/// want a small offset from exactly one minute to avoid synchronising
/// with other processes that trigger behaviour every minute.
const REPLICATION_LAG_REPORT_PERIOD: Duration = Duration::from_millis(60_325);

/// Warn about DB files for which replication is this far behind.
const REPLICATION_LAG_REPORT_THRESHOLD: Duration = Duration::from_secs(120);

/// Time background "touch" to fully cover each db file once per period.
const PATROL_TOUCH_PERIOD: Duration = Duration::from_secs(24 * 3600);

/// Compression level for zstd. 0 maps to zstd's internal default
/// compression level.
const ZSTD_COMPRESSION_LEVEL: i32 = 0;

/// Disable compression with a negative level value.
const ZSTD_COMPRESSION_DISABLE: i32 = -1;

lazy_static::lazy_static! {
    static ref RECENT_WORK: Mutex<RecentWorkSet> = Mutex::new(RecentWorkSet::new(COPY_REQUEST_MEMORY, COPY_REQUEST_JITTER));
}

/// Messages to maintain the set of active spooling directory: `Join`
/// adds a new directory, `Leave` removes it.  There may be multiple
/// copiers for the same directory, so we must refcount them.
///
/// The optional secondary value for `Join` is a path to the
/// replicated file.
enum ActiveSetMaintenance {
    Join(Arc<PathBuf>, Option<PathBuf>),
    Leave(Arc<PathBuf>),
}

/// A `Copier` is only a message-passing handle to a background worker
/// thread.
///
/// When all the underlying `Sender` have been dropped, the thread
/// will be notified and commence shutdown.
#[derive(Debug)]
pub(crate) struct Copier {
    ready_buffers: Sender<Arc<PathBuf>>,
    maintenance: Sender<ActiveSetMaintenance>,
    spool_path: Option<Arc<PathBuf>>,
}

type Governor = governor::RateLimiter<
    governor::state::direct::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::MonotonicClock,
>;

/// An opaque identifier for a file's contents inode.
#[derive(Debug, Eq, PartialEq, Hash)]
struct FileIdentifier {
    btime: Option<std::time::SystemTime>,
    len: u64,
    dev: u64,
    ino: u64,
    ctime: i64,
    ctime_nsec: i64,
}

/// This state belongs to the copier worker that is currently
/// uploading the contents of a spool directory for that database
/// file.
#[derive(Debug, Default)]
struct CopierUploadState {
    // We always acquire the lock that surrounds `CopierUploadState`
    // first around accesses to `recent_staged_directories`, so we
    // never block on the mutex; we only need the Arc + Mutex to
    // guarantee a 'static lifetime for async.
    recent_staged_directories: Arc<Mutex<uluru::LRUCache<FileIdentifier, STAGED_MANIFEST_MEMORY>>>,
}

/// The `CopierSpoolState` represent what we know about the spool for
/// a given replicated sqlite db file.
#[derive(Debug, Default)]
struct CopierSpoolState {
    spool_path: Arc<PathBuf>,
    source: PathBuf,
    // Refcount for this spool state.
    count: AtomicUsize,

    // Set to true if we find that the replicated data is behind
    // the source sqlite file on disk.
    stale: AtomicBool,

    // Set to true by the `CopierBackend` thread when we have
    // successfully queued up this pool state for upload, cleared
    // by `CopierWorker`s immediately as they scan working on
    // the `SpoolState`.
    signaled: AtomicBool,

    // Last time we successfully copied a manifest blob.
    last_manifest_copy: RacySystemTime,

    // Last time we started a copy scan.
    last_scanned: RacySystemTime,

    // Incremented when we successfully handle the spool directory.
    consecutive_successes: AtomicU64,
    last_success: RacySystemTime,

    // Incremented when we fail to handle the spool directory.
    consecutive_failures: AtomicU64,
    last_failure: RacySystemTime,

    // Incremented when we upload a new manifest blob.
    consecutive_updates: AtomicU64,
    last_update: RacySystemTime,

    // Copier workers attempt to acquire this mutex before processing
    // the spool path's contents.
    upload_lock: Mutex<CopierUploadState>,
}

type DateTime = chrono::DateTime<chrono::Utc>;

fn is_epoch(date_time: &DateTime) -> bool {
    let epoch: DateTime = std::time::SystemTime::UNIX_EPOCH.into();
    date_time == &epoch
}

#[derive(Debug, serde::Serialize)]
struct CopierSpoolLagInfo {
    // ctime for the local sqlite db file.
    #[serde(skip_serializing_if = "is_epoch")]
    source_file_ctime: DateTime,

    // ctime in the last replicated manifest proto.
    replicated_file_ctime: DateTime,

    // true if the source and replicated headers match.  This
    // indicates that the replicated file is semantically equivalent
    // to the source file, although its bytes might differ.
    sqlite_headers_match: bool,

    // true if the `CopierSpoolState::upload_lock` is currently taken.
    // This indicates that the spool directory is currently being
    // copied.
    locked: bool,

    // time of the last copier scan
    #[serde(skip_serializing_if = "is_epoch")]
    last_scanned: DateTime,

    // Success / failure / update statistics.
    consecutive_successes: u64,
    #[serde(skip_serializing_if = "is_epoch")]
    last_success: DateTime,

    consecutive_failures: u64,
    #[serde(skip_serializing_if = "is_epoch")]
    last_failure: DateTime,

    consecutive_updates: u64,
    #[serde(skip_serializing_if = "is_epoch")]
    last_update: DateTime,
}

#[derive(Debug)]
struct CopierBackend {
    ready_buffers: Receiver<Arc<PathBuf>>,
    maintenance: Receiver<ActiveSetMaintenance>,
    // Channel for edge-triggered work units.
    edge_workers: Sender<Arc<CopierSpoolState>>,
    // Channel for work units triggered by replication lag.
    lag_workers: Sender<Arc<CopierSpoolState>>,
    // Channel for background maintenance work units.
    maintenance_workers: Sender<Arc<CopierSpoolState>>,
    periodic_lag_scan: Receiver<std::time::Instant>,
    // Map from PathBuf for spool directories to their state.
    // The key is always equal to the value's `path` field.
    active_spool_paths: HashMap<Arc<PathBuf>, Arc<CopierSpoolState>>,
}

impl Clone for Copier {
    fn clone(&self) -> Self {
        self.incref(None);

        Copier {
            ready_buffers: self.ready_buffers.clone(),
            maintenance: self.maintenance.clone(),
            spool_path: self.spool_path.clone(),
        }
    }
}

impl Drop for Copier {
    fn drop(&mut self) {
        self.delref();
    }
}

impl Copier {
    /// Returns a handle for the global `Copier` worker.
    #[instrument(level = "debug")]
    pub fn get_global_copier() -> Copier {
        lazy_static::lazy_static! {
            static ref GLOBAL_COPIER: Copier = Copier::new();
        }

        GLOBAL_COPIER.clone()
    }

    /// Returns a handle for a fresh Copier.
    #[instrument(level = "debug")]
    pub fn new() -> Copier {
        Copier::new_with_capacity(100)
    }

    /// Increments the refcount for the current spool path, if any.
    #[instrument(level = "debug")]
    fn incref(&self, source_file: Option<PathBuf>) {
        if let Some(path) = &self.spool_path {
            self.maintenance
                .send(ActiveSetMaintenance::Join(path.clone(), source_file))
                .expect("channel should not disconnect");
        }
    }

    /// Decrements the refcount for the current spool path, if any.
    #[instrument(level = "debug")]
    fn delref(&self) {
        if let Some(path) = &self.spool_path {
            self.maintenance
                .send(ActiveSetMaintenance::Leave(path.clone()))
                .expect("channel should not disconnect");
        }
    }

    /// Returns a copy of `self` with an updated `spool_path`.
    #[instrument(level = "debug")]
    pub fn with_spool_path(mut self, spool_path: Arc<PathBuf>, file_path: PathBuf) -> Copier {
        let noop = matches!(&self.spool_path, Some(old) if *old == spool_path);
        if !noop {
            self.delref();
            self.spool_path = Some(spool_path);
            self.incref(Some(file_path));
        }

        self
    }

    /// Returns a handle for a fresh Copier that allows for
    /// `channel_capacity` pending signalled ready buffer
    /// before dropping anything.
    #[instrument(level = "debug")]
    pub fn new_with_capacity(channel_capacity: usize) -> Copier {
        let (mut backend, buf_send, maintenance_send) =
            CopierBackend::new(WORKER_COUNT, channel_capacity);
        std::thread::Builder::new()
            .name("verneuil-copier-backend".to_string())
            .spawn(move || backend.handle_requests())
            .expect("failed to spawn copier backend thread");

        let ret = Copier {
            ready_buffers: buf_send,
            maintenance: maintenance_send,
            spool_path: None,
        };

        ret.incref(None);
        ret
    }

    /// Attempts to signal that the "ready" buffer subdirectory in
    /// `parent_directory` is available for copying.
    #[instrument(level = "debug")]
    pub fn signal_ready_buffer(&self) {
        // Eat the failure for now.  We may fail to replicate a write
        // transaction when the copier is falling behind; this delays
        // replication until the next write, but isn't incorrect.
        if let Some(parent_directory) = &self.spool_path {
            drop_result!(self.ready_buffers.try_send(parent_directory.clone()),
                         e => chain_info!(e, "failed to signal ready buffer",
                                          path=?parent_directory));
        }
    }
}

/// Synchronously copies any pending replication data in `path`.
#[instrument]
pub fn copy_spool_path(path: &Path) -> Result<()> {
    lazy_static::lazy_static! {
        static ref WORKER: CopierWorker = {
            let governor = Arc::new(governor::RateLimiter::direct_with_clock(
                SYNCHRONOUS_COPY_RATE_QUOTA,
                &Default::default(),
            ));

            let (_send, recv) = crossbeam_channel::bounded(1);
            CopierWorker {
                edge_work: recv.clone(),
                lag_work: recv.clone(),
                maintenance_work: recv,
                governor,
            }
        };
    }

    // Assume the replication target is behind: someone asked for a
    // synchronous copy.
    WORKER.handle_spooling_directory(
        &Default::default(),
        &mut Default::default(),
        /*stale=*/ true,
        path,
        /*sleep_on_credential_failure=*/ false,
    )?;
    Ok(())
}

/// Ensures the directory at `target` does not exist.
///
/// Returns Ok if this was achieved, and Err otherwise.
#[instrument(level = "debug")]
fn ensure_directory_removed(target: &Path) -> Result<()> {
    match std::fs::remove_dir(&target) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(chain_error!(e, "failed to remove directory", ?target)),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ConsumeDirectoryPolicy {
    /// If we remove nothing, we just iterate over the files...
    /// "scan" directory might be a more appropriate name.
    KeepAll,

    /// Remove files that have been successfully consumed.
    RemoveFiles,

    /// Also remove the directory, if it's now empty.
    RemoveFilesAndDirectory,
}

/// Lists the files in `to_consume`, and passes them to `consumer`.
/// When the `consumer` returns Ok(None) or Ok(future) where the
/// future eventually resolves to Ok(()) for a file, attempts to
/// remove it if the policy is `RemoveFiles` or
/// `RemoveFilesAndDirectory`.
///
/// Finally, ensures the `to_consume` directory is gone if the policy
/// is `RemoveFilesAndDirectory`; on success, this implies that every
/// file in it has been consumed.
///
/// Periodically checks any of the directories in `stop_if_exists`
/// exist and contain files.  If one does, stops consuming files.
#[instrument(level = "debug", skip(consumer), err)]
fn consume_directory<R: 'static + Future<Output = Result<()>>>(
    mut to_consume: PathBuf,
    mut consumer: impl FnMut(&OsStr, File) -> Result<Option<R>>,
    policy: ConsumeDirectoryPolicy,
    stop_if_exists: &[&Path],
) -> Result<()> {
    use ConsumeDirectoryPolicy::*;

    let delete_file = matches!(policy, RemoveFiles | RemoveFilesAndDirectory);
    let mut consume_files = |rt: &tokio::runtime::Runtime,
                             local_set: &tokio::task::LocalSet,
                             dirents: std::fs::ReadDir,
                             to_consume: &mut PathBuf| {
        use itertools::Itertools;

        let consume_limit = Arc::new(tokio::sync::Semaphore::new(
            CONSUME_DIRECTORY_CONCURRENCY_LIMIT,
        ));

        for names in &dirents
            .flatten()
            .map(|dirent| dirent.file_name())
            .chunks(CONSUME_DIRECTORY_BATCH_SIZE)
        {
            // Break out early if the `stop_if_exists` path contains files.
            if stop_if_exists
                .iter()
                .any(|path| !matches!(directory_is_empty_or_absent(path), Ok(true)))
            {
                break;
            }

            // Force eager evaluation for each chunk of files
            for name in names.collect::<Vec<_>>() {
                to_consume.push(&name);
                let file_or = File::open(&to_consume).map_err(|e| {
                    filtered_io_error!(e,
                                       ErrorKind::NotFound => Level::DEBUG,
                                       "failed to open file to copy", path=?to_consume)
                });
                if let Ok(contents) = file_or {
                    // Logs any failure, and deletes the file on
                    // success, if the caller asked for that.
                    let cleanup =
                        move |consume_result: Result<()>, name: &OsStr, to_consume: &Path| {
                            let result = consume_result.map_err(|e| {
                                chain_info!(e, "failed to consume file", ?name, ?to_consume)
                            });
                            if result.is_ok() && delete_file {
                                // Attempt to remove the file.  It's ok if
                                // this fails: either someone else removed
                                // the file, or `ensure_directory_removed`
                                // will fail, correctly signaling failure.
                                drop_result!(
                                std::fs::remove_file(&to_consume),
                                e => filtered_io_error!(
                                    e, ErrorKind::NotFound => Level::DEBUG,
                                    "failed to remove consumed file", path=?to_consume));
                            }
                        };

                    match consumer(&name, contents) {
                        Ok(None) => cleanup(Ok(()), &name, to_consume),
                        Ok(Some(continuation)) => {
                            let name = name.to_owned();
                            let to_consume = to_consume.clone();
                            let consume_limit = consume_limit.clone();
                            // Wait until a token is available.
                            // Convert errors to None: we never close
                            // the Semaphore explicitly, and we'd
                            // rather make progress too fast than
                            // stop or crash.
                            let token = local_set
                                .block_on(rt, async {
                                    // Let pending tasks make a bit of progress.
                                    tokio::task::yield_now().await;
                                    consume_limit.acquire_owned().await.map_err(|e| {
                                        chain_error!(e, "failed to acquire copy semaphore")
                                    })
                                })
                                .ok();

                            // Spawn a new task that now owns the token.
                            local_set.spawn_local(async move {
                                cleanup(continuation.await, &name, &to_consume);
                                std::mem::drop(token);
                            });
                        }
                        Err(e) => cleanup(Err(e), &name, to_consume),
                    };
                }

                to_consume.pop();
            }
        }
    };

    match std::fs::read_dir(&to_consume) {
        Ok(dirents) => {
            call_with_executor(|runtime| {
                let local_set = tokio::task::LocalSet::new();
                consume_files(runtime, &local_set, dirents, &mut to_consume);
                // Wait for any pending task before returning.
                call_with_slow_logging(
                    Duration::from_secs(30),
                    || runtime.block_on(local_set),
                    |duration| tracing::info!(?duration, ?to_consume, "slow consume_files join"),
                );
            });
            if policy == RemoveFilesAndDirectory {
                // If we can't get rid of that directory, it must be
                // non-empty, which means we failed to consume some
                // file... in which case we must report failure.
                ensure_directory_removed(&to_consume)
            } else {
                Ok(())
            }
        }

        // It's OK if the directory is already gone (and thus empty).
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(chain_error!(err, "failed to list directory", path=?to_consume)),
    }
}

/// Creates `bucket` if it does not already exists.
#[instrument(level = "debug", skip(bucket), err)]
fn ensure_bucket_exists(bucket: &Bucket) -> Result<()> {
    let bucket_location = bucket.location_blocking().map_err(|e| {
        chain_debug!(
            e,
            "failed to get buccket location",
            name=%bucket.name(),
            region=?bucket.region()
        )
    });
    if matches!(bucket_location, Ok((_, 200))) {
        return Ok(());
    }

    let result = if bucket.is_subdomain_style() {
        Bucket::create_blocking
    } else {
        Bucket::create_with_path_style_blocking
    }(
        &bucket.name(),
        bucket.region(),
        bucket.credentials().clone(),
        s3::bucket_ops::BucketConfiguration::private(),
    );

    match result {
        Ok(response)
            if (response.response_code >= 200 && response.response_code < 300) ||
            // Conflicts on create is usually because the bucket already exists.
            response.response_code == 409 =>
        {
            Ok(())
        }
        Ok(response) => Err(fresh_warn!("failed to create bucket in S3",
                                        response=?(response.response_code, response.response_text),
                                        name=?bucket.name(), region=?bucket.region())),
        Err(e) => Err(chain_warn!(e, "failed to create bucket in S3",
                                  name=?bucket.name(), region=?bucket.region())),
    }
}

/// Attempts to configure a `Bucket` from a `ReplicationTarget`.  Once
/// configured, the `Copier` will use the same bucket object to
/// publish objects.
#[instrument(level = "debug", skip(bucket_extractor, creds), err)]
fn create_target(
    target: &ReplicationTarget,
    bucket_extractor: impl FnOnce(&S3ReplicationTarget) -> &str,
    creds: Credentials,
) -> Result<Option<Bucket>> {
    use ReplicationTarget::*;

    match target {
        S3(s3) => {
            let region = parse_s3_region_specification(&s3.region, s3.endpoint.as_deref());
            let bucket_name = bucket_extractor(s3);
            let mut bucket = Bucket::new(bucket_name, region, creds)
                .map_err(|e| chain_error!(e, "failed to create S3 bucket object", ?s3))?;

            if s3.domain_addressing {
                bucket.set_subdomain_style();
            } else {
                bucket.set_path_style();
            }

            if s3.create_buckets_on_demand {
                ensure_bucket_exists(&bucket)?;
            }

            bucket.set_request_timeout(Some(COPY_REQUEST_TIMEOUT));
            Ok(Some(bucket))
        }
        ReadOnly(_) | Local(_) => Ok(None),
    }
}

/// Invokes `timed` and checks how long that call took.  If the time
/// exceeds the time limit, invokes `slow_logger` before returning as
/// usual.
fn call_with_slow_logging<T>(
    limit: Duration,
    timed: impl FnOnce() -> T,
    slow_logger: impl FnOnce(Duration),
) -> T {
    let start = std::time::Instant::now();
    let ret = timed();

    let elapsed = start.elapsed();
    if elapsed >= limit {
        slow_logger(elapsed);
    }

    ret
}

async fn await_with_slow_logging<T, R: Future<Output = T>>(
    limit: Duration,
    timed: impl FnOnce() -> R,
    slow_logger: impl FnOnce(Duration),
) -> T {
    let start = std::time::Instant::now();
    let ret = timed().await;

    let elapsed = start.elapsed();
    if elapsed >= limit {
        slow_logger(elapsed);
    }

    ret
}

/// Attempts to publish the `contents` to `name` in all `targets`.
///
/// If `level >= 0`, compresses the contents with zstd, at that level.
#[allow(clippy::suspicious_else_formatting)] // clippy complains about the macro
#[instrument(level = "debug", skip(targets), err)]
async fn copy_file(
    name: &OsStr,
    contents: &mut File,
    level: i32,
    targets: &[Bucket],
) -> Result<()> {
    use rand::Rng;
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;

    let mut rng = rand::thread_rng();

    let blob_name = name
        .to_str()
        .ok_or_else(|| fresh_error!("invalid name", ?name))?;

    let mut bytes = Vec::new();
    // TODO: check that chunk fingerprints match, check that directories checksum?
    contents
        .seek(SeekFrom::Start(0))
        .map_err(|e| chain_error!(e, "failed to seek file"))?;
    contents
        .read_to_end(&mut bytes)
        .map_err(|e| chain_error!(e, "failed to read file contents", ?blob_name))?;

    if level >= 0 {
        match zstd::encode_all(bytes.as_slice(), level) {
            Ok(encoded) => bytes = encoded,
            Err(e) => tracing::warn!(?e, "failed to zstd-compress data"),
        }
    }

    for target in targets {
        for i in 0..=COPY_RETRY_LIMIT {
            match await_with_slow_logging(
                Duration::from_secs(10),
                || target.put_object_with_content_type(&blob_name, &bytes, CHUNK_CONTENT_TYPE),
                |duration| tracing::info!(?duration, ?blob_name, len = bytes.len(), "slow S3 PUT"),
            )
            .await
            {
                Ok((_, code)) if (200..300).contains(&code) => {
                    break;
                }
                Ok((body, code)) if code < 500 && code != 408 && code != 429 => {
                    // If something went wrong, clear the recent
                    // wrong: maybe the remote is in a bad state.
                    RECENT_WORK.lock().unwrap().clear();

                    // Permanent failure.  In theory, we should maybe
                    // retry on 400: RequestTimeout, but we'll catch
                    // it in the next background scan.
                    return Err(chain_error!(
                        (body, code),
                        "failed to post chunk",
                        %target.name,
                        ?blob_name,
                        len = bytes.len()
                    ));
                }
                err => {
                    if i == COPY_RETRY_LIMIT {
                        return Err(chain_warn!(
                            err,
                            "reached retry limit",
                            %target.name,
                            ?blob_name,
                            COPY_RETRY_LIMIT,
                            len = bytes.len()
                        ));
                    }

                    let sleep = COPY_RETRY_BASE_WAIT.mul_f64(COPY_RETRY_MULTIPLIER.powi(i));
                    let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);
                    let backoff = sleep.mul_f64(jitter_scale);

                    tracing::info!(
                        ?err,
                        ?backoff,
                        %target.name,
                        ?blob_name,
                        len = bytes.len(),
                        "backing off after a failed PUT"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    Ok(())
}

/// Attempts to touch the blob `name` in all `targets`, by copying the
/// blob to itself.
///
/// The buckets in `targets` must only be used for `touch_blob`.
///
/// This function only returns `Err` if we successfully contacted the
/// remote blob store and something is actively wrong with `blob_name`
/// (e.g., it doesn't exist).
#[instrument(level = "debug", skip(targets), err)]
fn touch_blob(blob_name: &str, targets: &mut [Bucket]) -> Result<()> {
    use rand::Rng;

    const COPY_SOURCE: &str = "x-amz-copy-source";
    const METADATA_DIRECTIVE: &str = "x-amz-metadata-directive";
    const METADATA_DIRECTIVE_VALUE: &str = "REPLACE";

    let mut rng = rand::thread_rng();

    for target in targets {
        let location_name = format!("{}/{}", target.name, blob_name);
        // The value must be URL encoded (yes, that is double encoding
        // given that blob names are themselves percent encoded).
        let url_encoded_name = percent_encoding::utf8_percent_encode(
            &location_name,
            percent_encoding::NON_ALPHANUMERIC,
        );

        target.add_header(COPY_SOURCE, &url_encoded_name.to_string());
        // We're about to copy an object to itself.  S3 only allows this
        // if we replace all metadata.
        target.add_header(METADATA_DIRECTIVE, METADATA_DIRECTIVE_VALUE);

        for i in 0..=COPY_RETRY_LIMIT {
            match call_with_slow_logging(
                Duration::from_secs(10),
                || {
                    target.put_object_with_content_type_blocking(
                        &blob_name,
                        &[],
                        CHUNK_CONTENT_TYPE,
                    )
                },
                |duration| tracing::info!(?duration, ?blob_name, "slow S3 COPY"),
            ) {
                Ok((_, code)) if (200..300).contains(&code) => {
                    break;
                }
                Ok((body, 404)) => {
                    // If something went wrong, clear the recent
                    // wrong: maybe the remote is in a bad state.
                    RECENT_WORK.lock().unwrap().clear();

                    // Something's definitely wrong with our
                    // replication data if we can't find the blob.
                    return Err(chain_error!(
                        (body, 404),
                        "chunk not found",
                        %target.name,
                        ?blob_name
                    ));
                }
                // If it's a non-404 error, don't retry.
                Ok((body, code)) if (400..500).contains(&code) && code != 408 && code != 429 => {
                    RECENT_WORK.lock().unwrap().clear();

                    let _ = chain_warn!((body, code), "failed to touch chunk", %target.name, ?blob_name);
                    break;
                }
                err => {
                    if i == COPY_RETRY_LIMIT {
                        let _ = chain_warn!(
                            err,
                            "reached retry limit",
                            %target.name,
                            ?blob_name,
                            COPY_RETRY_LIMIT
                        );

                        // Don't error out if the remote is unreachable.
                        break;
                    }

                    let sleep = COPY_RETRY_BASE_WAIT.mul_f64(COPY_RETRY_MULTIPLIER.powi(i));
                    let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);
                    let backoff = sleep.mul_f64(jitter_scale);

                    tracing::info!(
                        ?err,
                        ?backoff,
                        %target.name,
                        ?blob_name,
                        "backing off after a failed PUT"
                    );
                    std::thread::sleep(backoff);
                }
            }
        }
    }

    Ok(())
}

/// Attempts to publish the contents of `data` to the `cache`, for
/// chunk id `chunk`.
///
/// Logs and drops any error: readers can always get the data they
/// need from remote storage.  On the other hand, populating a local
/// cache doesn't guarantee visibility (or durability).
#[tracing::instrument(level = "debug")]
fn publish_chunk_to_cache(cache: &Cache, chunk: &OsStr, data: &mut File) {
    use kismet_cache::CacheHit::*;
    use kismet_cache::CacheHitAction;

    let chunk = match chunk.to_str() {
        Some(chunk) => chunk,
        None => {
            tracing::info!(?chunk, "non-utf-8 chunk filename");
            return;
        }
    };

    let fprint = match replication_buffer::chunk_name_fingerprint(chunk) {
        Some(fprint) => fprint,
        None => {
            tracing::info!(?chunk, "malformed chunk filename");
            return;
        }
    };

    match cache.get_or_update(
        kismet_cache::Key::new(chunk, fprint.hash(), fprint.secondary()),
        |hit| match hit {
            Primary(_) => CacheHitAction::Accept,
            Secondary(_) => CacheHitAction::Replace,
        },
        |dst, _| {
            use std::io::Seek;
            use std::io::SeekFrom;

            data.seek(SeekFrom::Start(0))?;
            std::io::copy(data, dst)?;
            Ok(())
        },
    ) {
        Ok(_) => {}
        Err(e) => {
            let _ = chain_warn!(e, "failed to publish chunk to cache", ?chunk);
        }
    };
}

/// Returns whether the directory at `path` is empty or just does
/// not exist at all.
#[instrument(level = "debug")]
fn directory_is_empty_or_absent(path: &Path) -> Result<bool> {
    match std::fs::read_dir(path) {
        Ok(mut dirents) => Ok(dirents.next().is_none()),
        // It's OK if the directory is already gone (and thus empty).
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(true),
        Err(err) => Err(chain_error!(
            err,
            "failed to list directory contents",
            ?path
        )),
    }
}

impl FileIdentifier {
    /// Returns a tuple that identifies a given file; if a given path has
    /// the same identifier, it is the same (unless someone is maliciously
    /// tampering with it).
    #[instrument(level = "debug")]
    fn new(file: &File) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let meta = file
            .metadata()
            .map_err(|e| chain_error!(e, "failed to stat file"))?;
        Ok(FileIdentifier {
            btime: meta
                .created()
                .map_err(|e| chain_debug!(e, "failed to compute file creation time", ?meta))
                .ok(),
            len: meta.len(),
            dev: meta.dev(),
            ino: meta.ino(),
            ctime: meta.ctime(),
            ctime_nsec: meta.ctime_nsec(),
        })
    }

    /// Returns whether `self == other`, except for `ctime`/`ctime_nsec`.
    fn equal_except_ctime(&self, other: &FileIdentifier) -> bool {
        (self.btime, self.len, self.dev, self.ino) == (other.btime, other.len, other.dev, other.ino)
    }
}

#[derive(Debug)]
struct CopierWorker {
    // Edge-triggered work units, enqueued at the end of a sqlite
    // transaction.
    edge_work: Receiver<Arc<CopierSpoolState>>,
    // Work units in reaction to noticing replication lag.
    lag_work: Receiver<Arc<CopierSpoolState>>,
    // Maintenance work units, enqueued periodically for background
    // scans.  Some workers should not process maintenance work, and
    // are instead dedicated to edge and lag triggered work.  For such
    // workers, `maintenance_work` is `crossbeam_channel::never()`.
    maintenance_work: Receiver<Arc<CopierSpoolState>>,
    governor: Arc<Governor>,
}

/// Attempts to force a full snapshot the next time a change `Tracker`
/// synchronises the db file.
#[instrument]
fn force_full_snapshot(spool_path: &Path, source: &Path) {
    fn reset_db_file_id(path: &Path) -> Result<()> {
        use std::fs::OpenOptions;

        let file = match OpenOptions::new()
            .create(false)
            .write(true)
            .truncate(false)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(chain_error!(e, "failed to open sqlite db file")),
        };

        clear_version_id(&file)
    }

    // Mess with the sqlite file's version id if we can.
    drop_result!(reset_db_file_id(source),
                 e => chain_error!(e, "failed to clear version id", ?source));

    // And now delete all (manifest) files in staging/meta.
    let staging = replication_buffer::mutable_staging_directory(spool_path.to_owned());
    let meta_directory = replication_buffer::directory_meta(staging);
    drop_result!(consume_directory(meta_directory,
                                   |_, _| -> Result<Option<future::Ready<Result<()>>>> { Ok(None) },
                                   ConsumeDirectoryPolicy::RemoveFiles, &[]),
                 e => chain_error!(e, "failed to delete staged meta files", ?spool_path));
}

/// Waits for the meta copy lock on the spooling directory at
/// `parent`.  This function may return Ok() without actually owning
/// the lock: the copy lock is only used to avoid letting slow uploads
/// overwrite more recent data, but isn't necessary for correctness.
/// In the worst case, we will eventually overwrite with fresh data,
/// as long as copiers can make progress.
///
/// Returns Err on failure, Ok on success or if we waited long enough.
#[instrument(level = "debug", err)]
fn wait_for_meta_copy_lock(parent: &Path) -> Result<Option<OfdLock>> {
    use rand::Rng;

    if let Some(file) = replication_buffer::acquire_meta_copy_lock(parent.to_path_buf())? {
        return Ok(Some(file));
    };

    let mut rng = rand::thread_rng();
    let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);
    let backoff = COPY_LOCK_CONTENTION_WAIT.mul_f64(jitter_scale);

    tracing::info!(
        ?backoff,
        ?parent,
        "failed to acquire meta copy lock. sleeping."
    );
    std::thread::sleep(backoff);

    let ret = replication_buffer::acquire_meta_copy_lock(parent.to_path_buf());
    if matches!(&ret, Ok(Some(_))) {
        tracing::debug!(
            ?backoff,
            ?parent,
            "successfully acquired meta copy lock after sleeping"
        );
    } else {
        tracing::info!(
            ?backoff,
            ?parent,
            ?ret,
            "failed to acquire meta copy lock after sleeping"
        );

        if rng.gen_bool(COPY_LOCK_RESET_RATE) {
            tracing::info!(?parent, "resetting stuck meta copy lock file");
            drop_result!(replication_buffer::reset_meta_copy_lock(parent.to_path_buf()),
                         e => chain_error!(e, "failed to reset meta copy lock after acquisition failure",
                                           ?backoff, ?parent));
        }
    }

    ret
}

impl CopierWorker {
    /// Sleeps until the governor lets us fire the next set of API calls.
    #[instrument(level = "debug", skip(self))]
    fn pace(&self) {
        use rand::Rng;
        use std::time::Instant;

        let deadline_or = match self.governor.check() {
            Ok(_) => None,
            Err(delay) => {
                let mut rng = rand::thread_rng();
                let wait_time = delay.wait_time_from(Instant::now());
                let jitter_scale = rng.gen_range(0.0..RATE_LIMIT_SLEEP_JITTER_FRAC);
                let extra = wait_time.mul_f64(jitter_scale);
                let deadline = delay.earliest_possible();
                Some(deadline.checked_add(extra).unwrap_or(deadline))
            }
        };

        // `tokio::runtime::Handle`'s executor is broken with current
        // thread runtimes.  Always use `call_with_executor` to get
        // the thread-local current thread executor.
        call_with_executor(|rt| {
            rt.block_on(async {
                if let Some(deadline) = deadline_or {
                    tokio::time::sleep_until(deadline.into()).await;
                } else {
                    tokio::task::yield_now().await;
                }
            })
        });
    }

    /// Sleeps until `MIN_MANIFEST_COPY_DELAY` has elapsed since `last_manifest_copy`.
    #[instrument(level = "debug", skip(self))]
    fn delay_manifest_copy(&self, last_manifest_copy: &RacySystemTime) {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let elapsed = last_manifest_copy
            .load()
            .elapsed()
            .unwrap_or(Duration::ZERO);
        let delay = match MIN_MANIFEST_COPY_DELAY.checked_sub(elapsed) {
            None => return,
            Some(delay) => delay,
        };
        let jitter_scale = rng.gen_range(1.0..1.5);
        let sleep = delay.mul_f64(jitter_scale);

        call_with_executor(|rt| {
            rt.block_on(async {
                tokio::time::sleep(sleep).await;
            })
        })
    }

    /// Handles one "ready" directory: rename it to "consuming", copy
    /// the contents, and delete the corresponding files and
    /// directories as we go.
    ///
    /// Renaming the "ready" directory makes it possible for change
    /// `Tracker`s to publish a new "ready" directory immediately.
    ///
    /// Once *everything* has been copied, the "consuming" directory
    /// will be empty, which will make it possible to rename a fresher
    /// "ready" snapshot over it.
    ///
    /// Returns whether we successfully updated the remote snapshot.
    #[instrument(skip(self, creds), err)]
    fn handle_ready_directory(
        &self,
        last_manifest_copy: &RacySystemTime,
        cache: &Cache,
        targets: &ReplicationTargetList,
        creds: Credentials,
        parent: PathBuf,
    ) -> Result<bool> {
        let consuming = match replication_buffer::snapshot_ready_directory(parent.clone())? {
            Some(ret) => ret,
            None => return Ok(false),
        };

        {
            let chunks_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.chunk_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .flatten() // remove None
                .collect::<Vec<_>>();

            // If we don't have replication target, best to leave the data
            // where it is.
            if chunks_buckets.is_empty() {
                return Ok(false);
            }

            let chunks_buckets = Arc::new(chunks_buckets);
            consume_directory(
                replication_buffer::directory_chunks(consuming.clone()),
                |name, mut file| {
                    let work_unit = WorkUnit::new((targets, name));

                    // Maybe we recently handled this work unit, and
                    // there's nothing to do.
                    if RECENT_WORK
                        .lock()
                        .unwrap()
                        .has_recent(&work_unit, COPY_REQUEST_MIN_AGE)
                        .is_some()
                    {
                        return Ok(None);
                    }

                    self.pace();
                    publish_chunk_to_cache(cache, name, &mut file);

                    let name = name.to_owned();
                    let chunks_buckets = chunks_buckets.clone();

                    Ok(Some(async move {
                        copy_file(&name, &mut file, ZSTD_COMPRESSION_LEVEL, &chunks_buckets)
                            .await?;
                        RECENT_WORK.lock().unwrap().observe(&work_unit);
                        Ok(())
                    }))
                },
                ConsumeDirectoryPolicy::RemoveFilesAndDirectory,
                &[],
            )?;
        }

        let did_something = Arc::new(AtomicBool::new(false));

        {
            let meta_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.manifest_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .flatten() // Drop `None`
                .collect::<Vec<_>>();

            if meta_buckets.is_empty() {
                return Ok(false);
            }

            self.delay_manifest_copy(last_manifest_copy);
            let meta_buckets = Arc::new(meta_buckets);
            let _lock = wait_for_meta_copy_lock(&parent)?;
            consume_directory(
                replication_buffer::directory_meta(consuming.clone()),
                |name, mut file| {
                    self.pace();
                    let name = name.to_owned();
                    let did_something = did_something.clone();
                    let meta_buckets = meta_buckets.clone();
                    let parent = parent.to_owned();

                    Ok(Some(async move {
                        copy_file(&name, &mut file, ZSTD_COMPRESSION_DISABLE, &meta_buckets)
                            .await?;
                        replication_buffer::tap_manifest_file(&parent, &name, &mut file).map_err(
                            |e| {
                                chain_warn!(
                                    e,
                                    "failed to tap replicated manifest file",
                                    ?name,
                                    ?parent
                                )
                            },
                        )?;
                        did_something.store(true, Ordering::Relaxed);
                        Ok(())
                    }))
                },
                ConsumeDirectoryPolicy::RemoveFilesAndDirectory,
                &[],
            )?;

            last_manifest_copy.store_now();
        }

        // The pseudo unique directory should be empty now.  If it
        // isn't, the `remove_dir` call will fail.
        drop_result!(std::fs::remove_dir(&consuming),
                     // It's ok if someone else already beat us to the
                     // cleanup.
                     e if e.kind() == ErrorKind::NotFound => {},
                     e => chain_info!(e, "failed to clean up pseudo-unique consuming directory"));

        // And now try to get rid of the hopefully consuming directory.
        drop_result!(replication_buffer::remove_consuming_directory_if_empty(parent),
                     e => chain_info!(e, "failed to clean up consuming directory"));
        Ok(did_something.load(Ordering::Relaxed))
    }

    /// Handles one "staging" directory: copy the chunks, then copy
    /// the metadata blobs if nothing has changed since.
    ///
    /// This function can only make progress if the caller first
    /// calls `handle_ready_directory`.
    #[allow(clippy::too_many_arguments)] // It's only called in one place.
    #[instrument(skip(self, creds), err)]
    fn handle_staging_directory(
        &self,
        last_manifest_copy: &RacySystemTime,
        state: &CopierUploadState,
        stale: bool,
        cache: &Cache,
        targets: &ReplicationTargetList,
        creds: Credentials,
        parent: PathBuf,
    ) -> Result<bool> {
        use rand::Rng;

        const FORCE_META_PROBABILITY: f64 = 0.05;

        let consuming_directory = replication_buffer::mutable_consuming_directory(parent.clone());
        let ready_directory = replication_buffer::mutable_ready_directory(parent.clone());
        let staging = replication_buffer::mutable_staging_directory(parent.clone());
        let chunks_directory = replication_buffer::directory_chunks(staging.clone());
        let meta_directory = replication_buffer::directory_meta(staging);

        // If true, we always try to upload the contents of the meta
        // directory, even if we think that might be useless.
        let force_meta = Arc::new(AtomicBool::new(
            stale || rand::thread_rng().gen_bool(FORCE_META_PROBABILITY),
        ));

        // Check for an early exit condition here, before potentially
        // sleeping in `self.delay_manifest_copy`: `Tracker`s had time
        // to publish a new `ready` directory while we were consuming
        // the old one in `handle_ready_directory`.
        if !directory_is_empty_or_absent(&ready_directory)? {
            tracing::debug!(?ready_directory, "ready directory exists");
            return Ok(false);
        }

        // The `ready` directory might have been empty because it was
        // moved to `consuming`.  Check that it didn't happen.
        //
        // This state is worth logging about at info level: it could
        // also mean that we're failing to copy the contents of the
        // `consuming` directory (in which case we leave files behind).
        if !directory_is_empty_or_absent(&consuming_directory)? {
            tracing::info!(?consuming_directory, "consuming directory exists");
            return Ok(false);
        }

        // It's always safe to publish chunks: they don't have any
        // dependency.
        {
            let chunks_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.chunk_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .flatten() // Drop `None`.
                .collect::<Vec<_>>();

            // If we don't have any replication target, best to leave
            // the data where it is.
            if chunks_buckets.is_empty() {
                return Ok(false);
            }

            // Delay here instead of just before the manifest copy: we
            // might as well let change `Tracker`s make progress and
            // publish a new "ready" directory while we wait.
            self.delay_manifest_copy(last_manifest_copy);

            let chunks_buckets = Arc::new(chunks_buckets);
            consume_directory(
                chunks_directory.clone(),
                |name: &OsStr, mut file| {
                    let work_unit = WorkUnit::new((targets, name));

                    // Maybe we recently handled this work unit, and
                    // there's nothing to do.
                    if RECENT_WORK
                        .lock()
                        .unwrap()
                        .has_recent(&work_unit, COPY_REQUEST_MIN_AGE)
                        .is_some()
                    {
                        return Ok(None);
                    }

                    self.pace();
                    publish_chunk_to_cache(cache, name, &mut file);

                    let name = name.to_owned();
                    let chunks_buckets = chunks_buckets.clone();
                    let force_meta = force_meta.clone();
                    Ok(Some(async move {
                        copy_file(&name, &mut file, ZSTD_COMPRESSION_LEVEL, &chunks_buckets)
                            .await?;
                        // Always upload the contents of the meta
                        // directory if we found chunks to upload.
                        force_meta.store(true, Ordering::Relaxed);
                        RECENT_WORK.lock().unwrap().observe(&work_unit);
                        Ok(())
                    }))
                },
                ConsumeDirectoryPolicy::RemoveFiles,
                &[&ready_directory, &consuming_directory],
            )?;
        }

        // Snapshot the current meta files.  We hang on to the `File`s
        // to prevent inode reuse.
        let mut initial_meta: HashMap<std::ffi::OsString, (FileIdentifier, File)> = HashMap::new();
        consume_directory(
            meta_directory.clone(),
            &mut |name: &OsStr, file| -> Result<Option<future::Ready<Result<()>>>> {
                initial_meta.insert(name.to_owned(), (FileIdentifier::new(&file)?, file));
                Ok(None)
            },
            ConsumeDirectoryPolicy::KeepAll,
            &[],
        )?;

        // We must now make sure that we have published all the chunks
        // before publishing the meta files.
        if !directory_is_empty_or_absent(&chunks_directory)? {
            tracing::debug!(?chunks_directory, "unpublished staged chunks remain");
            return Ok(false);
        }

        // `chunks` might have been emptied because its contents were
        // moved to `ready` (and then maybe to `consuming`).  Make
        // sure such a move didn't happen *before* our check for a
        // non-empty chunks directory.

        // We must first check for a `ready` directory: if it doesn't
        // exist, it might now be `consuming`.
        if !directory_is_empty_or_absent(&ready_directory)? {
            return Ok(false);
        }

        // The `ready` directory might have been empty because it was
        // moved to `consuming`.  Check that it didn't happen.
        if !directory_is_empty_or_absent(&consuming_directory)? {
            return Ok(false);
        }

        // The `consuming` directory might be empty because it was
        // all fully uploaded.  That's fine: either way, there's no
        // missing chunk in remote storage.
        //
        // A new `ready` directory might exist now, but it must have
        // been created after our check that the `chunks` directory
        // was empty.  It's safe to assume that all the chunks needed
        // by the manifests we read earlier have been copied.
        let did_something = Arc::new(AtomicBool::new(false));

        {
            let meta_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.manifest_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .flatten() // Drop `None`
                .collect::<Vec<_>>();

            if meta_buckets.is_empty() {
                return Ok(false);
            }

            // We don't need to `delay_manifest_copy` here: we already
            // slept for more than long enough before copying chunks.
            let meta_buckets = Arc::new(meta_buckets);
            let _lock = wait_for_meta_copy_lock(&parent)?;
            consume_directory(
                meta_directory,
                |name, mut file| {
                    let identifier = FileIdentifier::new(&file)?;

                    // This is a new manifest file, we don't want to upload it.
                    if initial_meta.get(name).map(|x| &x.0) != Some(&identifier) {
                        return Ok(None);
                    }

                    // If we're not forcing uploads and we think we
                    // have already uploaded this file, nothing to do.
                    // We must ignore ctime because tapping a file
                    // changes its hardlink count, and thus updates
                    // its ctime.
                    if !force_meta.load(Ordering::Relaxed)
                        && state
                            .recent_staged_directories
                            .lock()
                            .unwrap()
                            .find(|x| x.equal_except_ctime(&identifier))
                            .is_some()
                    {
                        return Ok(None);
                    }

                    self.pace();
                    let name = name.to_owned();
                    let did_something = did_something.clone();
                    let meta_buckets = meta_buckets.clone();
                    let parent = parent.to_owned();
                    let recent_staged_directories = state.recent_staged_directories.clone();

                    Ok(Some(async move {
                        copy_file(&name, &mut file, ZSTD_COMPRESSION_DISABLE, &meta_buckets)
                            .await?;
                        replication_buffer::tap_manifest_file(&parent, &name, &mut file).map_err(
                            |e| {
                                chain_warn!(
                                    e,
                                    "failed to tap replicated manifest file",
                                    ?name,
                                    ?parent
                                )
                            },
                        )?;

                        recent_staged_directories.lock().unwrap().insert(identifier);
                        did_something.store(true, Ordering::Relaxed);
                        Ok(())
                    }))
                },
                ConsumeDirectoryPolicy::KeepAll,
                &[],
            )?;

            last_manifest_copy.store_now();
        }

        Ok(did_something.load(Ordering::Relaxed))
    }

    /// Processes one spooling directory that should be ready for
    /// replication.
    ///
    /// Returns `Err` on failure, `Ok(true)` if we updated snapshots,
    /// and `Ok(false)` if we successfully did not make progress.
    #[instrument(skip(self), err)]
    fn handle_spooling_directory(
        &self,
        last_manifest_copy: &RacySystemTime,
        state: &mut CopierUploadState,
        stale: bool,
        spool: &Path,
        sleep_on_credential_failure: bool,
    ) -> Result<bool> {
        let mut did_something = false;

        let creds = Credentials::default().map_err(|e| {
            use rand::Rng;

            let backoff = FAILED_CREDENTIALS_SLEEP.mul_f64(rand::thread_rng().gen_range(1.0..2.0));
            // Log before sleeping.
            let err = chain_error!(e, "failed to get S3 credentials", ?backoff);
            if sleep_on_credential_failure {
                std::thread::sleep(backoff);
            }

            err
        })?;

        // Try to read the metadata JSON, which tells us where to
        // replicate the chunks and meta files.  If we can't do
        // that, leave this precious data where it is...  We don't
        // provide any hard liveness guarantee on replication, so
        // that's not incorrect.  Even when replication is stuck,
        // the buffering system bounds the amount of replication
        // data we keep around.
        let targets: ReplicationTargetList = {
            let metadata = replication_buffer::buffer_metadata_file(spool.to_path_buf());
            let contents = std::fs::read(&*metadata)
                .map_err(|e| chain_error!(e, "failed to read .metadata file", ?metadata))?;

            serde_json::from_slice(&contents).map_err(|e| {
                chain_error!(e, "failed to parse .metadata file", ?metadata, ?contents)
            })?
        };

        let cache = apply_local_cache_replication_target(
            kismet_cache::CacheBuilder::new(),
            &targets.replication_targets,
        )
        .auto_sync(false)
        .take()
        .build();

        // We want to treat these directories as missing if they're
        // empty.  Our logic should handle errors that way, but we can
        // get more useful logs if we clean up here.
        for cleanup in [
            replication_buffer::mutable_consuming_directory(spool.to_path_buf()),
            replication_buffer::mutable_ready_directory(spool.to_path_buf()),
        ]
        .iter()
        {
            let is_empty = directory_is_empty_or_absent(cleanup)
                .map_err(|e| chain_debug!(e, "failed to list directory", ?cleanup));
            if matches!(is_empty, Ok(true)) {
                // It's not an error if this fails: we expect failures
                // when the directory becomes non-empty, and we never
                // lose data.
                drop_result!(std::fs::remove_dir(&cleanup),
                             e => filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG,
                                                     "failed to remove directory", ?cleanup));
            }
        }

        did_something |= call_with_slow_logging(
            Duration::from_secs(60),
            || {
                self.handle_ready_directory(
                    last_manifest_copy,
                    &cache,
                    &targets,
                    creds.clone(),
                    spool.to_path_buf(),
                )
                .map_err(|e| chain_warn!(e, "failed to handle ready directory", ?spool))
            },
            |duration| tracing::info!(?duration, ?spool, "slow handle_ready_directory"),
        )?;

        // Opportunistically try to copy from the "staging"
        // directory.  That's never staler than "ready", so we do
        // not go backward in our replication.
        match call_with_slow_logging(
            Duration::from_secs(60),
            || {
                self.handle_staging_directory(
                    last_manifest_copy,
                    state,
                    stale,
                    &cache,
                    &targets,
                    creds.clone(),
                    spool.to_path_buf(),
                )
            },
            |duration| tracing::info!(?duration, ?spool, "slow handle_staging_directory"),
        ) {
            Ok(ret) => did_something |= ret,
            Err(e) => {
                if !did_something {
                    return Err(chain_warn!(e, "failed to handle staging directory", ?spool));
                }
            }
        }

        // And now see if the ready directory was updated again.
        // We only upload meta files (manifest protos) if we
        // observed that the "ready" directory was empty while the
        // meta files had the same value as when we entered
        // "handle_staging_directory".  Anything we now find in
        // the "ready" directory must be at least as recent as
        // what we found in staging, so, again, replication
        // cannot go backwards.
        match call_with_slow_logging(
            Duration::from_secs(60),
            || {
                self.handle_ready_directory(
                    last_manifest_copy,
                    &cache,
                    &targets,
                    creds,
                    spool.to_path_buf(),
                )
            },
            |duration| tracing::info!(?duration, ?spool, "slow re-handle_ready_directory"),
        ) {
            Ok(ret) => did_something |= ret,
            Err(e) => {
                if !did_something {
                    return Err(chain_info!(e, "failed to rehandle ready directory", ?spool));
                }
            }
        }

        // When we get here, the remote data should be at least as
        // fresh as the last staged snapshot when we entered the
        // loop body.
        Ok(did_something)
    }

    /// Attempts to touch a small pseudrandom subset of the chunks
    /// referred by the latest uploaded manifest.
    ///
    /// Only errors out if we successfully connected to remote storage
    /// and failed to update one of the "touched" chunks.
    #[instrument(skip(self), err)]
    fn patrol_touch_chunks(
        &self,
        spool_path: &Path,
        source: &Path,
        time_since_last_patrol: Duration,
    ) -> Result<()> {
        use rand::seq::SliceRandom;
        use rand::Rng;

        // Stop touching the dependencies once the source file has
        // been deleted.
        if !source.exists() {
            tracing::debug!(?spool_path, ?source, "source db file not found");
            return Ok(());
        }

        // Compute what fraction of the file's current chunks we want to touch.
        let coverage_fraction = (time_since_last_patrol.as_secs_f64()
            / PATROL_TOUCH_PERIOD.as_secs_f64())
        .clamp(0.0, 1.0);
        let tap_file = replication_buffer::construct_tapped_manifest_path(spool_path, source)?;
        let mut chunks = parse_manifest_chunks(&tap_file)?;
        let mut rng = rand::thread_rng();

        // Touch that fraction of the chunks list, with randomised
        // rounding for any fractional number of chunks: rounding
        // down might consistently round to 0, and rounding up
        // ends up triggering a lot of useless work.
        let desired_touch_count = chunks.len() as f64 * coverage_fraction;
        // Bump the randomised rounding probability up a little: we'd
        // rather touch too many chunks than too few, if something goes
        // wrong with the PRNG.
        let round_up_probability = (desired_touch_count.fract() * 2.0).clamp(0.0, 1.0);
        let touch_count = (desired_touch_count.floor() as usize)
            .saturating_add(rng.gen_bool(round_up_probability) as usize)
            .clamp(0, chunks.len());

        let (shuffled, _) = chunks.partial_shuffle(&mut rng, touch_count);
        if shuffled.is_empty() {
            return Ok(());
        }

        let targets: ReplicationTargetList = {
            let metadata = replication_buffer::buffer_metadata_file(spool_path.to_path_buf());

            // Failing to read replication targets isn't a reason to
            // trigger a full snapshot from scratch.
            let contents = match std::fs::read(&*metadata) {
                Ok(contents) => contents,
                Err(e) => {
                    let _ = chain_error!(e, "failed to read .metadata file", ?metadata);
                    return Ok(());
                }
            };

            match serde_json::from_slice(&contents) {
                Ok(decoded) => decoded,
                Err(e) => {
                    let _ = chain_error!(e, "failed to parse .metadata file", ?metadata, ?contents);
                    return Ok(());
                }
            }
        };

        // Similarly, a failure here shouldn't trigger a full snapshot.
        let creds = match Credentials::default() {
            Ok(creds) => creds,
            Err(e) => {
                let _ = chain_error!(e, "failed to get S3 credentials");
                return Ok(());
            }
        };

        let mut chunks_buckets = targets
            .replication_targets
            .iter()
            .map(|target| create_target(target, |s3| &s3.chunk_bucket, creds.clone()))
            .flatten() // TODO: how do we want to handle failures here?
            .flatten() // Drop `None`
            .collect::<Vec<_>>();

        if chunks_buckets.is_empty() {
            return Ok(());
        }

        for fprint in shuffled {
            self.pace();
            touch_blob(
                &replication_buffer::fingerprint_chunk_name(fprint),
                &mut chunks_buckets,
            )?;
        }

        Ok(())
    }

    fn worker_loop(&self) {
        use std::time::SystemTime;

        // We remember the last buffer we failed to copy, and retry it
        // roughly every RETRY_PERIOD if there's nothing else to do.
        const RETRY_PERIOD: Duration = Duration::from_secs(1);

        let handle = |state: &CopierSpoolState, upload_state: &mut CopierUploadState| {
            let last_scanned = state.last_scanned.load();

            // Check if we have to pause because we're scanning
            // the same directory too frequently.
            if let Ok(elapsed) = last_scanned.elapsed() {
                if let Some(remaining) = MIN_COPY_PERIOD.checked_sub(elapsed) {
                    use rand::Rng;

                    // Time hasn't gone backward, and we have to
                    // wait at least for the `remaining` duration
                    // to elapse.  Do that, plus a random
                    // additional delay of up to `MIN_COPY_PERIOD`
                    // to jitter things a bit.
                    let jitter = MIN_COPY_PERIOD.mul_f64(rand::thread_rng().gen_range(0.0..1.0));
                    std::thread::sleep(remaining + jitter);
                }
            }

            state.signaled.store(false, Ordering::Relaxed);
            state.last_scanned.store_now();
            let stale = state.stale.swap(false, Ordering::Relaxed);
            match call_with_slow_logging(
                Duration::from_secs(60),
                || {
                    self.handle_spooling_directory(
                        &state.last_manifest_copy,
                        upload_state,
                        stale,
                        &state.spool_path,
                        /*sleep_on_credential_failure=*/ true,
                    )
                },
                |duration| tracing::info!(?duration, spool=?state.spool_path, "slow handle_spooling_directory"),
            ) {
                Ok(true) => {
                    state.consecutive_successes.fetch_add(1, Ordering::Relaxed);
                    state.consecutive_failures.store(0, Ordering::Relaxed);
                    state.consecutive_updates.fetch_add(1, Ordering::Relaxed);

                    let now = SystemTime::now();
                    state.last_success.store(now);
                    state.last_update.store(now);
                }
                Ok(false) => {
                    state.consecutive_successes.fetch_add(1, Ordering::Relaxed);
                    state.consecutive_failures.store(0, Ordering::Relaxed);
                    state.consecutive_updates.store(0, Ordering::Relaxed);

                    state.last_success.store_now();
                }
                Err(e) => {
                    let _ =
                        chain_info!(e, "failed to handle spooling directory", ?state.spool_path);
                    state.consecutive_successes.store(0, Ordering::Relaxed);
                    state.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                    state.consecutive_updates.store(0, Ordering::Relaxed);

                    state.last_failure.store_now();
                }
            }

            if last_scanned > SystemTime::UNIX_EPOCH {
                let spool_path = state.spool_path.clone();
                let source = state.source.clone();

                Some(move |this: &CopierWorker| {
                    if let Err(e) = this.patrol_touch_chunks(
                        &spool_path,
                        &source,
                        SystemTime::now()
                            .duration_since(last_scanned)
                            .unwrap_or_default(),
                    ) {
                        let _ = chain_warn!(e, "failed to touch chunks. forcing a full snapshot.",
                                                db=?source, ?spool_path);

                        force_full_snapshot(&spool_path, &source);
                    }
                })
            } else {
                None
            }
        };

        // We remember the last state that we failed to copy because
        // we couldn't get its copy lock, and retry until success.
        let mut queued: Option<Arc<CopierSpoolState>> = None;
        let mut rng = rand::thread_rng();
        let mut get = |queued: &mut Option<Arc<CopierSpoolState>>| {
            use rand::Rng;

            let queued_channel = if queued.is_none() {
                crossbeam_channel::never()
            } else {
                // This is an internal retry that's not really
                // observable externally.  Make sure to jitter, but
                // not worth parameterising.
                crossbeam_channel::after(RETRY_PERIOD.mul_f64(rng.gen_range(1.0..2.0)))
            };
            crossbeam_channel::select! {
                recv(self.edge_work) -> ret => ret,
                recv(self.lag_work) -> ret => ret,
                recv(self.maintenance_work) -> ret => ret,
                // `queued_channel` is `never()` when `queued == None`,
                // so the body must be successful.
                recv(queued_channel) -> _ => Ok(queued.take().expect("must have data")),
            }
        };

        while let Ok(state) = get(&mut queued) {
            // This variable will be populated with `Some(closure)` if
            // we want to touch replicated chunks with a patrol scan,
            // outside the critical section.
            let mut follow_up_work = None;
            let mut success = false;

            if let Ok(mut upload_state) = state.upload_lock.try_lock() {
                follow_up_work = handle(&state, &mut *upload_state);
                success = true;
            }

            if let Some(work) = follow_up_work {
                call_with_slow_logging(
                    Duration::from_secs(60),
                    || work(self),
                    |duration| tracing::info!(?duration, spool=?state.spool_path, "slow follow_up_work"),
                );
            }

            if !success {
                // If we failed to acquire the copy lock, we'll keep
                // retrying the last failure.
                queued = Some(state);
            }
        }
    }
}

/// Returns the path for the source db given the spool directory path.
fn source_db_for_spool(spool_path: &Path) -> Result<Option<PathBuf>> {
    let file_name = spool_path
        .parent()
        .ok_or_else(|| fresh_warn!("Spool path does not have an inode component", ?spool_path))?
        .file_name()
        .ok_or_else(|| fresh_warn!("Spool path does not have a path component", ?spool_path))?
        .to_str()
        .ok_or_else(|| fresh_warn!("Final spool path component is not valid utf-8", ?spool_path))?;

    Ok(replication_buffer::restore_slashes(file_name)?.map(|x| x.into()))
}

impl CopierSpoolState {
    /// Returns a pair of (key, lag info).  The key is the source db
    /// file if it is known, and the mangled replication path otherwise.
    fn lag_info(&self) -> Result<(PathBuf, CopierSpoolLagInfo)> {
        use std::os::unix::fs::MetadataExt;
        use std::time::SystemTime;

        let key = self.source.clone();
        let stat = key.metadata().map_err(|e| {
            filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG,
                               "failed to stat source db file")
        })?;
        let source_file_ctime = SystemTime::UNIX_EPOCH
            + std::time::Duration::new(stat.ctime() as u64, stat.ctime_nsec() as u32);

        let tap_file = replication_buffer::construct_tapped_manifest_path(&self.spool_path, &key)?;
        let (fprint, replicated_file_ctime) = parse_manifest_info(&tap_file)?;

        let sqlite_headers_match = match File::open(&key) {
            Ok(file) => crate::manifest_schema::fingerprint_sqlite_header(&file) == fprint,
            Err(e) => {
                let _ = chain_info!(e, "failed to open source file", ?key);
                false
            }
        };

        if source_file_ctime > replicated_file_ctime && !sqlite_headers_match {
            self.stale.store(true, Ordering::Relaxed);
        }

        Ok((
            key,
            CopierSpoolLagInfo {
                source_file_ctime: source_file_ctime.into(),
                replicated_file_ctime: replicated_file_ctime.into(),
                sqlite_headers_match,

                locked: self.upload_lock.try_lock().is_err(),
                last_scanned: self.last_scanned.load().into(),

                consecutive_successes: self.consecutive_successes.load(Ordering::Relaxed),
                last_success: self.last_success.load().into(),

                consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
                last_failure: self.last_failure.load().into(),

                consecutive_updates: self.consecutive_updates.load(Ordering::Relaxed),
                last_update: self.last_update.load().into(),
            },
        ))
    }
}

impl CopierBackend {
    fn new(
        worker_count: usize,
        channel_capacity: usize,
    ) -> (Self, Sender<Arc<PathBuf>>, Sender<ActiveSetMaintenance>) {
        let governor = Arc::new(governor::RateLimiter::direct_with_clock(
            COPY_RATE_QUOTA,
            &Default::default(),
        ));

        let (edge_workers, edge_recv) = crossbeam_channel::bounded(channel_capacity);
        let (lag_workers, lag_recv) = crossbeam_channel::bounded(channel_capacity);
        let (maintenance_workers, maintenance_recv) = crossbeam_channel::bounded(channel_capacity);
        for i in 0..worker_count.max(1) {
            let worker = CopierWorker {
                edge_work: edge_recv.clone(),
                lag_work: lag_recv.clone(),
                // Don't look for background maintenance work in 1/2
                // (arbitrary fraction) of the workers: let them focus
                // on edge- or lag-triggered work we know has value.
                maintenance_work: if i < worker_count / 2 {
                    crossbeam_channel::never()
                } else {
                    maintenance_recv.clone()
                },
                governor: governor.clone(),
            };

            std::thread::Builder::new()
                .name(format!("verneuil-copier-worker/{}", i))
                .spawn(move || worker.worker_loop())
                .expect("failed to spawn copier worker thread");
        }

        let (buf_send, buf_recv) = crossbeam_channel::bounded(channel_capacity);
        let (maintenance_send, maintenance_recv) = crossbeam_channel::bounded(channel_capacity);

        let backend = CopierBackend {
            ready_buffers: buf_recv,
            maintenance: maintenance_recv,
            edge_workers,
            lag_workers,
            maintenance_workers,
            periodic_lag_scan: crossbeam_channel::tick(REPLICATION_LAG_REPORT_PERIOD),
            active_spool_paths: HashMap::new(),
        };

        (backend, buf_send, maintenance_send)
    }

    /// Attempts to send `work` to the spool directory replication workers.
    /// Returns None on success, `work` if the channel is full, and
    /// panics if the workers disconnected.
    #[instrument]
    fn send_work(
        dst: &Sender<Arc<CopierSpoolState>>,
        work: Arc<CopierSpoolState>,
    ) -> Option<Arc<CopierSpoolState>> {
        use crossbeam_channel::TrySendError::*;
        match dst.try_send(work) {
            Ok(_) => None,
            Err(Full(work)) => Some(work),
            Err(Disconnected(_)) => panic!("workers disconnected"),
        }
    }

    /// Gathers statistics on replication lag for all currently
    /// tracked sqlite db files.
    ///
    /// Logs at WARNing level when lag is above the
    /// `REPLICATION_LAG_REPORT_THRESHOLD`.
    #[instrument(skip(self))]
    fn scan_for_replication_lag(&self) {
        use rand::prelude::SliceRandom;

        let mut newly_stale = Vec::new();
        let mut consistently_stale = Vec::new();

        let stats: BTreeMap<String, CopierSpoolLagInfo> = self
            .active_spool_paths
            .values()
            .map(|spool_state| -> Result<_> {
                let old_stale = spool_state.stale.load(Ordering::Relaxed);

                let (k, v) = spool_state.lag_info().map_err(|e| {
                    chain_error!(e, "failed to extract replication lag info", ?spool_state)
                })?;

                // Accumulate stale states in vectors: we want to push
                // them to the `lag_workers` channel.
                if spool_state.stale.load(Ordering::Relaxed) {
                    if old_stale {
                        consistently_stale.push(spool_state.clone());
                    } else {
                        newly_stale.push(spool_state.clone());
                    }
                }

                Ok((k.to_string_lossy().into_owned(), v))
            })
            .flatten()
            .collect();

        newly_stale.shuffle(&mut rand::thread_rng());
        for state in newly_stale {
            // Sending only fails because the queue is full.  That's
            // not worth complaining about: in the worst case, the
            // background scan will get to this spool directory.
            let _ = self.lag_workers.try_send(state);
        }

        // Queue up states that have been stale for a while if there's
        // still room.
        consistently_stale.shuffle(&mut rand::thread_rng());
        for state in consistently_stale {
            let _ = self.lag_workers.try_send(state);
        }

        let mut num_maybe_stale = 0usize;
        let mut num_stale = 0usize;

        for (path, stats) in stats.iter() {
            // If the replicated file is behind...
            if matches!(stats
                .source_file_ctime
                .signed_duration_since(stats.replicated_file_ctime)
                .to_std(),
                Ok(lag) if lag > Duration::new(0, 1))
            {
                let now: DateTime = std::time::SystemTime::now().into();

                // and this has been the case for a while.
                match now.signed_duration_since(stats.source_file_ctime).to_std() {
                    Ok(delay) if delay >= REPLICATION_LAG_REPORT_THRESHOLD => {
                        let json_stats = serde_json::to_string(&stats)
                            .expect("failed to serialise replication lag statistics");

                        // If the headers match, this is probably a
                        // false positive.  For example, we have seen
                        // this happen with no-op write transactions.
                        if stats.sqlite_headers_match {
                            tracing::debug!(?path, ?delay, %json_stats,
                                            "replication lag exceeds threshold, but file headers are up to date");
                            num_maybe_stale += 1;
                        } else {
                            tracing::warn!(?path, ?delay, %json_stats,
                                           "replication lag exceeds threshold");
                            num_stale += 1;
                        }
                    }
                    _ => {}
                }
            }
        }

        let json_stats =
            serde_json::to_string(&stats).expect("failed to serialise replication lag statistics");
        tracing::info!(num_tracked=stats.len(), num_stale, num_maybe_stale, %json_stats,
                       "computed replication lag");
    }

    /// Queues a state for copying, unless it is already enqueued.
    fn queue_active_state(&self, state: &Arc<CopierSpoolState>) {
        if !state.signaled.load(Ordering::Relaxed) {
            // Flip the flag to true before the worker can
            // reset it.
            state.signaled.store(true, Ordering::Relaxed);
            if Self::send_work(&self.edge_workers, state.clone()).is_some() {
                // If we failed to send work, clear
                // the flag ourself and try again.
                state.signaled.store(false, Ordering::Relaxed);
            }
        }
    }

    /// Handles the next request from our channels.  Returns true on
    /// success, false if the worker thread should abort.
    #[instrument(level = "debug", skip(self))]
    fn handle_one_request(&mut self, timeout: Duration) -> bool {
        use ActiveSetMaintenance::*;

        crossbeam_channel::select! {
            recv(self.ready_buffers) -> ready => match ready {
                Ok(ready) => {
                    // Data in `ready_buffer` is only advisory, it's
                    // never incorrect to drop the work unit.
                    if let Some(state) = self.active_spool_paths.get(&ready) {
                        self.queue_active_state(state);
                    }
                },
                // Errors only happen when there is no more sender.
                // That means the worker should shut down.
                Err(_) => return false,
            },
            recv(self.maintenance) -> maintenance => match maintenance {
                Ok(Join(spool_path, source_or)) => {
                    let mut source = match source_db_for_spool(&spool_path) {
                        Ok(Some(path)) => path,
                        Ok(None) => if let Some(path) = &source_or {
                            path.clone()
                        } else {
                            tracing::debug!(?spool_path, "unable to decode overlong source db path");
                            return true;
                        }
                        Err(err) => {
                            tracing::warn!(?spool_path, ?err, "failed to decode source db path");
                            return true;
                        }
                    };

                    if let Some(path) = &source_or {
                        if path != &source {
                            tracing::warn!(?spool_path, ?source, ?path, "registering a new CopierSpoolState with a mismatching source path");
                            // Override with what the caller says, but
                            // this shouldn't happen.
                            source = path.to_owned();
                        }
                    }

                    self.active_spool_paths
                        .entry(spool_path.clone())
                        .or_insert_with(|| {
                            // The initial spool `Join` message (via
                            // `Copier::with_spool_path`) should
                            // always have a source.  It's only later
                            // clones that lack the source path.
                            if source_or.is_none() {
                                tracing::warn!(?spool_path, ?source,
                                               "registering a new CopierSpoolState without a source path");
                            }

                            Arc::new(CopierSpoolState{
                                spool_path,
                                source,
                                ..Default::default()
                            })
                        })
                        .count
                        // No one else writes to this field, so we don't
                        // have to worry about the count flapping around 0.
                        .fetch_add(1, Ordering::Relaxed);
                },
                Ok(Leave(path)) => {
                    if let Some(v) = self.active_spool_paths.get_mut(&path) {
                        if v.count.fetch_sub(1, Ordering::Relaxed) == 1 {
                            self.active_spool_paths.remove(&path);
                        }
                    }
                },
                Err(_) => return false,
            },
            recv(self.periodic_lag_scan) -> _ => self.scan_for_replication_lag(),
            default(timeout) => {},
        }

        true
    }

    /// Process directories that should be ready for replication, one
    /// at a time.
    ///
    /// When the write ends of the channel are all gone, stop pulling
    /// work.
    fn handle_requests(&mut self) {
        use rand::seq::SliceRandom;
        use rand::Rng;

        fn shuffle_active_set(
            active: &HashMap<Arc<PathBuf>, Arc<CopierSpoolState>>,
            rng: &mut impl rand::Rng,
        ) -> Vec<Arc<CopierSpoolState>> {
            let mut keys: Vec<_> = active.values().cloned().collect();

            keys.shuffle(rng);
            // Move stale records to the end, so they're popped first.
            keys.sort_by(|x, y| {
                x.stale
                    .load(Ordering::Relaxed)
                    .cmp(&y.stale.load(Ordering::Relaxed))
            });
            keys
        }

        let mut rng = rand::thread_rng();
        let mut active_set = shuffle_active_set(&self.active_spool_paths, &mut rng);

        loop {
            let _span = info_span!(
                "handle_requests",
                num_active = self.active_spool_paths.len(),
                num_bg_scan = active_set.len()
            );

            let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);
            if !self.handle_one_request(BACKGROUND_SCAN_PERIOD.mul_f64(jitter_scale)) {
                break;
            }

            match active_set.pop() {
                Some(state) => {
                    // Unconditionally signal for work: we don't want to get
                    // stuck if `state.signaled` somehow never gets cleared,
                    // and the background scan is our catch-all fix for such
                    // liveness bugs.
                    state.signaled.store(true, Ordering::Relaxed);
                    if let Some(state) = Self::send_work(&self.maintenance_workers, state) {
                        state.signaled.store(false, Ordering::Relaxed);
                        // Push pack to the `active_set` on failure.
                        active_set.push(state);
                    }
                }
                None => active_set = shuffle_active_set(&self.active_spool_paths, &mut rng),
            }
        }

        let _span = info_span!(
            "handle_requests_shutdown",
            num_active = self.active_spool_paths.len(),
            num_bg_scan = active_set.len()
        );

        // Try to handle all ready buffers before leaving.  Don't do
        // anything if the work channel is full: we're probably
        // shutting down, and it's unlikely that we'll complete all
        // that work.
        while let Ok(ready) = self.ready_buffers.try_recv() {
            let _span = info_span!("handle_requests_shutdown_ready_buffers", ?ready);
            // Remove from the set of active paths: we want to skip
            // anything that's not active anymore, and we don't want
            // to scan directories twice during shutdown.
            if let Some(state) = self.active_spool_paths.remove(&ready) {
                if !state.signaled.load(Ordering::Relaxed) {
                    Self::send_work(&self.edge_workers, state);
                }
            }
        }

        // One final pass through all remaining spooling directories.
        for state in shuffle_active_set(&self.active_spool_paths, &mut rng) {
            let _span = info_span!("handle_requests_shutdown_bg_scan", ?state);
            if !state.signaled.load(Ordering::Relaxed) {
                Self::send_work(&self.maintenance_workers, state);
            }
        }
    }
}
