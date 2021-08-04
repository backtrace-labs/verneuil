//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
use core::num::NonZeroU32;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug_span;
use tracing::info_span;
use tracing::instrument;
use tracing::Level;

use crate::chain_debug;
use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::drop_result;
use crate::filtered_io_error;
use crate::fresh_error;
use crate::fresh_warn;
use crate::manifest_schema::clear_version_id;
use crate::manifest_schema::parse_manifest_chunks;
use crate::manifest_schema::parse_manifest_info;
use crate::ofd_lock::OfdLock;
use crate::racy_time::RacySystemTime;
use crate::replication_buffer;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::ReplicationTargetList;
use crate::replication_target::S3ReplicationTarget;
use crate::result::Result;

const CHUNK_CONTENT_TYPE: &str = "application/octet-stream";

/// Rate limit for individual blob uploads.
///
/// We want to guarantee a low average rate (e.g., 2 per second)
/// to bound our costs, but also avoid slowing down replication in
/// the common case, when write transactions are rare.
const COPY_RATE_QUOTA: governor::Quota =
    governor::Quota::per_second(unsafe { NonZeroU32::new_unchecked(2) })
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

/// How many times do we retry on transient errors?
const COPY_RETRY_LIMIT: i32 = 2;

/// Initial back-off delay (+/- jitter).
const COPY_RETRY_BASE_WAIT: Duration = Duration::from_secs(1);

/// Grow the back-off delay by `COPY_RETRY_MULTIPLIER` after
/// consecutive failures.
const COPY_RETRY_MULTIPLIER: f64 = 10.0;

/// Perform background work for one spooling directory approximately
/// once per BACKGROUND_SCAN_PERIOD.
const BACKGROUND_SCAN_PERIOD: Duration = Duration::from_secs(5);

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

/// We aim for ~2 requests / second.  We shouldn't need more than four
/// worker threads to achieve that, with additional sleeping / backoff.
const WORKER_COUNT: usize = 4;

/// How many of the last manifest files we have uploaded must we
/// remember?  This memory lets us avoid repeated uploads of the
/// same "staged" manifest file.
const STAGED_MANIFEST_MEMORY: usize = 2;

/// Try to identify lagging replication roughly at this period.
const REPLICATION_LAG_REPORT_PERIOD: Duration = Duration::from_secs(61);

/// Warn about DB files for which replication is this far behind.
const REPLICATION_LAG_REPORT_THRESHOLD: Duration = Duration::from_secs(120);

/// Time background "touch" to fully cover each db file once per period.
const PATROL_TOUCH_PERIOD: Duration = Duration::from_secs(24 * 3600);

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
    recent_staged_directories: uluru::LRUCache<FileIdentifier, STAGED_MANIFEST_MEMORY>,
}

/// The `CopierSpoolState` represent what we know about the spool for
/// a given replicated sqlite db file.
#[derive(Debug, Default)]
struct CopierSpoolState {
    spool_path: Arc<PathBuf>,
    source: Option<PathBuf>,
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
    upload_lock: std::sync::Mutex<CopierUploadState>,
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
    // When the backend newly detects that replicated data is late, it
    // queues it up for a copy in the `stale_buffers` channel.  We use
    // dedicated channels instead of overloading `ready_buffer` to
    // preserve fairness: we don't want currently up-to-date replicas
    // to keep falling behind until they're detected as stale.
    stale_buffers: (
        Sender<Arc<CopierSpoolState>>,
        Receiver<Arc<CopierSpoolState>>,
    ),
    maintenance: Receiver<ActiveSetMaintenance>,
    workers: Sender<Arc<CopierSpoolState>>,
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
        std::thread::spawn(move || backend.handle_requests());

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
                work: recv,
                governor,
            }
        };
    }

    // Assume the replication target is behind: someone asked for a
    // synchronous copy.
    WORKER.handle_spooling_directory(&mut Default::default(), /*stale=*/ true, path)?;
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
/// When the `consumer` returns Ok for a file, attempts to remove it
/// if the policy is `RemoveFiles` or `RemoveFilesAndDirectory`.
///
/// Finally, ensures the `to_consume` directory is gone if the policy
/// is `RemoveFilesAndDirectory`; on success, this implies that every
/// file in it has been consumed.
fn consume_directory(
    mut to_consume: PathBuf,
    mut consumer: impl FnMut(&OsStr, File) -> Result<()>,
    policy: ConsumeDirectoryPolicy,
) -> Result<()> {
    use ConsumeDirectoryPolicy::*;

    let _span = debug_span!("consume_directory", ?to_consume, ?policy);

    let delete_file = matches!(policy, RemoveFiles | RemoveFilesAndDirectory);
    match std::fs::read_dir(&to_consume) {
        Ok(dirents) => {
            for file in dirents.flatten() {
                let name = file.file_name();

                to_consume.push(&name);
                let file_or = File::open(&to_consume).map_err(|e| {
                    filtered_io_error!(e,
                                       ErrorKind::NotFound => Level::DEBUG,
                                       "failed to open file to copy", path=?to_consume)
                });
                if let Ok(contents) = file_or {
                    if consumer(&name, contents)
                        .map_err(|e| chain_info!(e, "failed to consume file", ?name, ?to_consume))
                        .is_ok()
                        && delete_file
                    {
                        // Attempt to remove the file.  It's ok if
                        // this fails: either someone else removed
                        // the file, or `ensure_directory_removed`
                        // will fail, correctly signaling failure.
                        drop_result!(std::fs::remove_file(&to_consume),
                                     e => filtered_io_error!(
                                         e, ErrorKind::NotFound => Level::DEBUG,
                                         "failed to remove consumed file", path=?to_consume));
                    }
                }

                to_consume.pop();
            }

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
#[instrument(level = "debug")]
fn ensure_bucket_exists(bucket: &Bucket) -> Result<()> {
    let bucket_location = bucket.location().map_err(|e| {
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
        Bucket::create
    } else {
        Bucket::create_with_path_style
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
fn create_target(
    target: &ReplicationTarget,
    bucket_extractor: impl FnOnce(&S3ReplicationTarget) -> &str,
    creds: Credentials,
) -> Result<Bucket> {
    use ReplicationTarget::*;

    let _span = debug_span!("create_target", ?target);

    match target {
        S3(s3) => {
            let region = if let Some(endpoint) = &s3.endpoint {
                s3::Region::Custom {
                    region: s3.region.clone(),
                    endpoint: endpoint.clone(),
                }
            } else {
                s3.region
                    .parse()
                    .map_err(|e| chain_error!(e, "failed to parse S3 region", ?s3))?
            };

            let bucket_name = bucket_extractor(&s3);
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

            Ok(bucket)
        }
    }
}

/// Attempts to publish the `contents` to `name` in all `targets`.
#[instrument(level = "debug")]
fn copy_file(name: &OsStr, contents: &mut File, targets: &[Bucket]) -> Result<()> {
    use rand::Rng;
    use std::io::Read;

    let mut rng = rand::thread_rng();

    let blob_name = name
        .to_str()
        .ok_or_else(|| fresh_error!("invalid name", ?name))?;

    let mut bytes = Vec::new();
    // TODO: check that chunk fingerprints match, check that directories checksum?
    contents
        .read_to_end(&mut bytes)
        .map_err(|e| chain_error!(e, "failed to read file contents", ?blob_name))?;

    for target in targets {
        for i in 0..=COPY_RETRY_LIMIT {
            match target.put_object_with_content_type(&blob_name, &bytes, CHUNK_CONTENT_TYPE) {
                Ok((_, code)) if (200..300).contains(&code) => {
                    break;
                }
                Ok((body, code)) if code < 500 && code != 408 && code != 429 => {
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
                        "backing off after a failed PUT."
                    );
                    std::thread::sleep(backoff);
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
#[instrument(level = "debug")]
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
            &percent_encoding::NON_ALPHANUMERIC,
        );

        target.add_header(COPY_SOURCE, &url_encoded_name.to_string());
        // We're about to copy an object to itself.  S3 only allows this
        // if we replace all metadata.
        target.add_header(METADATA_DIRECTIVE, METADATA_DIRECTIVE_VALUE);

        for i in 0..=COPY_RETRY_LIMIT {
            match target.put_object_with_content_type(&blob_name, &[], CHUNK_CONTENT_TYPE) {
                Ok((_, code)) if (200..300).contains(&code) => {
                    break;
                }
                Ok((body, 404)) => {
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
                        "backing off after a failed PUT."
                    );
                    std::thread::sleep(backoff);
                }
            }
        }
    }

    Ok(())
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
    work: Receiver<Arc<CopierSpoolState>>,
    governor: Arc<Governor>,
}

/// Attempts to force a full snapshot the next time a change `Tracker`
/// synchronises the db file.
#[instrument]
fn force_full_snapshot(state: &CopierSpoolState) {
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
    if let Some(path) = &state.source {
        drop_result!(reset_db_file_id(&path),
                     e => chain_error!(e, "failed to clear version id", ?path));
    }

    // And now delete all (manifest) files in staging/meta.
    let staging = replication_buffer::mutable_staging_directory(state.spool_path.to_path_buf());
    let meta_directory = replication_buffer::directory_meta(staging);
    drop_result!(consume_directory(meta_directory, |_, _| Ok(()),
                                   ConsumeDirectoryPolicy::RemoveFiles),
                 e => chain_error!(e, "failed to delete staged meta files", ?state.spool_path));
}

/// Waits for the meta copy lock on the spooling directory at
/// `parent`.  This function may return Ok() without actually owning
/// the lock: the copy lock is only used to avoid letting slow uploads
/// overwrite more recent data, but isn't necessary for correctness.
/// In the worst case, we will eventually overwrite with fresh data,
/// as long as copiers can make progress.
///
/// Returns Err on failure, Ok on success or if we waited long enough.
#[instrument(level = "debug")]
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
            "successfully acquired meta copy lock after sleeping."
        );
    } else {
        tracing::info!(
            ?backoff,
            ?parent,
            ?ret,
            "failed to acquire meta copy lock after sleeping."
        );

        if rng.gen_bool(COPY_LOCK_RESET_RATE) {
            tracing::info!(?parent, "resetting stuck meta copy lock file.");
            drop_result!(replication_buffer::reset_meta_copy_lock(parent.to_path_buf()),
                         e => chain_error!(e, "failed to reset meta copy lock after acquisition failure",
                                           ?backoff, ?parent));
        }
    }

    ret
}

impl CopierWorker {
    /// Sleeps until the governor lets us fire the next set of API calls.
    #[instrument(level = "debug")]
    fn pace(&self) {
        use rand::Rng;

        match self.governor.check() {
            Ok(_) => {}
            Err(delay) => {
                let mut rng = rand::thread_rng();
                let wait_time = delay.wait_time_from(std::time::Instant::now());
                let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);

                std::thread::sleep(wait_time.mul_f64(jitter_scale));
            }
        }
    }

    /// Handles one "ready" directory: copy the contents, and delete
    /// the corresponding files and directory as we go.  Once *everything*
    /// has been copied, the directory will be empty, which will
    /// make it possible to rename fresh replication data over it.
    ///
    /// Returns whether we successfully updated the remote snapshot.
    fn handle_ready_directory(
        &self,
        targets: &ReplicationTargetList,
        creds: Credentials,
        parent: PathBuf,
    ) -> Result<bool> {
        let _span = info_span!("handle_ready_directory", ?targets, ?parent);

        let (ready, _file) = match replication_buffer::snapshot_ready_directory(parent.clone())? {
            Some(ret) => ret,
            None => return Ok(false),
        };

        {
            let chunks_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.chunk_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .collect::<Vec<_>>();

            // If we don't have replication target, best to leave the data
            // where it is.
            if chunks_buckets.is_empty() {
                return Ok(false);
            }

            consume_directory(
                replication_buffer::directory_chunks(ready.clone()),
                |name, mut file| {
                    self.pace();
                    copy_file(name, &mut file, &chunks_buckets)
                },
                ConsumeDirectoryPolicy::RemoveFilesAndDirectory,
            )?;
        }

        let mut did_something = false;

        {
            let meta_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.manifest_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .collect::<Vec<_>>();

            if meta_buckets.is_empty() {
                return Ok(false);
            }

            let _lock = wait_for_meta_copy_lock(&parent)?;
            consume_directory(
                replication_buffer::directory_meta(ready),
                |name, mut file| {
                    self.pace();
                    copy_file(name, &mut file, &meta_buckets)?;
                    replication_buffer::tap_meta_file(&parent, name, &file).map_err(|e| {
                        chain_warn!(e, "failed to tap replicated meta file", ?name, ?parent)
                    })?;
                    did_something = true;
                    Ok(())
                },
                ConsumeDirectoryPolicy::RemoveFilesAndDirectory,
            )?;
        }

        // And now try to get rid of the hopefully empty directory.
        drop_result!(replication_buffer::remove_ready_directory_if_empty(parent),
                     e => chain_info!(e, "failed to clean up ready directory"));
        Ok(did_something)
    }

    /// Handles one "staging" directory: copy the chunks, then copy
    /// the metadata blobs if nothing has changed since.
    ///
    /// This function can only make progress if the caller first
    /// calls `handle_ready_directory`.
    fn handle_staging_directory(
        &self,
        state: &mut CopierUploadState,
        stale: bool,
        targets: &ReplicationTargetList,
        creds: Credentials,
        parent: PathBuf,
    ) -> Result<bool> {
        use rand::Rng;

        const FORCE_META_PROBABILITY: f64 = 0.05;

        let _span = info_span!("handle_staging_directory", ?targets, ?parent);

        let staging = replication_buffer::mutable_staging_directory(parent.clone());
        let chunks_directory = replication_buffer::directory_chunks(staging.clone());
        let meta_directory = replication_buffer::directory_meta(staging);

        // If true, we always try to upload the contents of the meta
        // directory, even if we think that might be useless.
        let mut force_meta = stale || rand::thread_rng().gen_bool(FORCE_META_PROBABILITY);

        // It's always safe to publish chunks: they don't have any
        // dependency.
        {
            let chunks_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.chunk_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .collect::<Vec<_>>();

            // If we don't have replication target, best to leave the data
            // where it is.
            if chunks_buckets.is_empty() {
                return Ok(false);
            }

            consume_directory(
                chunks_directory.clone(),
                |name: &OsStr, mut file| {
                    self.pace();
                    copy_file(name, &mut file, &chunks_buckets)?;
                    // Always upload the contents of the meta
                    // directory if we found chunks to upload.
                    force_meta = true;
                    Ok(())
                },
                ConsumeDirectoryPolicy::RemoveFiles,
            )?;
        }

        // Snapshot the current meta files.  We hang on to the file to prevent
        // inode reuse.
        let mut initial_meta: HashMap<std::ffi::OsString, (FileIdentifier, File)> = HashMap::new();
        consume_directory(
            meta_directory.clone(),
            &mut |name: &OsStr, file| {
                initial_meta.insert(name.to_owned(), (FileIdentifier::new(&file)?, file));
                Ok(())
            },
            ConsumeDirectoryPolicy::KeepAll,
        )?;

        // We must now make sure that we have published all the chunks
        // before publishing the meta files.
        if !directory_is_empty_or_absent(&chunks_directory)? {
            tracing::info!(?chunks_directory, "unpublished staged chunks remain");
            return Ok(false);
        }

        // If the "ready" directory now exists, we may have observed an
        // empty `chunks_directory` because a Tracker cleared everything
        // once the data to replicate was in "ready."
        //
        // However, if we now observe that the ready directory doesn't
        // exist, either that didn't happen, or another copier already
        // replicated its contents.  Either way, it's safe to copy the
        // meta files (unless they have changed).
        {
            let ready_directory = replication_buffer::mutable_ready_directory(parent.clone());
            if !directory_is_empty_or_absent(&ready_directory)? {
                tracing::info!(?ready_directory, "ready directory exists");
                return Ok(false);
            }
        }

        let mut did_something = false;

        {
            let meta_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.manifest_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .collect::<Vec<_>>();

            if meta_buckets.is_empty() {
                return Ok(false);
            }

            let _lock = wait_for_meta_copy_lock(&parent)?;
            consume_directory(
                meta_directory,
                &mut |name: &OsStr, mut file| {
                    let identifier = FileIdentifier::new(&file)?;

                    // This is a new manifest file, we don't want to upload it.
                    if initial_meta.get(name).map(|x| &x.0) != Some(&identifier) {
                        return Ok(());
                    }

                    // If we're not forcing uploads and we think we
                    // have already uploaded this file, nothing to do.
                    // We must ignore ctime because tapping a file
                    // changes its hardlink count, and thus updates
                    // its ctime.
                    if !force_meta
                        && state
                            .recent_staged_directories
                            .find(|x| x.equal_except_ctime(&identifier))
                            .is_some()
                    {
                        return Ok(());
                    }

                    self.pace();
                    copy_file(name, &mut file, &meta_buckets)?;
                    replication_buffer::tap_meta_file(&parent, name, &file).map_err(|e| {
                        chain_warn!(e, "failed to tap replicated meta file", ?name, ?parent)
                    })?;

                    state.recent_staged_directories.insert(identifier);
                    did_something = true;
                    Ok(())
                },
                ConsumeDirectoryPolicy::KeepAll,
            )?;
        }

        Ok(did_something)
    }

    /// Processes one spooling directory that should be ready for
    /// replication.
    ///
    /// Returns `Err` on failure, `Ok(true)` if we updated snapshots,
    /// and `Ok(false)` if we successfully did not make progress.
    #[instrument]
    fn handle_spooling_directory(
        &self,
        state: &mut CopierUploadState,
        stale: bool,
        spool: &Path,
    ) -> Result<bool> {
        let mut did_something = false;
        let creds =
            Credentials::default().map_err(|e| chain_error!(e, "failed to get S3 credentials"))?;

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

        let ready = replication_buffer::mutable_ready_directory(spool.to_path_buf());
        let is_empty = directory_is_empty_or_absent(&ready)
            .map_err(|e| chain_debug!(e, "failed to list directory", ?ready));
        if matches!(is_empty, Ok(true)) {
            // It's not an error if this fails: we expect
            // failures when `ready` becomes non-empty, and
            // we never lose data.
            drop_result!(std::fs::remove_dir(&ready),
                         e => filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG,
                                                 "failed to remove ready directory", ?ready));
        } else {
            // Failures are expected when concurrent processes
            // or copiers work on the same `path`.  Even when
            // `handle_directory` fails, we're either making
            // progress, or `path` is in a bad state and we
            // choose to keep it untouched rather than drop
            // data that we have failed to copy to the
            // replication targets.
            did_something |= self
                .handle_ready_directory(&targets, creds.clone(), spool.to_path_buf())
                .map_err(|e| chain_warn!(e, "failed to handle ready directory", ?spool))?;

            // We've done some work here (handled a non-empty
            // "ready" directory).  If the governor isn't
            // immediately ready for us, bail and let another
            // directory make progress.
            if self.governor.check().is_err() {
                return Ok(did_something);
            }
        }

        // Opportunistically try to copy from the "staging"
        // directory.  That's never staler than "ready", so we do
        // not go backward in our replication.
        match self.handle_staging_directory(
            state,
            stale,
            &targets,
            creds.clone(),
            spool.to_path_buf(),
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
        match self.handle_ready_directory(&targets, creds, spool.to_path_buf()) {
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
    #[instrument]
    fn patrol_touch_chunks(
        &self,
        spool_state: &CopierSpoolState,
        time_since_last_patrol: Duration,
    ) -> Result<()> {
        use rand::seq::SliceRandom;
        use rand::Rng;

        let spool_path = &*spool_state.spool_path;
        let source = match &spool_state.source {
            Some(source) => source,
            None => {
                tracing::debug!(?spool_path, ?spool_state, "source db file unknown");
                return Ok(());
            }
        };

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
        let tap_file = replication_buffer::construct_tapped_meta_path(spool_path, source)?;
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
            .collect::<Vec<_>>();

        if chunks_buckets.is_empty() {
            return Ok(());
        }

        for fprint in shuffled {
            self.pace();
            touch_blob(
                &replication_buffer::fingerprint_chunk_name(&fprint),
                &mut chunks_buckets,
            )?;
        }

        Ok(())
    }

    fn worker_loop(&self) {
        use std::time::SystemTime;

        while let Ok(state) = self.work.recv() {
            if let Ok(mut upload_state) = state.upload_lock.try_lock() {
                let last_scanned = state.last_scanned.load();

                state.signaled.store(false, Ordering::Relaxed);
                state.last_scanned.store_now();
                let stale = state.stale.swap(false, Ordering::Relaxed);
                match self.handle_spooling_directory(&mut upload_state, stale, &state.spool_path) {
                    Ok(true) => {
                        state.consecutive_successes.fetch_add(1, Ordering::Relaxed);
                        state.consecutive_failures.store(0, Ordering::Relaxed);
                        state.consecutive_updates.fetch_add(1, Ordering::Relaxed);

                        let now = std::time::SystemTime::now();
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
                        let _ = chain_info!(e, "failed to handle spooling directory", ?state.spool_path);
                        state.consecutive_successes.store(0, Ordering::Relaxed);
                        state.consecutive_failures.fetch_add(1, Ordering::Relaxed);
                        state.consecutive_updates.store(0, Ordering::Relaxed);

                        state.last_failure.store_now();
                    }
                }

                if last_scanned > SystemTime::UNIX_EPOCH {
                    if let Err(e) = self.patrol_touch_chunks(
                        &state,
                        SystemTime::now()
                            .duration_since(last_scanned)
                            .unwrap_or_default(),
                    ) {
                        let _ = chain_warn!(e, "failed to touch chunks. forcing a full snapshot.",
                                            db=?state.source, ?state.spool_path);

                        force_full_snapshot(&state);
                    }
                }
            }
        }
    }
}

impl CopierSpoolState {
    /// Returns a pair of (key, lag info).  The key is the source db
    /// file if it is known, and the mangled replication path otherwise.
    fn lag_info(&self) -> Result<(PathBuf, CopierSpoolLagInfo)> {
        use std::os::unix::fs::MetadataExt;
        use std::time::SystemTime;

        let (key, source_file_ctime, replicated_file_ctime, sqlite_headers_match) =
            if let Some(path) = &self.source {
                let stat = path.metadata().map_err(|e| {
                    filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG,
                                       "failed to stat source db file")
                })?;
                let source_ctime = SystemTime::UNIX_EPOCH
                    + std::time::Duration::new(stat.ctime() as u64, stat.ctime_nsec() as u32);

                let tap_file =
                    replication_buffer::construct_tapped_meta_path(&self.spool_path, path)?;
                let (fprint, ctime) = parse_manifest_info(&tap_file)?;

                let headers_match = match File::open(&path) {
                    Ok(file) => crate::manifest_schema::fingerprint_sqlite_header(&file) == fprint,
                    Err(e) => {
                        let _ = chain_info!(e, "failed to open source file", ?path);
                        false
                    }
                };

                if source_ctime > ctime && !headers_match {
                    self.stale.store(true, Ordering::Relaxed);
                }

                (path.clone(), source_ctime, ctime, headers_match)
            } else {
                (
                    Path::new(self.spool_path.file_name().unwrap_or_default()).to_path_buf(),
                    SystemTime::UNIX_EPOCH,
                    SystemTime::UNIX_EPOCH,
                    false,
                )
            };

        Ok((
            key,
            CopierSpoolLagInfo {
                source_file_ctime: source_file_ctime.into(),
                replicated_file_ctime: replicated_file_ctime.into(),
                sqlite_headers_match,
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

        let (worker_send, worker_recv) = crossbeam_channel::bounded(channel_capacity);
        for _ in 0..worker_count.max(1) {
            let worker = CopierWorker {
                work: worker_recv.clone(),
                governor: governor.clone(),
            };

            std::thread::spawn(move || worker.worker_loop());
        }

        let (buf_send, buf_recv) = crossbeam_channel::bounded(channel_capacity);
        let (maintenance_send, maintenance_recv) = crossbeam_channel::bounded(channel_capacity);

        let backend = CopierBackend {
            ready_buffers: buf_recv,
            stale_buffers: crossbeam_channel::bounded(channel_capacity),
            maintenance: maintenance_recv,
            workers: worker_send,
            periodic_lag_scan: crossbeam_channel::tick(REPLICATION_LAG_REPORT_PERIOD),
            active_spool_paths: HashMap::new(),
        };

        (backend, buf_send, maintenance_send)
    }

    /// Attempts to send `work` to the spool directory replication workers.
    /// Returns None on success, `work` if the channel is full, and
    /// panics if the workers disconnected.
    #[instrument]
    fn send_work(&self, work: Arc<CopierSpoolState>) -> Option<Arc<CopierSpoolState>> {
        use crossbeam_channel::TrySendError::*;
        match self.workers.try_send(work) {
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
    #[instrument]
    fn scan_for_replication_lag(&self) {
        use rand::prelude::SliceRandom;

        let mut newly_stale = Vec::new();

        let stats: BTreeMap<String, CopierSpoolLagInfo> = self
            .active_spool_paths
            .values()
            .map(|spool_state| -> Result<_> {
                let old_stale = spool_state.stale.load(Ordering::Relaxed);

                let (k, v) = spool_state.lag_info().map_err(|e| {
                    chain_error!(e, "failed to extract replication lag info", ?spool_state)
                })?;

                // Accumulate states that are newly detected as stale
                // in the `newly_stale` vector: we want to push them to
                // the `stale_buffers` channel.
                if !old_stale && spool_state.stale.load(Ordering::Relaxed) {
                    newly_stale.push(spool_state.clone());
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
            let _ = self.stale_buffers.0.try_send(state);
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

                        // If the headers match, this is probably a false positive.,
                        if stats.sqlite_headers_match {
                            tracing::info!(?path, ?delay, %json_stats,
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
            if self.send_work(state.clone()).is_some() {
                // If we failed to send work, clear
                // the flag ourself and try again.
                state.signaled.store(false, Ordering::Relaxed);
            }
        }
    }

    /// Handles the next request from our channels.  Returns true on
    /// success, false if the worker thread should abort.
    #[instrument(level = "debug")]
    fn handle_one_request(&mut self, timeout: Duration) -> bool {
        use ActiveSetMaintenance::*;

        crossbeam_channel::select! {
            recv(self.ready_buffers) -> ready => match ready {
                Ok(ready) => {
                    // Data in `ready_buffer` is only advisory, it's
                    // never incorrect to drop the work unit.
                    if let Some(state) = self.active_spool_paths.get(&ready) {
                        self.queue_active_state(&state);
                    }
                },
                // Errors only happen when there is no more sender.
                // That means the worker should shut down.
                Err(_) => return false,
            },
            recv(self.stale_buffers.1) -> stale => match stale {
                Ok(stale) => self.queue_active_state(&stale),
                Err(_) => return false,
            },
            recv(self.maintenance) -> maintenance => match maintenance {
                Ok(Join(spool_path, source_or)) => {
                    self.active_spool_paths
                        .entry(spool_path.clone())
                        .or_insert_with(|| {
                            // The initial spool `Join` message (via
                            // `Copier::with_spool_path`) should
                            // always have a source.  It's only later
                            // clones that lack the source path.
                            if source_or.is_none() {
                                tracing::warn!(?spool_path,
                                               "registering a new CopierSpoolState without a source path.");
                            }

                            Arc::new(CopierSpoolState{
                                spool_path,
                                source: source_or,
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
                    if let Some(state) = self.send_work(state) {
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
                    self.send_work(state);
                }
            }
        }

        // One final pass through all remaining spooling directories.
        for state in shuffle_active_set(&self.active_spool_paths, &mut rng) {
            let _span = info_span!("handle_requests_shutdown_bg_scan", ?state);
            if !state.signaled.load(Ordering::Relaxed) {
                self.send_work(state);
            }
        }
    }
}
