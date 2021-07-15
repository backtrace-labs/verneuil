//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
use core::num::NonZeroU32;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
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
use crate::fresh_info;
use crate::fresh_warn;
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

/// We aim for ~2 requests / second.  We shouldn't need more than two
/// worker threads to achieve that.
const WORKER_COUNT: usize = 2;

/// Messages to maintain the set of active spooling directory: `Join`
/// adds a new directory, `Leave` removes it.  There may be multiple
/// copiers for the same directory, so we must refcount them.
enum ActiveSetMaintenance {
    Join(Arc<PathBuf>),
    Leave(Arc<PathBuf>),
}

/// A `Copier` is only a message-passing handle to a background worker
/// thread.
///
/// When all the underlying `Sender` have been dropped, the thread
/// will be notified and commence shutdown.
#[derive(Debug)]
pub(crate) struct Copier {
    ready_buffers: crossbeam_channel::Sender<Arc<PathBuf>>,
    maintenance: crossbeam_channel::Sender<ActiveSetMaintenance>,
    spool_path: Option<Arc<PathBuf>>,
}

type Governor = governor::RateLimiter<
    governor::state::direct::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::MonotonicClock,
>;

/// The `CopierSpoolState` represent what we know about the spool for
/// a given replicated sqlite db file.
#[derive(Debug)]
struct CopierSpoolState {
    path: Arc<PathBuf>,
    count: AtomicUsize,
}

#[derive(Debug)]
struct CopierBackend {
    ready_buffers: crossbeam_channel::Receiver<Arc<PathBuf>>,
    maintenance: crossbeam_channel::Receiver<ActiveSetMaintenance>,
    workers: crossbeam_channel::Sender<Arc<CopierSpoolState>>,
    // Map from PathBuf for spool directories to their state.
    // The key is always equal to the value's `path` field.
    active_spool_paths: HashMap<Arc<PathBuf>, Arc<CopierSpoolState>>,
}

impl Clone for Copier {
    fn clone(&self) -> Self {
        self.incref();

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
    fn incref(&self) {
        if let Some(path) = &self.spool_path {
            self.maintenance
                .send(ActiveSetMaintenance::Join(path.clone()))
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
    pub fn with_spool_path(mut self, spool_path: Arc<PathBuf>) -> Copier {
        let noop = matches!(&self.spool_path, Some(old) if *old == spool_path);
        if !noop {
            self.delref();
            self.spool_path = Some(spool_path);
            self.incref();
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

        ret.incref();
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

/// Attempts to fetch `name` from each chunk bucket in `targets`.
/// Returns the result for each target, or None if missing.
///
/// Only used for internal testing.
#[cfg(feature = "verneuil_test_vfs")]
pub(crate) fn fetch_chunk_from_targets(
    targets: &[ReplicationTarget],
    name: &str,
) -> Vec<Result<Option<Vec<u8>>>> {
    targets
        .iter()
        .map(|target| fetch_chunk_from_one_target(target, name))
        .collect()
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
                Ok((body, code)) if code < 500 => {
                    // Permanent failure.  In theory, we should maybe
                    // retry on 400: RequestTimeout, but we'll catch
                    // it in the next background scan.
                    return Err(chain_error!(
                        (body, code),
                        "failed to post chunk",
                        ?blob_name,
                        len = bytes.len()
                    ));
                }
                err => {
                    if i == COPY_RETRY_LIMIT {
                        return Err(chain_warn!(
                            err,
                            "reached retry limit",
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

/// Fetches the contents of blob `name` in `target`'s chunk bucket.
#[cfg(feature = "verneuil_test_vfs")]
fn fetch_chunk_from_one_target(target: &ReplicationTarget, name: &str) -> Result<Option<Vec<u8>>> {
    let creds = Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;
    let bucket = create_target(target, |s3| &s3.chunk_bucket, creds)?;

    match bucket.get_object(name) {
        Ok((payload, 200)) => Ok(Some(payload)),
        Ok((_, 404)) => Ok(None),
        ret => Err(chain_error!(ret, "failed to get chunk")),
    }
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

/// Returns a tuple that identifies a given file; if a given path has
/// the same identifier, it is the same (unless someone is maliciously
/// tampering with it).
#[instrument(level = "debug")]
fn file_identifier(
    file: &File,
) -> Result<(Option<std::time::SystemTime>, u64, u64, u64, i64, i64)> {
    use std::os::unix::fs::MetadataExt;

    let meta = file
        .metadata()
        .map_err(|e| chain_error!(e, "failed to stat file"))?;
    Ok((
        meta.created()
            .map_err(|e| chain_debug!(e, "failed to compute file creation time", ?meta))
            .ok(),
        meta.len(),
        meta.dev(),
        meta.ino(),
        meta.ctime(),
        meta.ctime_nsec(),
    ))
}

#[derive(Debug)]
struct CopierWorker {
    work: crossbeam_channel::Receiver<Arc<CopierSpoolState>>,
    governor: Arc<Governor>,
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
    fn handle_ready_directory(
        &self,
        targets: &ReplicationTargetList,
        creds: Credentials,
        parent: PathBuf,
    ) -> Result<()> {
        let _span = info_span!("handle_ready_directory", ?targets, ?parent);

        let (ready, _file) = replication_buffer::snapshot_ready_directory(parent.clone())?;

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
                return Ok(());
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

        {
            let meta_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.directory_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .collect::<Vec<_>>();

            if meta_buckets.is_empty() {
                return Ok(());
            }

            consume_directory(
                replication_buffer::directory_meta(ready),
                |name, mut file| {
                    self.pace();
                    copy_file(name, &mut file, &meta_buckets)?;
                    replication_buffer::tap_meta_file(&parent, name, &file).map_err(|e| {
                        chain_warn!(e, "failed to tap replicated meta file", ?name, ?parent)
                    })
                },
                ConsumeDirectoryPolicy::RemoveFilesAndDirectory,
            )?;
        }

        // And now try to get rid of the hopefully empty directory.
        drop_result!(replication_buffer::remove_ready_directory_if_empty(parent),
                     e => chain_info!(e, "failed to clean up ready directory"));

        Ok(())
    }

    /// Handles one "staging" directory: copy the chunks, then copy
    /// the metadata blobs if nothing has changed since.
    ///
    /// This function can only make progress if the caller first
    /// calls `handle_ready_directory`.
    fn handle_staging_directory(
        &self,
        targets: &ReplicationTargetList,
        creds: Credentials,
        parent: PathBuf,
    ) -> Result<()> {
        let _span = info_span!("handle_staging_directory", ?targets, ?parent);

        let staging = replication_buffer::mutable_staging_directory(parent.clone());
        let chunks_directory = replication_buffer::directory_chunks(staging.clone());
        let meta_directory = replication_buffer::directory_meta(staging);

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
                return Ok(());
            }

            let mut published = 0;

            consume_directory(
                chunks_directory.clone(),
                &mut |name: &OsStr, mut file| {
                    self.pace();
                    copy_file(name, &mut file, &chunks_buckets)?;
                    published += 1;
                    Ok(())
                },
                ConsumeDirectoryPolicy::RemoveFiles,
            )?;

            // Assume there is no update to publish if the chunks
            // directory is empty.
            if published == 0 {
                return Ok(());
            }
        }

        // Snapshot the current meta files.
        let mut initial_meta = HashMap::new();
        consume_directory(
            meta_directory.clone(),
            &mut |name: &OsStr, file| {
                initial_meta.insert(name.to_owned(), file_identifier(&file)?);
                Ok(())
            },
            ConsumeDirectoryPolicy::KeepAll,
        )?;

        // We must now make sure that we have published all the chunks
        // before publishing the meta files.
        if !directory_is_empty_or_absent(&chunks_directory)? {
            return Err(fresh_info!(
                "unpublished staged chunks remain",
                ?chunks_directory
            ));
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
                return Err(fresh_info!("ready directory exists", ?ready_directory));
            }
        }

        {
            let meta_buckets = targets
                .replication_targets
                .iter()
                .map(|target| create_target(target, |s3| &s3.directory_bucket, creds.clone()))
                .flatten() // TODO: how do we want to handle failures here?
                .collect::<Vec<_>>();

            if meta_buckets.is_empty() {
                return Ok(());
            }

            consume_directory(
                meta_directory,
                &mut |name: &OsStr, mut file| {
                    if initial_meta.get(name) == Some(&file_identifier(&file)?) {
                        self.pace();
                        copy_file(name, &mut file, &meta_buckets)?;
                        replication_buffer::tap_meta_file(&parent, name, &file).map_err(|e| {
                            chain_warn!(e, "failed to tap replicated meta file", ?name, ?parent)
                        })?;
                    }

                    Ok(())
                },
                ConsumeDirectoryPolicy::KeepAll,
            )?;
        }

        Ok(())
    }

    /// Processes one spooling directory that should be ready for
    /// replication.
    #[instrument]
    fn handle_spooling_directory(&self, spool: &Path) {
        let creds_or =
            Credentials::default().map_err(|e| chain_error!(e, "failed to get S3 credentials"));
        if let Ok(creds) = creds_or {
            // Try to read the metadata JSON, which tells us where to
            // replicate the chunks and meta files.  If we can't do
            // that, leave this precious data where it is...  We don't
            // provide any hard liveness guarantee on replication, so
            // that's not incorrect.  Even when replication is stuck,
            // the buffering system bounds the amount of replication
            // data we keep around.
            let targets_or: Result<ReplicationTargetList> = (|| {
                let metadata = replication_buffer::buffer_metadata_file(spool.to_path_buf());
                let contents = std::fs::read(&*metadata)
                    .map_err(|e| chain_error!(e, "failed to read .metadata file", ?metadata))?;
                let parsed = serde_json::from_slice(&contents).map_err(|e| {
                    chain_error!(e, "failed to parse .metadata file", ?metadata, ?contents)
                })?;

                Ok(parsed)
            })();

            if let Ok(targets) = targets_or {
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
                    drop_result!(self.handle_ready_directory(&targets, creds.clone(), spool.to_path_buf()),
                                 e => chain_info!(e, "failed to handle ready directory", ?spool));

                    // We've done some work here (handled a non-empty
                    // "ready" directory).  If the governor isn't
                    // immediately ready for us, bail and let another
                    // directory make progress.
                    if self.governor.check().is_err() {
                        return;
                    }
                }

                // Opportunistically try to copy from the "staging"
                // directory.  That's never staler than "ready", so we do
                // not go backward in our replication.
                drop_result!(self.handle_staging_directory(&targets, creds.clone(), spool.to_path_buf()),
                             e => chain_info!(e, "failed to handle staging directory", ?spool));

                // And now see if the ready directory was updated again.
                // We only upload meta files (directory protos) if we
                // observed that the "ready" directory was empty while the
                // meta files had the same value as when we entered
                // "handle_staging_directory".  Anything we now find in
                // the "ready" directory must be at least as recent as
                // what we found in staging, so, again, replication
                // cannot go backwards.
                drop_result!(self.handle_ready_directory(&targets, creds, spool.to_path_buf()),
                             e => chain_info!(e, "failed to rehandle ready directory", ?spool));

                // When we get here, the remote data should be at least as
                // fresh as the last staged snapshot when we entered the
                // loop body.
            }
        }
    }

    fn worker_loop(&self) {
        while let Ok(state) = self.work.recv() {
            self.handle_spooling_directory(&state.path);
        }
    }
}

impl CopierBackend {
    fn new(
        worker_count: usize,
        channel_capacity: usize,
    ) -> (
        Self,
        crossbeam_channel::Sender<Arc<PathBuf>>,
        crossbeam_channel::Sender<ActiveSetMaintenance>,
    ) {
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
            maintenance: maintenance_recv,
            workers: worker_send,
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
            self.send_work(state.clone());
            }
                },
                // Errors only happen when there is no more sender.
                // That means the worker should shut down.
                Err(_) => return false,
            },
            recv(self.maintenance) -> maintenance => match maintenance {
                Ok(Join(path)) => {
                    self.active_spool_paths
                        .entry(path.clone())
                        .or_insert_with(|| Arc::new(CopierSpoolState{
                            path,
                            count: AtomicUsize::new(0),
                        }))
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

            if let Some(state) = active_set.pop() {
                if let Some(state) = self.send_work(state) {
                    // Push pack to the `active_set` on failure.
                    active_set.push(state);
                }
            } else {
                active_set = shuffle_active_set(&self.active_spool_paths, &mut rng);
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
                self.send_work(state);
            }
        }

        // One final pass through all remaining spooling directories.
        for path in shuffle_active_set(&self.active_spool_paths, &mut rng) {
            let _span = info_span!("handle_requests_shutdown_bg_scan", ?path);
            self.send_work(path);
        }
    }
}
