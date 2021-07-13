//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
use crate::replication_buffer;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::ReplicationTargetList;
use crate::replication_target::S3ReplicationTarget;

use core::num::NonZeroU32;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
    Join(PathBuf),
    Leave(PathBuf),
}

/// A `Copier` is only a message-passing handle to a background worker
/// thread.
///
/// When all the underlying `Sender` have been dropped, the thread
/// will be notified and commence shutdown.
#[derive(Debug)]
pub(crate) struct Copier {
    ready_buffers: crossbeam_channel::Sender<PathBuf>,
    maintenance: crossbeam_channel::Sender<ActiveSetMaintenance>,
    spool_path: Option<PathBuf>,
}

type Governor = governor::RateLimiter<
    governor::state::direct::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::MonotonicClock,
>;

struct CopierBackend {
    ready_buffers: crossbeam_channel::Receiver<PathBuf>,
    maintenance: crossbeam_channel::Receiver<ActiveSetMaintenance>,
    workers: crossbeam_channel::Sender<PathBuf>,
    // Map from PathBuf for spool directories to refcount.
    active_spool_paths: HashMap<PathBuf, usize>,
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
    pub fn get_global_copier(spool_path: Option<PathBuf>) -> Copier {
        lazy_static::lazy_static! {
            static ref GLOBAL_COPIER: Copier = Copier::new(None);
        }

        GLOBAL_COPIER.with_spool_path(spool_path)
    }

    /// Returns a handle for a fresh Copier.
    pub fn new(spool_path: Option<PathBuf>) -> Copier {
        Copier::new_with_capacity(spool_path, 100)
    }

    /// Increments the refcount for the current spool path, if any.
    fn incref(&self) {
        if let Some(path) = &self.spool_path {
            self.maintenance
                .send(ActiveSetMaintenance::Join(path.clone()))
                .expect("channel should not disconnect");
        }
    }

    /// Decrements the refcount for the current spool path, if any.
    fn delref(&self) {
        if let Some(path) = &self.spool_path {
            self.maintenance
                .send(ActiveSetMaintenance::Leave(path.clone()))
                .expect("channel should not disconnect");
        }
    }

    pub fn with_spool_path(&self, spool_path: Option<PathBuf>) -> Copier {
        let mut copy = self.clone();

        copy.delref();
        copy.spool_path = spool_path;
        copy.incref();
        copy
    }

    /// Returns a handle for a fresh Copier that allows for
    /// `channel_capacity` pending signalled ready buffer
    /// before dropping anything.
    pub fn new_with_capacity(spool_path: Option<PathBuf>, channel_capacity: usize) -> Copier {
        let (mut backend, buf_send, maintenance_send) =
            CopierBackend::new(WORKER_COUNT, channel_capacity);
        std::thread::spawn(move || backend.handle_requests());

        let ret = Copier {
            ready_buffers: buf_send,
            maintenance: maintenance_send,
            spool_path,
        };

        ret.incref();
        ret
    }

    /// Attempts to signal that the "ready" buffer subdirectory in
    /// `parent_directory` is available for copying.
    pub fn signal_ready_buffer(&self) {
        // Eat the failure for now.  We may fail to replicate a write
        // transaction when the copier is falling behind; this delays
        // replication until the next write, but isn't incorrect.
        if let Some(parent_directory) = &self.spool_path {
            let _ = self.ready_buffers.try_send(parent_directory.clone());
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
fn ensure_directory_removed(target: &Path) -> Result<()> {
    match std::fs::remove_dir(&target) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        ret => ret,
    }
}

#[derive(PartialEq, Eq)]
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

    let delete_file = matches!(policy, RemoveFiles | RemoveFilesAndDirectory);
    match std::fs::read_dir(&to_consume) {
        Ok(dirents) => {
            for file in dirents.flatten() {
                let name = file.file_name();

                to_consume.push(&name);
                if let Ok(contents) = File::open(&to_consume) {
                    if consumer(&name, contents).is_ok() && delete_file {
                        // Attempt to remove the file.  It's ok if
                        // this fails: either someone else removed
                        // the file, or `ensure_directory_removed`
                        // will fail, correctly signaling failure.
                        let _ = std::fs::remove_file(&to_consume);
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
        Err(err) => Err(err),
    }
}

/// Creates `bucket` if it does not already exists.
fn ensure_bucket_exists(bucket: &Bucket) -> Result<()> {
    if matches!(bucket.location(), Ok((_, 200))) {
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
        _ => Err(Error::new(
            ErrorKind::Other,
            "failed to create bucket in S3",
        )),
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
                    .map_err(|_| Error::new(ErrorKind::Other, "failed to parse region"))?
            };

            let bucket_name = bucket_extractor(&s3);
            let mut bucket = Bucket::new(bucket_name, region, creds)
                .map_err(|_| Error::new(ErrorKind::Other, "failed to create bucket object"))?;

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
fn copy_file(name: &OsStr, mut contents: File, targets: &[Bucket]) -> Result<()> {
    use rand::Rng;
    use std::io::Read;

    let mut rng = rand::thread_rng();

    let blob_name = name
        .to_str()
        .ok_or_else(|| Error::new(ErrorKind::Other, "invalid name"))?;

    let mut bytes = Vec::new();
    // TODO: check that chunk fingerprints match, check that directories checksum?
    contents.read_to_end(&mut bytes)?;

    for target in targets {
        for i in 0..=COPY_RETRY_LIMIT {
            match target.put_object_with_content_type(&blob_name, &bytes, CHUNK_CONTENT_TYPE) {
                Ok((_, code)) if (200..300).contains(&code) => {
                    break;
                }
                Ok((_, code)) if code < 500 => {
                    // Permanent failure.  In theory, we should maybe
                    // retry on 400: RequestTimeout, but we'll catch
                    // it in the next background scan.
                    return Err(Error::new(ErrorKind::Other, "failed to post chunk"));
                }
                _ => {
                    if i == COPY_RETRY_LIMIT {
                        return Err(Error::new(ErrorKind::Other, "transient failure"));
                    }

                    let sleep = COPY_RETRY_BASE_WAIT.mul_f64(COPY_RETRY_MULTIPLIER.powi(i));
                    let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);
                    std::thread::sleep(sleep.mul_f64(jitter_scale));
                }
            }
        }
    }

    Ok(())
}

/// Fetches the contents of blob `name` in `target`'s chunk bucket.
#[cfg(feature = "verneuil_test_vfs")]
fn fetch_chunk_from_one_target(target: &ReplicationTarget, name: &str) -> Result<Option<Vec<u8>>> {
    let creds = Credentials::default()
        .map_err(|_| Error::new(ErrorKind::Other, "failed to get credentials"))?;
    let bucket = create_target(target, |s3| &s3.chunk_bucket, creds)?;

    match bucket.get_object(name) {
        Ok((payload, 200)) => Ok(Some(payload)),
        Ok((_, 404)) => Ok(None),
        Ok(_) => Err(Error::new(ErrorKind::Other, "failed to get chunk")),
        _ => Err(Error::new(ErrorKind::Other, "failed to connect")),
    }
}

/// Returns whether the directory at `path` is empty or just does
/// not exist at all.
fn directory_is_empty_or_absent(path: &Path) -> Result<bool> {
    match std::fs::read_dir(path) {
        Ok(mut dirents) => Ok(dirents.next().is_none()),
        // It's OK if the directory is already gone (and thus empty).
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err),
    }
}

/// Returns a tuple that identifies a given file; if a given path has
/// the same identifier, it is the same (unless someone is maliciously
/// tampering with it).
fn file_identifier(file: &File) -> Result<(std::time::SystemTime, u64, u64, u64, i64, i64)> {
    use std::os::unix::fs::MetadataExt;

    let meta = file.metadata()?;
    Ok((
        meta.created()?,
        meta.len(),
        meta.dev(),
        meta.ino(),
        meta.ctime(),
        meta.ctime_nsec(),
    ))
}

struct CopierWorker {
    work: crossbeam_channel::Receiver<PathBuf>,
    governor: Arc<Governor>,
}

impl CopierWorker {
    /// Sleeps until the governor lets us fire the next set of API calls.
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
        let (ready, _file) =
            replication_buffer::snapshot_ready_directory(parent.clone()).map_err(|e| e.to_io())?;

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
                |name, file| {
                    self.pace();
                    copy_file(name, file, &chunks_buckets)
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
                |name, file| {
                    self.pace();
                    copy_file(name, file, &meta_buckets)
                },
                ConsumeDirectoryPolicy::RemoveFilesAndDirectory,
            )?;
        }

        // And now try to get rid of the hopefully empty directory.
        let _ = replication_buffer::remove_ready_directory_if_empty(parent);

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
                &mut |name: &OsStr, file| {
                    self.pace();
                    copy_file(name, file, &chunks_buckets)?;
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
            return Err(Error::new(
                ErrorKind::Other,
                "unpublished staged chunks remain",
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
        if !directory_is_empty_or_absent(&replication_buffer::mutable_ready_directory(parent))? {
            return Err(Error::new(ErrorKind::Other, "ready directory exists"));
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
                &mut |name: &OsStr, file| {
                    if initial_meta.get(name) == Some(&file_identifier(&file)?) {
                        self.pace();
                        copy_file(name, file, &meta_buckets)
                    } else {
                        Ok(())
                    }
                },
                ConsumeDirectoryPolicy::KeepAll,
            )?;
        }

        Ok(())
    }

    /// Processes one spooling directory that should be ready for
    /// replication.
    fn handle_spooling_directory(&self, spool: PathBuf) {
        if let Ok(creds) = Credentials::default() {
            // Try to read the metadata JSON, which tells us where to
            // replicate the chunks and meta files.  If we can't do
            // that, leave this precious data where it is...  We don't
            // provide any hard liveness guarantee on replication, so
            // that's not incorrect.  Even when replication is stuck,
            // the buffering system bounds the amount of replication
            // data we keep around.
            let targets_or: Result<ReplicationTargetList> = (|| {
                let metadata = replication_buffer::buffer_metadata_file(spool.clone());
                Ok(serde_json::from_slice(&std::fs::read(&metadata)?)?)
            })();

            if let Ok(targets) = targets_or {
                let ready = replication_buffer::mutable_ready_directory(spool.clone());
                if matches!(directory_is_empty_or_absent(&ready), Ok(true)) {
                    // It's not an error if this fails: we expect
                    // failures when `ready` becomes non-empty, and
                    // we never lose data.
                    let _ = std::fs::remove_dir(&ready);
                } else {
                    // Failures are expected when concurrent processes
                    // or copiers work on the same `path`.  Even when
                    // `handle_directory` fails, we're either making
                    // progress, or `path` is in a bad state and we
                    // choose to keep it untouched rather than drop
                    // data that we have failed to copy to the
                    // replication targets.
                    let _ = self.handle_ready_directory(&targets, creds.clone(), spool.clone());

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
                let _ = self.handle_staging_directory(&targets, creds.clone(), spool.clone());

                // And now see if the ready directory was updated again.
                // We only upload meta files (directory protos) if we
                // observed that the "ready" directory was empty while the
                // meta files had the same value as when we entered
                // "handle_staging_directory".  Anything we now find in
                // the "ready" directory must be at least as recent as
                // what we found in staging, so, again, replication
                // cannot go backwards.
                let _ = self.handle_ready_directory(&targets, creds, spool);

                // When we get here, the remote data should be at least as
                // fresh as the last staged snapshot when we entered the
                // loop body.
            }
        }
    }

    fn worker_loop(&self) {
        while let Ok(path) = self.work.recv() {
            self.handle_spooling_directory(path);
        }
    }
}

impl CopierBackend {
    fn new(
        worker_count: usize,
        channel_capacity: usize,
    ) -> (
        Self,
        crossbeam_channel::Sender<PathBuf>,
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
    fn send_work(&self, work: PathBuf) -> Option<PathBuf> {
        use crossbeam_channel::TrySendError::*;
        match self.workers.try_send(work) {
            Ok(_) => None,
            Err(Full(work)) => Some(work),
            Err(Disconnected(_)) => panic!("workers disconnected"),
        }
    }

    /// Handles the next request from our channels.  Returns true on
    /// success, false if the worker thread should abort.
    fn handle_one_request(&mut self, timeout: Duration) -> bool {
        use ActiveSetMaintenance::*;

        crossbeam_channel::select! {
            recv(self.ready_buffers) -> ready => match ready {
                Ok(ready) => {
                    // Data in `ready_buffer` is only advisory, it's
                    // never incorrect to drop the work unit.
                    self.send_work(ready);
                },
                // Errors only happen when there is no more sender.
                // That means the worker should shut down.
                Err(_) => return false,
            },
            recv(self.maintenance) -> maintenance => match maintenance {
                Ok(Join(path)) => {
                    *self.active_spool_paths.entry(path).or_insert(0) += 1;
                },
                Ok(Leave(path)) => {
                    if let Some(v) = self.active_spool_paths.get_mut(&path) {
                        if *v > 1 {
                            *v -= 1;
                        } else {
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
            active: &HashMap<PathBuf, usize>,
            rng: &mut impl rand::Rng,
        ) -> Vec<PathBuf> {
            let mut keys: Vec<_> = active.keys().cloned().collect();

            keys.shuffle(rng);
            keys
        }

        let mut rng = rand::thread_rng();
        let mut active_set = shuffle_active_set(&self.active_spool_paths, &mut rng);

        loop {
            let jitter_scale = rng.gen_range(1.0..1.0 + RATE_LIMIT_SLEEP_JITTER_FRAC);
            if !self.handle_one_request(BACKGROUND_SCAN_PERIOD.mul_f64(jitter_scale)) {
                break;
            }

            if let Some(path) = active_set.pop() {
                if let Some(path) = self.send_work(path) {
                    // Push pack to the `active_set` on failure.
                    active_set.push(path);
                }
            } else {
                active_set = shuffle_active_set(&self.active_spool_paths, &mut rng);
            }
        }

        // Try to handle all ready buffers before leaving.  Don't do
        // anything if the work channel is full: we're probably
        // shutting down, and it's unlikely that we'll complete all
        // that work.
        while let Ok(ready) = self.ready_buffers.try_recv() {
            // Remove from the set of active paths: we want to skip
            // anything that's not active anymore, and we don't want
            // to scan directories twice during shutdown.
            if self.active_spool_paths.remove(&ready).is_some() {
                self.send_work(ready);
            }
        }

        // One final pass through all remaining spooling directories.
        for path in shuffle_active_set(&self.active_spool_paths, &mut rng) {
            self.send_work(path);
        }
    }
}
