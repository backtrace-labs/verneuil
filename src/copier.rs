//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
use crate::replication_buffer;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::ReplicationTargetList;
use crate::replication_target::S3ReplicationTarget;

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

const CHUNK_CONTENT_TYPE: &str = "application/octet-stream";

/// A `Copier` is only a message-passing handle to a background worker
/// thread.
///
/// When all the underlying `Sender` have been dropped, the thread
/// will be notified and commence shutdown.
#[derive(Clone, Debug)]
pub(crate) struct Copier {
    ready_buffers: crossbeam_channel::Sender<PathBuf>,
}

struct CopierBackend {
    ready_buffers: crossbeam_channel::Receiver<PathBuf>,
}

impl Copier {
    /// Returns a handle for the global `Copier` worker.
    pub fn get_global_copier() -> Copier {
        lazy_static::lazy_static! {
            static ref GLOBAL_COPIER: Copier = Copier::new();
        }

        GLOBAL_COPIER.clone()
    }

    /// Returns a handle for a fresh Copier.
    pub fn new() -> Copier {
        Copier::new_with_capacity(1000)
    }

    /// Returns a handle for a fresh Copier that allows for
    /// `channel_capacity` pending signalled ready buffer
    /// before dropping anything.
    pub fn new_with_capacity(channel_capacity: usize) -> Copier {
        let (sender, receiver) = crossbeam_channel::bounded(channel_capacity);
        let backend = CopierBackend {
            ready_buffers: receiver,
        };
        std::thread::spawn(move || backend.handle_requests());

        Copier {
            ready_buffers: sender,
        }
    }

    /// Attempts to signal that the "ready" buffer subdirectory in
    /// `parent_directory` is available for copying.
    pub fn signal_ready_buffer(&self, parent_directory: PathBuf) {
        // Eat the failure for now.  We may fail to replicate a write
        // transaction when the copier is falling behind; this delays
        // replication until the next write, but isn't incorrect.
        let _ = self.ready_buffers.try_send(parent_directory);
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
    use std::io::Read;

    let blob_name = name
        .to_str()
        .ok_or_else(|| Error::new(ErrorKind::Other, "invalid name"))?;

    let mut bytes = Vec::new();
    // TODO: check that chunk fingerprints match, check that directories checksum?
    contents.read_to_end(&mut bytes)?;

    for target in targets {
        match target.put_object_with_content_type(&blob_name, &bytes, CHUNK_CONTENT_TYPE) {
            Ok((_, code)) if (200..300).contains(&code) => {
                // Success!
            }
            Ok((_, code)) if code < 500 => {
                // Permanent failure.
                return Err(Error::new(ErrorKind::Other, "failed to post chunk"));
            }
            _ => {
                // Should retry here.
                return Err(Error::new(ErrorKind::Other, "transient failure"));
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

impl CopierBackend {
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
                |name, file| copy_file(name, file, &chunks_buckets),
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
                |name, file| copy_file(name, file, &meta_buckets),
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

    /// Process directories that should be ready for replication, one
    /// at a time.
    ///
    /// When the write ends of the channel are all gone, stop pulling
    /// work.
    fn handle_requests(&self) {
        // This only fails when the channel is closed.
        while let Ok(path) = self.ready_buffers.recv() {
            if let Ok(creds) = Credentials::default() {
                // Try to read the metadata JSON, which tells us where to
                // replicate the chunks and meta files.  If we can't do
                // that, leave this precious data where it is...  We don't
                // provide any hard liveness guarantee on replication, so
                // that's not incorrect.  Even when replication is stuck,
                // the buffering system bounds the amount of replication
                // data we keep around.
                let targets_or: Result<ReplicationTargetList> = (|| {
                    let metadata = replication_buffer::buffer_metadata_file(path.clone());
                    Ok(serde_json::from_slice(&std::fs::read(&metadata)?)?)
                })();

                if let Ok(targets) = targets_or {
                    // Failures are expected when concurrent processes or copiers
                    // work on the same `path`.  Even when `handle_directory`
                    // fails, we're either making progress, or `path` is in a bad
                    // state and we choose to keep it untouched rather than drop
                    // data that we have failed to copy to the replication targets.
                    let _ = self.handle_ready_directory(&targets, creds.clone(), path.clone());

                    // Opportunistically try to copy from the "staging"
                    // directory.  That's never staler than "ready", so we do
                    // not go backward in our replication.
                    let _ = self.handle_staging_directory(&targets, creds.clone(), path.clone());

                    // And now see if the ready directory was updated again.
                    // We only upload meta files (directory protos) if we
                    // observed that the "ready" directory was empty while the
                    // meta files had the same value as when we entered
                    // "handle_staging_directory".  Anything we now find in
                    // the "ready" directory must be at least as recent as
                    // what we found in staging, so, again, replication
                    // cannot go backwards.
                    let _ = self.handle_ready_directory(&targets, creds, path);

                    // When we get here, the remote data should be at least as
                    // fresh as the last staged snapshot when we entered the
                    // loop body.
                }
            }
        }
    }
}
