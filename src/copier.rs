//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
use crate::replication_buffer;
use crate::replication_target::ReplicationTargetList;

use std::ffi::OsStr;
use std::fs::File;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;

/// A `Copier` is only a message-passing handle to a background worker
/// thread.
///
/// When all the underlying `Sender` have been dropped, the thread
/// will be notified and commence shutdown.
#[derive(Clone, Debug)]
pub(crate) struct Copier {
    ready_buffers: crossbeam_channel::Sender<PathBuf>,
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
        std::thread::spawn(move || handle_requests(receiver));

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

/// Lists the files in `to_consume`, and passes them to `consumer`.
/// When the `consumer` returns Ok for a file, attempts to remove it.
///
/// Finally, ensures the `to_consume` directory is gone, which implies
/// the every file in it has been consumed.
fn consume_directory(
    mut to_consume: PathBuf,
    mut consumer: impl FnMut(&OsStr, File) -> Result<()>,
) -> Result<()> {
    match std::fs::read_dir(&to_consume) {
        Ok(dirents) => {
            for file in dirents.flatten() {
                let name = file.file_name();

                to_consume.push(&name);
                if let Ok(contents) = File::open(&to_consume) {
                    if consumer(&name, contents).is_ok() {
                        // Attempt to remove the file.  It's ok if
                        // this fails: either someone else removed
                        // the file, or `ensure_directory_removed`
                        // will fail, correctly signaling failure.
                        let _ = std::fs::remove_file(&to_consume);
                    }
                }

                to_consume.pop();
            }

            // If we can't get rid of that directory, it must be
            // non-empty, which means we failed to consume some
            // file... in which case we must report failure.
            ensure_directory_removed(&to_consume)
        }

        // It's OK if the directory is already gone (and thus empty).
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Dummy implementations of the code to send blobs to S3.
fn send_chunk(_name: &OsStr, _contents: File, _targets: &ReplicationTargetList) -> Result<()> {
    Ok(())
}

fn send_meta(_name: &OsStr, _contents: File, _targets: &ReplicationTargetList) -> Result<()> {
    Ok(())
}

/// Handles one "replicating" directory: copy the contents, and delete
/// the corresponding files and directory as we go.  Once *everything*
/// has been copied, the `parent` directory will be empty, which will
/// make it possible to rename the "ready" directory to "replicating."
fn handle_directory(parent: PathBuf) -> Result<()> {
    let (replicating, _file) = replication_buffer::snapshot_replicating_directory(parent)?;

    let metadata = replication_buffer::replicating_metadata_file(replicating.clone());

    // Try to read the metadata JSON, which tells us where to
    // replicate the chunks and meta files.  If we can't do
    // that, leave this precious data where it is...
    //
    // In particular, if the metadata file is missing, whoever removed
    // it should have made sure the chunks and meta were already
    // copied, and the corresponding directories thus empty and deleted.
    //
    // If that's not the case, fail by staying stuck: we don't provide
    // any liveness guarantee on replication, so that's not incorrect.
    // Even when replication is stuck, the buffering system bounds the
    // amount of replication data we keep around.
    let targets: ReplicationTargetList = serde_json::from_slice(&std::fs::read(&metadata)?)?;

    consume_directory(
        replication_buffer::replicating_chunks(replicating.clone()),
        |name, file| send_chunk(name, file, &targets),
    )?;
    consume_directory(
        replication_buffer::replicating_meta(replicating),
        |name, file| send_meta(name, file, &targets),
    )?;

    // Try to get rid of the metadata file.  If this fails, there's
    // nothing to do: the replicating directory is now empty, or
    // it's wedged in a bad state.
    let _ = std::fs::remove_file(&metadata);

    Ok(())
}

/// Process directories that should be ready for replication, one at a
/// time.
///
/// When the write ends of the channel are all gone, stop pulling work.
fn handle_requests(receiver: crossbeam_channel::Receiver<PathBuf>) {
    // This only fails when the channel is closed.
    while let Ok(path) = receiver.recv() {
        // Failures are expected when concurrent processes or copiers
        // work on the same `path`.  Even when `handle_directory`
        // fails, we're either making progress, or `path` is in a bad
        // state and we choose to keep it untouched rather than drop
        // data that we have failed to copy to the replication targets.
        let _ = handle_directory(path.clone());
        replication_buffer::acquire_ready_directory(path.clone());
        let _ = handle_directory(path);
    }
}
