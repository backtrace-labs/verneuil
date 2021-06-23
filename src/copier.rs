//! The copier module implements a thread that is responsible for
//! asynchronously acquiring the current "ready" buffer in any number
//! of replication directories, and sending the ready snapshot to
//! object stores like S3.
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

fn handle_requests(receiver: crossbeam_channel::Receiver<PathBuf>) {
    while let Ok(_path) = receiver.recv() {}
}
