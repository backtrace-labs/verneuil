/// An OFD (open file descriptor) lock associates an exclusive lock
/// with a `File` object (and its clones).  That lock is
/// system-global, and identified by the file (inode) at a given path.
use std::fs::File;
use std::path::Path;
use tracing::instrument;

use crate::chain_error;
use crate::error_from_os;
use crate::result::Result;

#[derive(Debug)]
pub(crate) struct OfdLock {
    file: File,
}

impl Drop for OfdLock {
    fn drop(&mut self) {
        use std::os::unix::io::AsRawFd;

        extern "C" {
            fn verneuil__ofd_lock_release(fd: i32) -> i32;
        }

        if unsafe { verneuil__ofd_lock_release(self.file.as_raw_fd()) } < 0 {
            let _ = error_from_os!("failed to release lock", ?self.file);
        }
    }
}

impl OfdLock {
    /// Attempts to acquire an exclusive lock on the file at
    /// `lock_path`.
    ///
    /// Returns Err on IO error, Ok(None) if the lock acquisition
    /// failed, and Ok(Some) on success.
    #[instrument(level = "debug")]
    pub fn try_lock(lock_path: &Path) -> Result<Option<OfdLock>> {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;

        extern "C" {
            fn verneuil__ofd_lock_exclusive(fd: i32) -> i32;
        }

        let file = std::fs::OpenOptions::new()
            .mode(0o666)
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .map_err(|e| chain_error!(e, "failed to open ofd lock file", ?lock_path))?;

        match unsafe { verneuil__ofd_lock_exclusive(file.as_raw_fd()) } {
            0 => Ok(Some(OfdLock { file })),
            1 => {
                tracing::info!(?lock_path, "ofd lock unavailable");
                Ok(None)
            }
            _ => Err(error_from_os!("failed to acquire ofd lock", ?lock_path)),
        }
    }
}
