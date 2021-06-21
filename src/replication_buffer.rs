//! The replication buffer subsystem manages replication data and
//! ensures that consistent snapshots are regularly propagated to
//! remote object storage.
//!
//! Each buffer is a directory that includes a "staging" directory.
//!
//! That staging directory has a "chunks" subdirectory, and a "meta"
//! subdirectory.  The chunks directory contains content-addressed
//! file chunks, and the "meta" directory named (url-encoded) metadata.
//!
//! Finally, there is also a "scratch" directory, which holds
//! named temporary files.
use crate::instance_id;

use std::fs::File;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use umash::Fingerprint;

#[derive(Debug)]
pub(crate) struct ReplicationBuffer {
    buffer_directory: PathBuf,
}

lazy_static::lazy_static! {
    static ref DEFAULT_STAGING_DIRECTORY: RwLock<Option<PathBuf>> = Default::default();
}

const STAGING: &str = "staging";
const CHUNKS: &str = "chunks";
const META: &str = "meta";
const SCRATCH: &str = "scratch";
const SUBDIRS: [&str; 3] = [CHUNKS, META, SCRATCH];

/// If this flag is set to true, stale replication directories that
/// look like they refer to overwritten databases will be deleted.
///
/// This is potentially lossy, since our path-mangling scheme isn't
/// invertible, so only enabled for sqlite tests.
pub(crate) static ENABLE_AUTO_CLEANUP: AtomicBool = AtomicBool::new(false);

/// Sets the default staging directory for replication subdirectories,
/// if it isn't already set.
pub(crate) fn set_default_staging_directory(path: &Path) -> Result<()> {
    let mut default = DEFAULT_STAGING_DIRECTORY.write().unwrap();

    if let Some(old_path) = &*default {
        if old_path == path {
            return Ok(());
        }

        return Err(Error::new(
            ErrorKind::InvalidInput,
            "default staging directory already set",
        ));
    }

    // We perform this I/O with a lock held, but we expect
    // `set_default_staging_directory` to be called early enough that
    // there is no contention.
    std::fs::create_dir_all(path)?;
    *default = Some(path.into());
    Ok(())
}

/// Removes directory separators from the input, and replaces them
/// with `#`. Either the input is not expected to ever include
/// slashes, or the result is allowed to collide for different inputs.
fn replace_slashes(input: &str) -> String {
    input.replace("/", "#")
}

/// Mangles a path to an extent database into a directory name:
/// forward slashes are turned into `#`.  By itself, this encoding is
/// ambiguous, but this mangled path will be combined with device and
/// inode ids.
fn mangle_path(path: &Path) -> Result<String> {
    let canonical = std::fs::canonicalize(path)?;
    let string = canonical.as_os_str().to_str().ok_or_else(|| {
        Error::new(
            ErrorKind::Other,
            "unable to convert canonical path to string",
        )
    })?;

    Ok(replace_slashes(string))
}

/// Constructs a unique human-readable filename for `fd` at `db_path`:
/// we derive a unique key with the file' device and inode ids (sqlite
/// does not support DB files with multiple links), and prefix that
/// with something that looks like the db file's canonical path for
/// debuggability.
fn db_file_key(db_path: &Path, fd: &File) -> Result<String> {
    use std::os::unix::fs::MetadataExt;

    let prefix = mangle_path(db_path)?;
    let meta = fd.metadata()?;

    Ok(format!("{}@{}.{}", prefix, meta.dev(), meta.ino()))
}

/// Attempts to delete all directories in the parent of `goal_path`
/// that start with `prefix`, except `goal_path`.
fn delete_stale_directories(goal_path: &Path, prefix: &str) -> Result<()> {
    let mut parent = goal_path.to_owned();
    if !parent.pop() {
        return Ok(());
    }

    let mut to_remove = vec![];

    for subdir_or in std::fs::read_dir(parent)? {
        let subdir = subdir_or?;
        if subdir.path() == goal_path {
            continue;
        }

        // Traverse the directory, and delete subdirectories in a
        // separate pass: some filesystems don't like it when you
        // traverse and mutate the directory at the same time.
        if subdir.file_name().to_string_lossy().starts_with(prefix) {
            to_remove.push(subdir);
        }
    }

    for subdir in to_remove {
        let _ = std::fs::remove_dir_all(subdir.path());
    }

    Ok(())
}

/// Creates a temporary file, populates it with `worker`, and
/// publishes it to `target` on success.
fn call_with_temp_file<T>(target: &Path, worker: impl Fn(&mut File) -> Result<T>) -> Result<T> {
    use std::ffi::CString;
    use std::os::raw::c_char;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::FromRawFd;

    // See c/file_ops.h
    extern "C" {
        fn verneuil__open_temp_file(directory: *const c_char, mode: i32) -> i32;
        fn verneuil__link_temp_file(fd: i32, target: *const c_char) -> i32;
    }

    let parent = target
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::Other, "no parent directory"))?;
    let parent_str = CString::new(parent.as_os_str().as_bytes())?;
    let target_str = CString::new(target.as_os_str().as_bytes())?;

    let fd = unsafe { verneuil__open_temp_file(parent_str.as_ptr(), 0o444) };
    if fd < 0 {
        return Err(Error::last_os_error());
    }

    let mut file = unsafe { File::from_raw_fd(fd) };
    let result = worker(&mut file)?;

    if unsafe { verneuil__link_temp_file(fd, target_str.as_ptr()) } < 0 {
        return Err(Error::last_os_error());
    }

    Ok(result)
}

impl ReplicationBuffer {
    /// Attempts to create a replication buffer for a file `fd` at
    /// `db_path`.
    ///
    /// That buffer is a directory,
    /// `$STAGING_DIRECTORY/$INSTANCE_ID/$MANGLED_DB_PATH@$DEVICE.$INODE`.
    /// The instance id subdirectory means we don't have to worry about
    /// seeing partial data left behind by prior OS crashes, and also
    /// makes it easy to clean up old directories.
    ///
    /// Returns `None` if the staging directory is not set.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the replication buffer directory could not be
    /// created.
    pub fn new(db_path: &Path, fd: &File) -> Result<Option<ReplicationBuffer>> {
        if let Some(mut staging) = DEFAULT_STAGING_DIRECTORY.read().unwrap().clone() {
            // Add an instance id.
            staging.push(format!(
                "verneuil-{}",
                replace_slashes(instance_id::instance_id())
            ));
            // And now add the unique local key for the db file.
            staging.push(db_file_key(db_path, fd)?);

            // Attempt to delete directories that refer to the same
            // path, but different inode.  This will do weird things
            // when two different paths look the same once slashes
            // are replaced with '#', so this logic is only enabled
            // for sqlite tests, which create a lot of dbs.
            if ENABLE_AUTO_CLEANUP.load(Ordering::Relaxed) {
                let _ = delete_stale_directories(&staging, &format!("{}@", mangle_path(db_path)?));
            }

            std::fs::create_dir_all(&staging)?;
            Ok(Some(ReplicationBuffer {
                buffer_directory: staging,
            }))
        } else {
            Ok(None)
        }
    }

    /// Attempts to create a staging directory for the replication
    /// buffer.
    ///
    /// Fails silently: downstream code has to handle all sorts of
    /// concurrent failures anyway.
    pub fn ensure_staging_dir(&self) {
        let mut buf = self.buffer_directory.clone();
        buf.push(STAGING);

        let _ = std::fs::create_dir_all(&buf);
        for subdir in &SUBDIRS {
            buf.push(subdir);
            let _ = std::fs::create_dir_all(&buf);
            buf.pop();
        }
    }

    /// Attempts to publish a chunk of data for `fprint`.  This file
    /// might already exist, in which case we don't have to do anything.
    pub fn stage_chunk(&self, fprint: Fingerprint, data: &[u8]) -> Result<()> {
        use std::io::Write;

        let chunk_name = format!("{:016x}.{:016x}", fprint.hash[0], fprint.hash[1]);

        let mut target = self.buffer_directory.clone();
        target.push(STAGING);
        target.push(CHUNKS);
        target.push(&chunk_name);

        if target.exists() {
            return Ok(());
        }

        call_with_temp_file(&target, |dst| dst.write_all(data))
    }
}
