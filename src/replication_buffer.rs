//! The replication buffer subsystem manages replication data and
//! ensures that consistent snapshots are regularly propagated to
//! remote object storage.
use crate::instance_id;

use std::fs::File;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;
use std::sync::RwLock;

#[derive(Debug)]
pub(crate) struct ReplicationBuffer {
    buffer_directory: PathBuf,
}

lazy_static::lazy_static! {
    static ref DEFAULT_STAGING_DIRECTORY: RwLock<Option<PathBuf>> = Default::default();
}

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
            std::fs::create_dir_all(&staging)?;
            Ok(Some(ReplicationBuffer {
                buffer_directory: staging,
            }))
        } else {
            Ok(None)
        }
    }
}
