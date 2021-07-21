//! The replication buffer subsystem manages replication data and
//! ensures that consistent snapshots are regularly propagated to
//! remote object storage.
//!
//! The base spooling directory contains a subdirectory keyed on the
//! current boot id: this prefix will change if the machine crashes,
//! and we can deal with missing data (as opposed to corrupt data) so
//! we don't have to slow down our operations with fsyncs everywhere.
//!
//! Finally, that subdirectory will include "spooling buffer"
//! sub(sub-)directories (`$MANGLED_PATH/$DEV.$INODE/`) for each
//! sqlite DB that is replicated.  The boot id directory also includes
//! a ".tap" subdirectory, where we store "meta" (directory) files
//! once they have been copied to remote storage.
//!
//! Each spooling buffer is a directory that includes a "staging"
//! directory, and a "ready" directory.
//!
//! The staging directory has a "chunks" subdirectory, and a "meta"
//! subdirectory.  The chunks directory contains content-addressed
//! file chunks, and the "meta" directory named (url-encoded)
//! metadata. Finally, there is also a "staging/scratch" directory,
//! which holds named temporary files.
//!
//! The "ready" directory always contains a directory that's ready to
//! be consumed by the replication subsystem.
//!
//! The files are *not* fsynced before publishing them: the buffer
//! directory is tagged with an instance id, so any file we observe
//! must have been created after the last crash or reboot.
//!
//! Two classes of processes interact with a replication buffer
//! directory: `Tracker`s produce data (update the replication
//! snapshot), while `Copier`s consume it.  The producers and
//! consumers' usage of a replication buffers provides the following
//! guarantees:
//!
//! - Except for "staging/scratch", files in "ready" and in "staging"
//!   are write-once: they may be overwritten, but only by atomic
//!   rename(2)/link(2).  The files are also created with read-only
//!   permissions.
//!
//! - Data in a "ready" directory only goes away once it has been
//!   copied to remote storage.  A Tracker can thus only publish a new
//!   "ready" directory once the old one (if any) has been fully copied.
//!
//! - When a new "ready" directory exists, all chunks referenced by
//!   its "ready/meta" files have already been copied, or are in the
//!   "ready/chunks" subdirectory.  This is true when a Tracker
//!   constructs a fresh "ready" directory and publishes it
//!   atomically, and remains true because copiers always consume
//!   (upload and delete) "ready/chunks" before "ready/meta".
//!
//! - Copiers eventually empty or delete a "ready" directory when its
//!   "chunks" and "meta" subdirectories are empty or absent.
//!
//! - All chunks referenced by "staging/meta" files have already been
//!   copied, are in "ready/chunks", or are in "staging/chunks".
//!   Copiers maintain this invariant by only deleting a file from
//!   "ready/chunks" once it has been uploaded.  Trackers maintain
//!   this invariant by:
//!     1. writing chunks to "staging/chunks" before publishing to
//!        "staging/meta";
//!     2. deleting all files in "staging/chunks" whenever the current
//!        snapshot has been published to "ready";
//!     3. otherwise only deleting unused files in "staging/chunks".
//!
//! The conditions for consumers (`Copier`s) are easy to enforce,
//! since copiers only perform monotonic or idempotent actions (upload
//! a file then delete it, remove an empty directory).  Of course,
//! producers (`Tracker`s) will publish new data, so consumers must
//! still worry about ABA.
//!
//! For the "ready" directory, `Copier`s access a snapshot of the
//! current directory via a file descriptor for that current directory
//! structure.  The snapshot is mutable, but, once published, only
//! `Copier`s update a "ready" directory, and only by removing files
//! or subdirectory once they are useless.  This RCU-like protocol
//! ensures that `Copier`s can always make progress even when
//! `Tracker`s constantly observe new writes.
//!
//! For the "staging" directory, it makes sense to split the analysis
//! between the easy part (chunks) and and the hard part (meta files).
//!
//! The invariant on chunks only refer to individual chunk files: a
//! chunk file never has any dependency.  We also don't have to worry
//! about ABA on chunk files: two chunk files with the same name are
//! assumed to have the same contents (that's how deduplication
//! works).  A `Copier` can thus always upload a given chunk file and
//! then delete it: even if the path now refers to a different inode,.
//!
//! The invariant on "meta" files refers to chunk files, but not to
//! other "meta" files.  This lets `Copier`s use something like a
//! sequence lock scheme on each "meta" file.  A `Copier` computes a
//! unique identifier for (the inode of) each meta file, checks that
//! all relevant chunks have definitely been published
//! ("staging/chunks" is empty, *and then* "ready" is empty or
//! missing), and finally uploads all meta files that have not changed.
//!
//! It's important to check for chunks to upload first in "staging",
//! and then in "ready" because we can't check for both atomically,
//! and "ready" is sticky: when we observe an empty "staging/chunks"
//! subdirectory, that might be because relevant chunks are now
//! in "ready".  However, if we then observe an empty "ready", either
//! that did not happen, or all chunks that used to be there have
//! been uploaded.  Either way, the unchanged meta files' dependencies
//! should all have been copied to remote storage.
//!
//! The conditions for producers (`Tracker`s) are more complex, and
//! involve non-monotonic/idempotent state updates.  However, they can
//! already rely on locking: `Tracker`s only update "staging" or
//! "ready" directories for a given db file while holding a read lock
//! on that file (and the db file is only mutated with a write lock
//! held).  While multiple `Tracker`s may concurrently update the
//! same directories, they will always work off the same db file,
//! and will thus always progress towards the same goal.
//!
//! The sequence lock approach for "staging" lets `Copier`s make
//! progress when there is data to copy, and `Tracker`s are idle.
//! Combined with the RCU approach for the "ready" directory, we can
//! guarantee progress both when `Tracker`s are idle, and when they're
//! constantly churning.
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::File;
use std::fs::Permissions;
use std::io::Error;
use std::io::ErrorKind;
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::time::Duration;
use tempfile::TempDir;
use tracing::instrument;
use tracing::Level;
use umash::Fingerprint;

use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::directory_schema::fingerprint_v1_chunk_list;
use crate::directory_schema::Directory;
use crate::drop_result;
use crate::error_from_os;
use crate::filtered_io_error;
use crate::filtered_os_error;
use crate::fresh_error;
use crate::fresh_info;
use crate::instance_id;
use crate::process_id::process_id;
use crate::replication_target::ReplicationTargetList;
use crate::result::Result;

#[derive(Debug)]
pub(crate) struct ReplicationBuffer {
    spooling_directory: PathBuf,
}

lazy_static::lazy_static! {
    static ref DEFAULT_SPOOLING_DIRECTORY: RwLock<Option<PathBuf>> = Default::default();
}

/// The ".tap" subdirectory isn't associated with any specific
/// replicated file, and is populated with name-addressed files
/// we successfully copied to remote storage.
const DOT_TAP: &str = ".tap";

/// The "staging" subdirectory is updated in-place to match the db.
const STAGING: &str = "staging";

/// When the "staging" subdirectory matches the db, it can be copied
/// atomically to an empty or missing "ready" subdirectory: the copy
/// logic clears the directory once everything has been copied.
const READY: &str = "ready";

/// The ".metadata" file, at the toplevel of the buffer directory,
/// tells copier where to copy ready and staged data.
const DOT_METADATA: &str = ".metadata";

/// The ".meta_copy_lock" file, at the toplevel of the buffer
/// directory, lets copiers serialise their upload and consumption of
/// directory blobs.  This serialisation prevents snapshots from going
/// back in time when a copier is slow to upload an old directory blob.
const DOT_META_COPY_LOCK: &str = ".meta_copy_lock";

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

/// Only delete scratch files when they're older than this grace
/// period.
const SCRATCH_FILE_GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Sets the default spooling directory for replication subdirectories,
/// if it isn't already set.
#[instrument]
pub(crate) fn set_default_spooling_directory(
    path: &Path,
    permissions: std::fs::Permissions,
) -> Result<()> {
    let mut default = DEFAULT_SPOOLING_DIRECTORY.write().unwrap();

    if let Some(old_path) = &*default {
        if old_path == path {
            return Ok(());
        }

        return Err(fresh_info!(
            "default spooling directory already set",
            ?old_path,
            ?path
        ));
    }

    // We perform this I/O with a lock held, but we expect
    // `set_default_spooling_directory` to be called early enough that
    // there is no contention.
    if !path.exists() {
        std::fs::create_dir_all(path)
            .map_err(|e| chain_error!(e, "failed to create directory", ?path))?;
        std::fs::set_permissions(path, permissions.clone())
            .map_err(|e| chain_error!(e, "failed to update permissions", ?path, ?permissions))?;
    }

    *default = Some(path.into());
    Ok(())
}

/// Creates a new read-only named temporary file in `spool_dir`'s
/// scratch directory.
///
/// On success, returns the temporary file and the scratch directory's path.
#[instrument(level = "debug")]
fn create_scratch_file(spool_dir: PathBuf) -> Result<(tempfile::NamedTempFile, PathBuf)> {
    let mut scratch = spool_dir;
    scratch.push(STAGING);
    scratch.push(SCRATCH);

    let temp = tempfile::Builder::new()
        .prefix(&(process_id() + "."))
        .suffix(".tmp")
        .tempfile_in(&scratch)
        .map_err(|e| chain_error!(e, "failed to create temporary file", ?scratch))?;

    temp.as_file()
        .set_permissions(Permissions::from_mode(0o444))
        .map_err(|e| chain_error!(e, "failed to set file read-only", ?temp))?;
    Ok((temp, scratch))
}

/// Returns the .tap path for `source_db`'s directory proto file,
/// given the path prefix for spooling directories (or None to use
/// the global default).
#[instrument(level = "debug")]
pub(crate) fn tapped_meta_path_in_spool_prefix(
    spool_prefix: Option<PathBuf>,
    source_db: &Path,
) -> Result<PathBuf> {
    match spool_prefix.or_else(|| DEFAULT_SPOOLING_DIRECTORY.read().unwrap().clone()) {
        None => Err(fresh_error!(
            "no spool_prefix provided or defaulted",
            ?source_db
        )),
        Some(mut tap) => {
            tap.push(DOT_TAP);
            tap.push(percent_encode_path_uri(source_db)?);
            Ok(tap)
        }
    }
}

/// Returns the .tap path for `source_db`'s directory proto file, given the file's `spool_dir`.
#[instrument(level = "debug")]
pub(crate) fn construct_tapped_meta_path(spool_dir: &Path, source_db: &Path) -> Result<PathBuf> {
    // Go up two directories, to go from `.../$MANGLED_PATH/$DEV.$INO` to the prefix, `...`.
    let base = spool_dir
        .parent()
        .map(Path::parent)
        .flatten()
        .ok_or_else(|| fresh_error!("invalid spool directory", ?spool_dir))?;

    tapped_meta_path_in_spool_prefix(Some(base.to_path_buf()), source_db)
}

/// Relinks `file` on top of `name` in the `.tap` directory for `spool_dir`.
#[instrument(level = "debug")]
pub(crate) fn tap_meta_file(spool_dir: &Path, name: &std::ffi::OsStr, file: &File) -> Result<()> {
    use rand::Rng;
    use std::os::unix::io::AsRawFd;

    // See c/file_ops.h
    extern "C" {
        fn verneuil__link_temp_file(fd: i32, target: *const c_char) -> i32;
    }

    // `spool_dir` is for a specific replicated file, i.e.,
    // ".../$MANGLED_PATH/$DEV.$INO/".  The ".tap" directory lives
    // in ".../.tap/"
    let base = spool_dir
        .parent()
        .map(Path::parent)
        .flatten()
        .ok_or_else(|| fresh_error!("invalid spool directory", ?spool_dir))?;
    let mut tap = base.to_path_buf();

    tap.push(DOT_TAP);
    std::fs::create_dir_all(&tap).map_err(|e| {
        filtered_io_error!(e, ErrorKind::AlreadyExists => Level::DEBUG,
                                        "failed to create .tap dir", dir=?tap)
    })?;
    std::fs::set_permissions(&tap, PermissionsExt::from_mode(0o777))
        .map_err(|e| chain_error!(e, "failed to set .tap permissions", dir=?tap))?;

    tap.push(name);

    let mut scratch = spool_dir.to_path_buf();
    scratch.push(STAGING);
    scratch.push(SCRATCH);
    scratch.push(format!(
        "{}.tap_meta.{}.{}.tmp",
        process_id(),
        name.to_string_lossy(),
        rand::thread_rng().gen::<u64>()
    ));

    let scratch_str = CString::new(scratch.as_os_str().as_bytes())
        .map_err(|e| chain_error!(e, "unable to convert scratch path to C string", ?scratch))?;

    // We must create a new name for the file's inode before we can rename it.
    if unsafe { verneuil__link_temp_file(file.as_raw_fd(), scratch_str.as_ptr()) } < 0 {
        return Err(
            filtered_os_error!(ErrorKind::NotFound => Level::DEBUG, "failed to create scratch file", ?scratch),
        );
    }

    std::fs::rename(&scratch, &tap).map_err(|e| {
        // If we failed to rename `scratch` away, try to clean up
        // before erroring out.
        drop_result!(std::fs::remove_file(&scratch),
                     e => chain_warn!(e, "failed to remove scratch file", ?scratch));
        chain_warn!(e, "failed to tap update meta file")
    })
}

/// Attempts to acquire the local lock around uploading "meta"
/// directory blobs.
///
/// Returns a File object that owns the lock on success (dropping that
/// file will release the lock), None on acquisition failure.
#[instrument(level = "debug")]
pub(crate) fn acquire_meta_copy_lock(spool_dir: PathBuf) -> Result<Option<File>> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    extern "C" {
        fn verneuil__ofd_lock_exclusive(fd: i32) -> i32;
    }

    let mut lock_path = spool_dir;
    lock_path.push(DOT_META_COPY_LOCK);

    let file = std::fs::OpenOptions::new()
        .mode(0o666)
        .read(true)
        .write(true)
        .create(true)
        .open(&lock_path)
        .map_err(|e| chain_error!(e, "failed to open meta copy lock file", ?lock_path))?;
    match unsafe { verneuil__ofd_lock_exclusive(file.as_raw_fd()) } {
        0 => Ok(Some(file)),
        1 => {
            tracing::info!(?lock_path, "meta copy lock unavailable");
            Ok(None)
        }
        _ => Err(error_from_os!(
            "failed to acquire meta copy lock",
            ?lock_path
        )),
    }
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
    let canonical = std::fs::canonicalize(path)
        .map_err(|e| chain_error!(e, "failed to canonicalize path", ?path))?;
    let string = canonical
        .as_os_str()
        .to_str()
        .ok_or_else(|| fresh_error!("unable to convert canonical path to string", ?canonical))?;

    Ok(replace_slashes(string))
}

/// Constructs a unique human-readable filename for `fd`: we derive a
/// unique key with the file's device and inode ids.
fn db_file_key(fd: &File) -> Result<String> {
    use std::os::unix::fs::MetadataExt;

    let meta = fd
        .metadata()
        .map_err(|e| chain_error!(e, "failed to stat file", ?fd))?;
    Ok(format!("{}.{}", meta.dev(), meta.ino()))
}

/// Attempts to delete all directories in the parent of `goal_path`
/// except `goal_path`.
#[instrument]
fn delete_stale_directories(goal_path: &Path) -> Result<()> {
    let mut parent = goal_path.to_owned();
    if !parent.pop() {
        return Ok(());
    }

    let goal_filename = goal_path.file_name();

    for subdir in std::fs::read_dir(&parent)
        .map_err(|e| chain_info!(e, "failed to list parent directory", path=?goal_path, ?parent))?
        .flatten()
    {
        if Some(subdir.file_name().as_os_str()) != goal_filename {
            parent.push(subdir.file_name());
            if let Err(error) = std::fs::remove_dir_all(&parent) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::info!(?parent, %error, "failed to remove sibling");
                }
            }
            parent.pop();
        }
    }

    Ok(())
}

/// Creates a temporary file, populates it with `worker`, and
/// publishes it to `target` on success.
///
/// Returns Ok(()) if the target file exists.
fn call_with_temp_file(target: &Path, worker: impl Fn(&mut File) -> Result<()>) -> Result<()> {
    use std::os::unix::io::FromRawFd;

    // See c/file_ops.h
    extern "C" {
        fn verneuil__open_temp_file(directory: *const c_char, mode: i32) -> i32;
        fn verneuil__link_temp_file(fd: i32, target: *const c_char) -> i32;
    }

    let _span = tracing::info_span!("call_with_temp_file", ?target);
    if target.exists() {
        return Ok(());
    }

    let parent = target.parent().expect("parent directory must exist");
    let parent_str = CString::new(parent.as_os_str().as_bytes())
        .map_err(|e| chain_error!(e, "unable to convert parent path to C string", ?parent))?;
    let target_str = CString::new(target.as_os_str().as_bytes())
        .map_err(|e| chain_error!(e, "unable to convert target path to C string", ?target))?;

    let fd = unsafe { verneuil__open_temp_file(parent_str.as_ptr(), 0o444) };
    if fd < 0 {
        return Err(error_from_os!("failed to open temporary file", ?parent_str));
    }

    let mut file = unsafe { File::from_raw_fd(fd) };
    let result = worker(&mut file)?;

    if unsafe { verneuil__link_temp_file(fd, target_str.as_ptr()) } < 0 {
        let error = Error::last_os_error();
        if error.kind() == ErrorKind::AlreadyExists {
            return Ok(());
        }

        return Err(chain_error!(
            error,
            "failed to publish temporary file",
            ?target_str
        ));
    }

    Ok(result)
}

/// Returns a conservatively percent-encoded URI for `path`.
/// The URI is of the form `verneuil:${hostname}/${path}`;
/// there will never be any double slash in `path` (it's a
/// canonical path), so these URIs should not collide for
/// different hosts.
fn percent_encode_path_uri(path: &Path) -> Result<String> {
    // Per https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html,
    // safe characters are alphanumeric characters, and a few special characters:
    // - Forward slash (/)
    // - Exclamation point (!)
    // - Hyphen (-)
    // - Underscore (_)
    // - Period (.)
    // - Asterisk (*)
    // - Single quote (')
    // - Open parenthesis (()
    // - Close parenthesis ())
    //
    // However, we must also escape forward slashes, since they're
    // directory separators.
    const ESCAPED: percent_encoding::AsciiSet = percent_encoding::NON_ALPHANUMERIC
        .remove(b'!')
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'*')
        .remove(b'\'')
        .remove(b'(')
        .remove(b')');

    lazy_static::lazy_static! {
        static ref PARAMS: umash::Params = umash::Params::derive(0, "verneuil path params");
    }

    let string = path
        .as_os_str()
        .to_str()
        .ok_or_else(|| fresh_error!("failed to convert path to string", ?path))?;
    let path_hash = umash::full_str(&PARAMS, 0, 0, &string);

    let name = format!(
        "{}-verneuil:{}:{:04x}/{}",
        instance_id::hostname_hash(),
        instance_id::hostname(),
        path_hash % (1 << (4 * 4)),
        string
    );
    Ok(percent_encoding::utf8_percent_encode(&name, &ESCAPED).to_string())
}

/// Returns the chunk name for a given fingerprint.  This name is
/// unambiguous and valid as both a POSIX filename and a S3 blob name.
pub(crate) fn fingerprint_chunk_name(fprint: &Fingerprint) -> String {
    lazy_static::lazy_static! {
        // We separate the primary and secondary hashes with a
        // percent-encoded slash: we don't expect many collisions in
        // the primary hash, but some platforms seem to shard based on
        // everything left of the last slash.  Without that separator,
        // our data might end up hashing to the same shard, despite
        // the high amount of entropy spread everywhere.
        static ref SEPARATOR: String = percent_encoding::utf8_percent_encode("/", percent_encoding::NON_ALPHANUMERIC).to_string();
    }

    format!(
        "{:016x}{}{:016x}",
        fprint.hash[0], &*SEPARATOR, fprint.hash[1]
    )
}

/// Attempts to read a valid Directory message from `file_path`.
/// Returns Ok(None) if the file does not exist.
#[instrument(level = "trace")]
fn read_directory_at_path(file_path: &Path) -> Result<Option<Directory>> {
    use prost::Message;

    let contents = match std::fs::read(file_path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(chain_error!(e, "failed to read directory", ?file_path)),
    };
    let directory = Directory::decode(&*contents)
        .map_err(|e| chain_error!(e, "failed to parse proto directory", ?file_path))?;

    match &directory.v1 {
        Some(v1)
            // We must have a v1 entry, the chunk fingerprints must match, and there
            // must be an even number of u64 in the list (we need two per fingerprint).
            if Some(fingerprint_v1_chunk_list(&v1.chunks).into()) == v1.contents_fprint
                && (v1.chunks.len() % 2) == 0 =>
        {
            Ok(Some(directory))
        },
        Some(_) => Err(fresh_error!("invalid chunk list", ?directory)),
        None => Err(fresh_error!("v1 format not found", ?directory)),
    }
}

/// Returns the path to the staging directory under `parent`; it may
/// be updated concurrently.
pub(crate) fn mutable_staging_directory(parent: PathBuf) -> PathBuf {
    let mut staging = parent;
    staging.push(STAGING);
    staging
}

/// Returns the path to the ready directory under `parent`; it may
/// be updated concurrently.
pub(crate) fn mutable_ready_directory(parent: PathBuf) -> PathBuf {
    let mut ready = parent;
    ready.push(READY);
    ready
}

/// Attempts to open a snapshot of the "ready" subdirectory of
/// `parent`.
///
/// On success, returns a process-local path to that subdirectory, and
/// a file object that must be kept alive to ensure the path is valid.
///
/// Returns Ok(None) if the directory does not exist.
#[instrument(level = "trace")]
pub(crate) fn snapshot_ready_directory(parent: PathBuf) -> Result<Option<(PathBuf, File)>> {
    use std::os::unix::io::FromRawFd;

    // See c/file_ops.h
    extern "C" {
        fn verneuil__open_directory(path: *const c_char) -> i32;
    }

    let ready = mutable_ready_directory(parent);
    let ready_str = CString::new(ready.as_os_str().as_bytes())
        .map_err(|e| chain_error!(e, "failed to convert path to C string", ?ready))?;
    let fd = unsafe { verneuil__open_directory(ready_str.as_ptr()) };
    if fd < 0 {
        let error = std::io::Error::last_os_error();

        if error.kind() == ErrorKind::NotFound {
            return Ok(None);
        }

        return Err(chain_error!(
            error,
            "failed to open ready directory",
            ?ready
        ));
    }

    let file = unsafe { File::from_raw_fd(fd) };
    Ok(Some((format!("/proc/self/fd/{}/", fd).into(), file)))
}

/// Attempts to remove the "ready" subdirectory of `parent`, if it is
/// empty.
#[instrument(level = "trace")]
pub(crate) fn remove_ready_directory_if_empty(parent: PathBuf) -> Result<()> {
    let mut ready = parent;
    ready.push(READY);

    match std::fs::remove_dir(&ready) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(chain_error!(e, "failed to remove ready directory", ?ready)),
    }
}

/// Returns a path for the metadata file in this buffer directory.
pub(crate) fn buffer_metadata_file(buffer: PathBuf) -> PathBuf {
    let mut metadata = buffer;
    metadata.push(DOT_METADATA);
    metadata
}

/// Returns a path for the chunks subdirectory in this `parent` directory.
pub(crate) fn directory_chunks(parent: PathBuf) -> PathBuf {
    let mut chunks = parent;
    chunks.push(CHUNKS);
    chunks
}

/// Returns a path for the meta subdirectory in this `parent` directory.
pub(crate) fn directory_meta(parent: PathBuf) -> PathBuf {
    let mut meta = parent;
    meta.push(META);
    meta
}

impl ReplicationBuffer {
    /// Attempts to create a replication buffer for a file `fd` at
    /// `db_path`.
    ///
    /// That buffer is a directory,
    /// `$SPOOLING_DIRECTORY/$INSTANCE_ID/$MANGLED_DB_PATH@$DEVICE.$INODE`.
    /// The instance id subdirectory means we don't have to worry about
    /// seeing partial data left behind by prior OS crashes, and also
    /// makes it easy to clean up old directories.
    ///
    /// Returns `None` if the spooling directory is not set.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the replication buffer directory could not be
    /// created.
    #[instrument]
    pub fn new(db_path: &Path, fd: &File) -> Result<Option<ReplicationBuffer>> {
        if let Some(mut spooling) = DEFAULT_SPOOLING_DIRECTORY.read().unwrap().clone() {
            // Add an instance id.
            spooling.push(format!(
                "verneuil-{}",
                replace_slashes(instance_id::instance_id())
            ));
            // And now add the unique local key for the db file.
            spooling.push(mangle_path(db_path)?);
            spooling.push(db_file_key(fd)?);

            // Attempt to delete directories that refer to the same
            // path, but different inode.  This will do weird things
            // when two different paths look the same once slashes
            // are replaced with '#', so this logic is only enabled
            // for sqlite tests, which create a lot of dbs, none of
            // which have colliding names.
            if ENABLE_AUTO_CLEANUP.load(Ordering::Relaxed) {
                drop_result!(delete_stale_directories(&spooling),
                             e => chain_info!(e, "failed to delete stale directories"));
            }

            std::fs::create_dir_all(&spooling).map_err(|e| {
                chain_error!(
                    e,
                    "failed to create spooling directory",
                    ?db_path,
                    ?spooling
                )
            })?;
            std::fs::set_permissions(&spooling, PermissionsExt::from_mode(0o777)).map_err(
                |e| chain_error!(e, "failed to set spooling dir permissions", dir=?spooling),
            )?;
            Ok(Some(ReplicationBuffer {
                spooling_directory: spooling,
            }))
        } else {
            Ok(None)
        }
    }

    /// Attempts to create a staging directory for the replication
    /// buffer.  For the staging directory to be useful, the metadata
    /// file must exist.  Create it.
    ///
    /// Fails silently: downstream code has to handle all sorts of
    /// concurrent failures anyway.
    #[instrument]
    pub fn ensure_staging_dir(&self, targets: &ReplicationTargetList, overwrite_meta: bool) {
        let mut buf = self.spooling_directory.clone();
        buf.push(STAGING);

        drop_result!(
            std::fs::create_dir_all(&buf),
            e => filtered_io_error!(e, ErrorKind::AlreadyExists => Level::DEBUG,
                                    "failed to create staging dir", dir=?buf)
        );
        drop_result!(std::fs::set_permissions(&buf, PermissionsExt::from_mode(0o777)),
                     e => chain_error!(e, "failed to set staging dir permissions", dir=?buf));
        for subdir in &SUBDIRS {
            buf.push(subdir);
            drop_result!(
                std::fs::create_dir_all(&buf),
                e => filtered_io_error!(e, ErrorKind::AlreadyExists => Level::DEBUG,
                                        "failed to create staging subdir", subdir, dir=?buf)
            );
            drop_result!(std::fs::set_permissions(&buf, PermissionsExt::from_mode(0o777)),
                         e => chain_error!(e, "failed to set staging subdir permissions", dir=?buf));
            buf.pop();
        }

        buf.pop();
        // Make sure the metadata file exists.
        buf.push(DOT_METADATA);
        if overwrite_meta || !buf.exists() {
            use std::io::Write;

            let json_bytes = serde_json::to_vec(&targets).expect("failed to serialize metadata.");
            if let Ok((temp, _)) = create_scratch_file(self.spooling_directory.clone()) {
                let written = temp.as_file().write_all(&json_bytes).map_err(|e| {
                    chain_error!(
                        e,
                        "failed to populate temporary file",
                        len = json_bytes.len()
                    )
                });
                if let Ok(()) = written {
                    drop_result!(
                        temp.persist(&buf),
                        e => chain_error!(e, "failed to publish .metadata", dst=?buf)
                    );
                }
            }
        }
    }

    /// Attempts to publish a chunk of data for `fprint`.  This file
    /// might already exist, in which case we don't have to do anything.
    #[instrument(level = "trace")]
    pub fn stage_chunk(&self, fprint: Fingerprint, data: &[u8]) -> Result<()> {
        use std::io::Write;

        let chunk_name = fingerprint_chunk_name(&fprint);

        let mut target = self.spooling_directory.clone();
        target.push(STAGING);
        target.push(CHUNKS);
        target.push(&chunk_name);

        call_with_temp_file(&target, |dst| {
            dst.write_all(data)
                .map_err(|e| chain_error!(e, "failed to write chunk", ?target, len = data.len()))
        })
    }

    /// Attempts to overwrite the directory file for the replicated file.
    #[instrument]
    pub fn publish_directory(&self, db_path: &Path, directory: &Directory) -> Result<()> {
        use prost::Message;
        use std::io::Write;

        let mut encoded = Vec::<u8>::new();
        directory
            .encode(&mut encoded)
            .map_err(|e| chain_error!(e, "failed to serialize directory proto", ?directory))?;

        let (temp, mut target) = create_scratch_file(self.spooling_directory.clone())?;
        temp.as_file().write_all(&encoded).map_err(|e| {
            chain_error!(
                e,
                "failed to write to temporary file",
                ?temp,
                len = encoded.len()
            )
        })?;

        target.pop();
        target.push(META);
        target.push(
            &percent_encode_path_uri(db_path)
                .map_err(|e| chain_error!(e, "invalid directory name", ?db_path))?,
        );
        temp.persist(&target)
            .map_err(|e| chain_error!(e, "failed to publish directory", ?target))?;
        Ok(())
    }

    /// Returns whether the "ready" subdirectory definitely exists.
    #[allow(dead_code)]
    pub fn ready_directory_present(&self) -> bool {
        let mut probe = self.spooling_directory.clone();
        probe.push(READY);

        probe.exists()
    }

    /// Attempts to parse the current ready directory file.
    #[instrument(level = "trace")]
    pub fn read_ready_directory(&self, db_path: &Path) -> Result<Option<Directory>> {
        let mut src = self.spooling_directory.clone();
        src.push(READY);
        src.push(META);
        src.push(&percent_encode_path_uri(db_path)?);
        read_directory_at_path(&src)
    }

    /// Attempts to parse the current staged directory file.
    #[instrument(level = "trace")]
    pub fn read_staged_directory(&self, db_path: &Path) -> Result<Option<Directory>> {
        let mut src = self.spooling_directory.clone();
        src.push(STAGING);
        src.push(META);
        src.push(&percent_encode_path_uri(db_path)?);
        read_directory_at_path(&src)
    }

    /// Attempts to parse the current staged directory file.
    #[allow(dead_code)]
    #[instrument(level = "trace")]
    pub fn read_staged_chunk(&self, fprint: &Fingerprint) -> Result<Vec<u8>> {
        let mut src = self.spooling_directory.clone();
        src.push(STAGING);
        src.push(CHUNKS);
        src.push(&fingerprint_chunk_name(fprint));

        std::fs::read(&src)
            .map_err(|e| filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG, "failed to read staged chunk file", ?src))
    }

    /// Attempts to parse the current staged directory file.
    #[allow(dead_code)]
    #[instrument(level = "trace")]
    pub fn read_ready_chunk(&self, fprint: &Fingerprint) -> Result<Vec<u8>> {
        let mut src = self.spooling_directory.clone();
        src.push(READY);
        src.push(CHUNKS);
        src.push(&fingerprint_chunk_name(fprint));

        std::fs::read(&src)
            .map_err(|e| filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG, "failed to read ready chunk file", ?src))
    }

    /// Attempts to copy the current "staging" buffer to a temporary
    /// buffer directory.
    ///
    /// This function should fail eagerly: it's better to fail
    /// spuriously and retry than to publish a partial buffer.
    ///
    /// On success, returns the temporary directory.
    #[instrument]
    pub fn prepare_ready_buffer(&self, chunks: &[Fingerprint]) -> Result<TempDir> {
        use tempfile::Builder;

        let live: HashSet<String> = chunks.iter().map(fingerprint_chunk_name).collect();

        let mut staging = self.spooling_directory.clone();
        staging.push(STAGING);

        let mut target = self.spooling_directory.clone();
        target.push(STAGING);
        target.push(SCRATCH);

        let temp = Builder::new()
            .prefix(&(process_id() + "."))
            .suffix(".tmp")
            .tempdir_in(&target)
            .map_err(|e| chain_error!(e, "failed to create temporary directory", ?target))?;

        let mut temp_path = temp.path().to_owned();
        std::fs::set_permissions(&temp_path, PermissionsExt::from_mode(0o777)).map_err(
            |e| chain_error!(e, "failed to set temporary dir permissions", dir=?temp_path),
        )?;

        // hardlink relevant chunk files to `temp_path/chunks`.
        staging.push(CHUNKS);
        temp_path.push(CHUNKS);
        std::fs::create_dir(&temp_path).map_err(|e| {
            filtered_io_error!(
                e,
                ErrorKind::NotFound => Level::DEBUG,
                "failed to create temporary chunks directory",
                ?temp_path
            )
        })?;
        std::fs::set_permissions(&temp_path, PermissionsExt::from_mode(0o777)).map_err(
            |e| chain_error!(e, "failed to set temporary chunks dir permissions", dir=?temp_path),
        )?;

        for file in std::fs::read_dir(&staging)
            .map_err(|e| chain_error!(e, "failed to list staged chunks", ?staging))?
        {
            if let Some(name) = file
                .map_err(|e| chain_error!(e, "failed to read chunk direntry"))?
                .file_name()
                .to_str()
            {
                if live.contains(name) {
                    staging.push(name);
                    temp_path.push(name);

                    // Attempt to hard-link the chunk.  It's ok if
                    // this fails because the file does not exist
                    // anymore: we expect copiers to come in and
                    // opportunistically publish chunks.
                    //
                    // We could also get a `NotFound` error if some
                    // path component is missing in `staging` or in
                    // `temp_path`; if that happens, we will fail
                    // downstream, when linking the meta files.
                    match std::fs::hard_link(&staging, &temp_path) {
                        Ok(_) => {}
                        Err(error) if error.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(chain_error!(err, "failed to link ready chunk")),
                    }

                    temp_path.pop();
                    staging.pop();
                }
            }
        }

        temp_path.pop();
        staging.pop();

        // hardlink all meta files to `temp_path/meta`.
        staging.push(META);
        temp_path.push(META);
        std::fs::create_dir(&temp_path).map_err(|e| {
            filtered_io_error!(
                e,
                ErrorKind::NotFound => Level::DEBUG,
                "failed to create temporary meta directory",
                ?temp_path
            )
        })?;
        std::fs::set_permissions(&temp_path, PermissionsExt::from_mode(0o777)).map_err(
            |e| chain_error!(e, "failed to set temporary meta dir permissions", dir=?temp_path),
        )?;

        for file_or in std::fs::read_dir(&staging)
            .map_err(|e| chain_error!(e, "failed to list staged meta", ?staging))?
        {
            let file = file_or.map_err(|e| chain_error!(e, "failed to read meta direntry"))?;
            staging.push(file.file_name());
            temp_path.push(file.file_name());

            // This *can* be expected if another process finished the
            // snapshot and cleaned up the scratch directory.
            std::fs::hard_link(&staging, &temp_path).map_err(|e| {
                filtered_io_error!(
                    e,
                    ErrorKind::NotFound => Level::DEBUG,
                    "failed to link ready directory",
                    ?staging,
                    ?temp_path
                )
            })?;

            temp_path.pop();
            staging.pop();
        }

        Ok(temp)
    }

    /// Attempts to publish a temporary buffer directory to an empty
    /// or missing "ready" buffer.
    #[instrument]
    pub fn publish_ready_buffer(&self, ready: TempDir) -> Result<()> {
        let mut target = self.spooling_directory.clone();
        target.push(READY);

        std::fs::rename(ready.path(), &target).map_err(|e| {
            filtered_io_error!(
                e,
                ErrorKind::AlreadyExists | ErrorKind::NotFound => Level::DEBUG,
                "failed to publish ready buffer",
                ?target,
                ?ready
            )
        })
    }

    /// Returns the spooling directory path for this buffer.
    pub fn spooling_directory(&self) -> &Path {
        &self.spooling_directory
    }

    /// Attempts to clean up any chunk file that's not referred by the
    /// `chunks` list.
    #[instrument]
    pub fn gc_chunks(&self, chunks: &[Fingerprint]) -> Result<()> {
        let live: HashSet<String> = chunks.iter().map(fingerprint_chunk_name).collect();

        let mut chunks = self.spooling_directory.clone();
        chunks.push(STAGING);
        chunks.push(CHUNKS);

        for file in std::fs::read_dir(&chunks)
            .map_err(|e| chain_error!(e, "failed to list staged chunks", ?chunks))?
            .flatten()
        {
            if let Some(name) = file.file_name().to_str() {
                if live.contains(name) {
                    continue;
                }
            }

            // Attempt to delete the file.  We don't guarantee
            // anything except that we keep everyting in `chunks`,
            // so eat failures, which could happen, e.g., with
            // concurrent GCs.
            chunks.push(file.file_name());
            match std::fs::remove_file(&chunks) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::error!(%error, ?chunks, "failed to clean up stale chunk file")
                }
            }
            chunks.pop();
        }

        Ok(())
    }

    /// Attempts to delete all temporary files and directory from "staging/scratch."
    #[instrument]
    pub fn cleanup_scratch_directory(&self) -> Result<()> {
        fn file_is_stale(path: &Path) -> Result<bool> {
            let meta = std::fs::metadata(path)
                .map_err(|e| filtered_io_error!(e, ErrorKind::NotFound => Level::DEBUG, "failed to stat file", ?path))?;
            let modified = meta
                .modified()
                .map_err(|e| chain_error!(e, "failed to fetch mtime", ?path))?;
            let elapsed = modified
                .elapsed()
                .map_err(|e| chain_warn!(e, "time went backward", ?path))?;
            Ok(elapsed >= SCRATCH_FILE_GRACE_PERIOD)
        }

        fn remove_file_if_stale(path: &Path) {
            if matches!(file_is_stale(path), Ok(false)) {
                return;
            }

            match std::fs::remove_file(path) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::error!(%error, ?path, "failed to remove stale scratch file")
                }
            }
        }

        let mut scratch = self.spooling_directory.clone();
        scratch.push(STAGING);
        scratch.push(SCRATCH);

        for entry in std::fs::read_dir(&scratch)
            .map_err(|e| chain_error!(e, "failed to list scratch directory", ?scratch))?
            .flatten()
        {
            let is_dir = match entry.file_type() {
                Ok(file_type) => file_type.is_dir(),
                Err(error) => {
                    tracing::info!(%error, ?entry, "failed to read scratch dentry");
                    false
                }
            };

            if !is_dir {
                // The entry isn't a directory.  We can unlink it
                // atomically, and there's nothing to do on failure:
                // we simply want to remove as many now-useless files
                // as possible.
                scratch.push(entry.file_name());
                remove_file_if_stale(&scratch);
                scratch.pop();
                continue;
            }

            // Temporary directories always have a ".tmp" suffix.
            // Atomically change that to ".del" to make sure all
            // further operations on that directory (including
            // publishing it as a new ready buffer) fail.
            scratch.push(entry.file_name());
            if scratch.extension() != Some(std::ffi::OsStr::new(".del")) {
                let old_path = scratch.clone();

                scratch.set_extension(".del");
                match std::fs::rename(&old_path, &scratch) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(error) => {
                        tracing::error!(%error, ?old_path, ?scratch, "failed to rename scratch subdirectory")
                    }
                };
            }

            // We know `path` is marked for deletion with a ".del"
            // extension.  We can now delete it recursively.  Nothing
            // to do if that fails... it's probably a concurrent
            // deletion, and that we care about is that this
            // subdirectory eventually goes away, if possible.
            match std::fs::remove_dir_all(&scratch) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::error!(%error, ?scratch, "failed to remove scratch subdirectory")
                }
            };
            scratch.pop();
        }

        Ok(())
    }
}

#[test]
fn percent_encode_sample() {
    const SAMPLE_PATH: &str = "/a@<<sd!%-_/.asd/*'fdg(g/)\\~).db";
    assert_eq!(
        percent_encode_path_uri(Path::new(SAMPLE_PATH)).expect("should convert"),
        // We assume the test host's name doesn't need escaping.
        format!(
            "{}-verneuil%3A{}%3A634c%2F%2Fa%40%3C%3Csd!%25-_%2F.asd%2F*\'fdg(g%2F)%5C%7E).db",
            instance_id::hostname_hash(),
            instance_id::hostname()
        )
    );
}
