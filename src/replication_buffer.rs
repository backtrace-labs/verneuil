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
//! a ".tap" subdirectory, where we store "meta" (manifest) files
//! once they have been copied to remote storage.
//!
//! Each spooling buffer is a directory that includes a "staging"
//! directory, and a "ready" directory.  It may temporarily include a
//! "consuming" directory.
//!
//! The staging directory has a "chunks" subdirectory, and a "meta"
//! subdirectory.  The chunks directory contains content-addressed
//! file chunks, and the "meta" directory named (url-encoded)
//! metadata. Finally, there is also a "staging/scratch" directory,
//! which holds named temporary files.
//!
//! The "ready" directory always contains a manifest that's ready to
//! be consumed by the replication subsystem.
//!
//! When the replication subsystem is ready to consume a "ready"
//! directory, the directory is first renamed to "consuming."
//!
//! That directory has one member, a pseudo-uniquely named directory.
//! Inside that pseudo-unique directory are a "chunks" and a "meta"
//! subdirectories; every chunk file in "chunks" must be published
//! before the manifest in "meta" is safe to copy over.  The
//! pseudo-unique name lets readers safely consume from a path without
//! having to worry about ABA.
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
//! - A "ready" directory only goes away when it's atomically renamed
//!   over an empty or absent "consuming" directory.
//!
//! - Data in a "consuming" directory only goes away once it has been
//!   copied to remote storage.  A Tracker can thus only publish a new
//!   "ready" directory once the old one (if any) has been fully copied.
//!
//! - When a new "ready" directory exists, all chunks referenced by
//!   its "ready/meta" files have already been copied, or are in the
//!   "ready/chunks" subdirectory.  This is true when a Tracker
//!   constructs a fresh "ready" directory and publishes it
//!   atomically and when it's renamed to "consuming," and
//!   remains true because copiers always consume
//!   (upload and delete) "consuming/chunks" before "consuming/meta".
//!
//! - Copiers eventually empty or delete a "consuming" directory when its
//!   "chunks" and "meta" subdirectories are empty or absent.
//!
//! - All chunks referenced by "staging/meta" files have already been
//!   copied, are in "{ready,consuming}/chunks", or are in "staging/chunks".
//!   Copiers maintain this invariant by only deleting a file from
//!   "consuming/chunks" once it has been uploaded.  Trackers maintain
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
//! For the "ready" directory, `Copier`s first rename it to
//! "consuming," and access a snapshot of the current directory via a
//! file descriptor for that directory structure.  The snapshot is
//! mutable, but, once published, only `Copier`s update a "consuming"
//! directory, and only by removing files or subdirectory once they
//! are useless.  This RCU-like protocol ensures that `Copier`s can
//! always make progress even when `Tracker`s constantly observe new
//! writes.
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
//! ("staging/chunks" is empty, *and then* both "ready" and
//! "consuming" are empty or missing), and finally uploads all meta
//! files that have not changed.
//!
//! It's important to check for chunks to upload first in "staging",
//! then in "ready" and finally in "consuming" because we can't check
//! for all atomically, and "ready/consuming" are sticky: when we
//! observe an empty "staging/chunks" subdirectory, that might be
//! because relevant chunks are now in "ready" or "consuming."
//! However, if we then observe an empty "ready", either that did not
//! happen, or it was renamed to "consuming."  If we also later
//! observe that "consuming" is empty or missing, the rename did not
//! happen or all chunks that used to be there have been uploaded.
//! Either way, the unchanged meta files' dependencies should all have
//! been copied to remote storage.
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
use std::fs::File;
use std::fs::Permissions;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tempfile::TempDir;
use tracing::instrument;
use tracing::Level;
use umash::Fingerprint;

use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::drop_result;
use crate::filtered_io_error;
use crate::fresh_error;
use crate::fresh_info;
use crate::instance_id;
use crate::loader::Chunk;
use crate::manifest_schema::Manifest;
use crate::ofd_lock::OfdLock;
use crate::process_id::process_id;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::ReplicationTargetList;
use crate::result::Result;

#[derive(Debug)]
pub(crate) struct ReplicationBuffer {
    spooling_directory: PathBuf,
}

lazy_static::lazy_static! {
    static ref DEFAULT_SPOOLING_DIRECTORY: RwLock<Option<PathBuf>> = Default::default();
}

/// Account for clock drift by up to this many seconds when probe for
/// per-boot directories: the directories' names contain the boot
/// time, and that value is subject to time adjustment.
///
/// We have observed 1 second adjustments (maybe a much smaller change
/// rounded to second precision) on EC2.  2 seconds should hopefully
/// be enough, except when opening a db with Verneuil immediately
/// after booting up a machine that had been off for hours.
const INSTANCE_ID_PROBE_RANGE: u64 = 2;

/// We tag the per-boot directory with a version string: if we upgrade
/// or downgrade to a different directory tree format, Verneuil will
/// treat that as no or stale data (a well tested path), rather than
/// getting stuck on an unexpected directory structure.
const ON_DISK_FORMAT_VERSION_SUFFIX: &str = "v2";

/// The ".tap" subdirectory isn't associated with any specific
/// replicated file, and is populated with name-addressed files
/// we successfully copied to remote storage.
const DOT_TAP: &str = ".tap";

/// The "staging" subdirectory is updated in-place to match the db.
const STAGING: &str = "staging";

/// When the "staging" subdirectory matches the db, it can be copied
/// atomically to an empty or missing "ready" subdirectory: the copy
/// logic renames the directory to "consuming."
const READY: &str = "ready";

/// When copiers are ready to consume a new "ready" directory, they
/// rename it to "consuming."
const CONSUMING: &str = "consuming";

/// The ".metadata" file, at the toplevel of the buffer directory,
/// tells copier where to copy ready and staged data.
const DOT_METADATA: &str = ".metadata";

/// The ".meta_copy_lock" file, at the toplevel of the buffer
/// directory, lets copiers serialise their upload and consumption of
/// manifest blobs.  This serialisation prevents snapshots from going
/// back in time when a copier is slow to upload an old manifest blob.
const DOT_META_COPY_LOCK: &str = ".meta_copy_lock";

const CHUNKS: &str = "chunks";
const META: &str = "meta";
const SCRATCH: &str = "scratch";
const SUBDIRS: [&str; 3] = [CHUNKS, META, SCRATCH];

/// Only delete scratch files when they're older than this grace
/// period.
const SCRATCH_FILE_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// Only delete orphan `.tap` manifest files when they're older than
/// this grace period.
const ORPHAN_TAP_FILE_GRACE_PERIOD: Duration = Duration::from_secs(24 * 3600);

/// Contemporary file systems (unixen and windows) all let files and
/// directories have up to 255 bytes or codepoints in their name.
///
/// Let invertibly mangled names go up to 230 *bytes*, just to avoid
/// issues with boundary value.  In practice, invertible names are
/// always strictly shorter than `MAX_MANGLED_NAME_LENGTH`, and
/// uninvertible ones strictly longer.
const MAX_MANGLED_NAME_LENGTH: usize = 230;

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
#[instrument(level = "debug", err)]
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

/// Returns a string that is safe to use as a filename and is
/// extremely unlikely to be repeated.
fn pseudo_unique_filename() -> String {
    use rand::Rng;
    use std::sync::atomic::AtomicU64;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let pid = process_id();
    let timestamp = match std::time::SystemTime::UNIX_EPOCH.elapsed() {
        Ok(duration) => duration.as_micros(),
        Err(_) => 0,
    };
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let random = rand::thread_rng().gen::<u128>();

    format!("{}.{}.{}.{:x}", pid, timestamp, seq, random)
}

/// Returns `base` extended with an arbitrary subdirectory, if these
/// is at least one.  Otherwise, returns an inexistent child of `base`.
fn append_subdirectory(mut base: PathBuf) -> PathBuf {
    fn find_child(base: &Path) -> std::io::Result<Option<std::ffi::OsString>> {
        if let Some(dirent) = std::fs::read_dir(base)?.flatten().next() {
            Ok(Some(dirent.file_name()))
        } else {
            Ok(None)
        }
    }

    match find_child(&base) {
        Ok(Some(child)) => base.push(child),
        _ => base.push(pseudo_unique_filename()),
    }

    base
}

/// Returns the .tap path given the path prefix for spooling
/// directories (or None to use the global default), if we can find
/// it.
#[instrument(level = "debug")]
pub(crate) fn tap_path_in_spool_prefix(spool_prefix: Option<PathBuf>) -> Option<PathBuf> {
    let mut prefix = spool_prefix.or_else(|| DEFAULT_SPOOLING_DIRECTORY.read().unwrap().clone())?;

    prefix.push(DOT_TAP);
    Some(prefix)
}

/// Returns the .tap path for `source_db`'s manifest proto file, given
/// the path prefix for spooling directories (or None to use the
/// global default).
#[instrument(level = "debug", err)]
pub(crate) fn tapped_manifest_path_in_spool_prefix(
    spool_prefix: Option<PathBuf>,
    source_db: &Path,
) -> Result<PathBuf> {
    let mut tap = match tap_path_in_spool_prefix(spool_prefix) {
        Some(path) => path,
        None => {
            return Err(fresh_error!(
                "no spool_prefix provided or defaulted",
                ?source_db
            ))
        }
    };

    tap.push(percent_encode_local_path_uri(source_db)?);
    Ok(tap)
}

/// Returns the .tap path for `source_db`'s manifest proto file, given
/// the file's `spool_dir`.
#[instrument(level = "debug", err)]
pub(crate) fn construct_tapped_manifest_path(
    spool_dir: &Path,
    source_db: &Path,
) -> Result<PathBuf> {
    // Go up two directories, to go from `.../$MANGLED_PATH/$DEV.$INO` to the prefix, `...`.
    let base = spool_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| fresh_error!("invalid spool directory", ?spool_dir))?;

    tapped_manifest_path_in_spool_prefix(Some(base.to_path_buf()), source_db)
}

/// Relinks `file` on top of `name` in the `.tap` directory for `spool_dir`.
#[instrument(level = "debug", err)]
pub(crate) fn tap_manifest_file(
    spool_dir: &Path,
    name: &std::ffi::OsStr,
    file: &mut File,
) -> Result<()> {
    use std::io::Seek;

    // `spool_dir` is for a specific replicated file, i.e.,
    // ".../$MANGLED_PATH/$DEV.$INO/".  The ".tap" directory lives
    // in ".../.tap/"
    let base = spool_dir
        .parent()
        .and_then(Path::parent)
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

    file.seek(std::io::SeekFrom::Start(0))
        .map_err(|e| chain_error!(e, "failed to rewind manifest file", ?file, ?tap))?;
    let (mut temp, _) = create_scratch_file(spool_dir.to_path_buf())?;
    std::io::copy(file, &mut temp)
        .map_err(|e| chain_error!(e, "failed to copy .tap file", ?tap))?;

    temp.persist(&tap)
        .map_err(|e| chain_warn!(e, "failed to tap updated manifest file"))?;
    Ok(())
}

/// Attempts to acquire the local lock around uploading "meta"
/// manifest blobs.
///
/// Returns a File object that owns the lock on success (dropping that
/// file will release the lock), None on acquisition failure.
#[instrument(level = "debug", err)]
pub(crate) fn acquire_meta_copy_lock(spool_dir: PathBuf) -> Result<Option<OfdLock>> {
    let mut lock_path = spool_dir;
    lock_path.push(DOT_META_COPY_LOCK);

    OfdLock::try_lock(&lock_path)
        .map_err(|e| chain_error!(e, "failed to acquire meta copy lock", ?lock_path))
}

/// Clears the meta copy lock's state for `spool_dir`.
#[instrument(level = "debug", err)]
pub(crate) fn reset_meta_copy_lock(spool_dir: PathBuf) -> Result<()> {
    let mut lock_path = spool_dir;
    lock_path.push(DOT_META_COPY_LOCK);

    std::fs::remove_file(&lock_path).map_err(|e| {
        filtered_io_error!(e,
                           ErrorKind::NotFound => Level::DEBUG,
                           "failed to remove lock file", ?lock_path)
    })
}

/// Percent-encodes control characters and `#` away, and replaces
/// slashes with `#`.  The result is invertible, and is the same as
/// just replacing `/` with `#` for most paths.
fn replace_slashes(input: &str) -> String {
    const ESCAPED: percent_encoding::AsciiSet = percent_encoding::CONTROLS.add(b'#').add(b'%');

    percent_encoding::utf8_percent_encode(input, &ESCAPED)
        .map(|fragment| fragment.replace('/', "#"))
        .collect()
}

/// Reverses `replace_slashes` if possible.
pub(crate) fn restore_slashes(input: &str) -> Result<Option<String>> {
    let slashified = input.replace('#', "/");

    // If the input string is more than `MAX_MANGLED_NAME_LENGTH`
    // bytes, assume it had to be truncated.
    if input.as_bytes().len() > MAX_MANGLED_NAME_LENGTH {
        Ok(None)
    } else {
        Ok(Some(
            percent_encoding::percent_decode_str(&slashified)
                .decode_utf8()
                .map_err(|e| chain_info!(e, "invalid utf-8 bytes"))?
                .to_string(),
        ))
    }
}

/// If `input` is at least as long as `MAX_MANGLED_NAME_LENGTH`,
/// shorten it to slightly more than `MAX_MANGLED_NAME_LENGTH`
/// in order to guarantee the name fits in `NAME_MAX`.
fn pseudo_uniquely_fit_string_in_filename(input: String) -> String {
    // Avoid fencepost bugs and never generate a mangled string
    // that's exactly MAX_MANGLED_NAME_LENGTH long.
    if input.as_bytes().len() < MAX_MANGLED_NAME_LENGTH {
        return input;
    }

    lazy_static::lazy_static! {
        static ref MANGLE_PARAMS: umash::Params = umash::Params::derive(0, b"verneuil path mangling params");
    }

    // This fingerprint will be printed as 2x 16 hex char strings.
    let fprint = MANGLE_PARAMS
        .fingerprinter(0)
        .write(input.as_bytes())
        .digest();

    // MAX_MANGLED_NAME_LENGTH is a bit short of the expected
    // limit on filenames; this lets us add a few extra bytes to
    // make sure the mangled path exceeds MAX_MANGLED_NAME_LENGTH,
    // even after round tripping to unicode codepoint and back to
    // encoded bytes.
    let fragment_len = 5 + (MAX_MANGLED_NAME_LENGTH - 32) / 2;
    // The conversion back from truncated bytes to codepoint to utf-8
    // bytes might add a couple bytes on each end, but
    // MAX_MANGLED_NAME_LENGTH is comfortable less than 255, so the
    // final mangled name is both definitely longer than
    // MAX_MANGLED_NAME_LENGTH and shorter than 255.
    let left = String::from_utf8_lossy(&input.as_bytes()[..fragment_len]);
    let right = String::from_utf8_lossy(&input.as_bytes()[input.len() - fragment_len..]);
    format!(
        "{}{:016x}{:016x}{}",
        left, fprint.hash[0], fprint.hash[1], right
    )
}

fn mangle_string(string: &str) -> String {
    pseudo_uniquely_fit_string_in_filename(replace_slashes(string))
}

/// Mangles a path to an extent database into a directory name:
/// control characters and `#` (and `%`) are percent-encoded away, and
/// forward slashes are turned into `#`.
fn mangle_path(path: &Path) -> Result<String> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|e| chain_error!(e, "failed to canonicalize path", ?path))?;
    let string = canonical
        .as_os_str()
        .to_str()
        .ok_or_else(|| fresh_error!("unable to convert canonical path to string", ?canonical))?;
    Ok(mangle_string(string))
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

/// Determines whether the file at `path` has been untouched for
/// `max_age` or longer.
fn file_is_stale(path: &Path, max_age: Duration) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let meta = std::fs::metadata(path).map_err(|e| {
        filtered_io_error!(e,
                           ErrorKind::NotFound => Level::DEBUG,
                           "failed to stat file", ?path)
    })?;
    let changed = Duration::new(meta.ctime() as u64, meta.ctime_nsec() as u32);

    let ctime = std::time::UNIX_EPOCH
        .checked_add(changed)
        .unwrap_or_else(std::time::SystemTime::now);
    let elapsed = ctime
        .elapsed()
        .map_err(|e| chain_warn!(e, "time went backward", ?path))?;
    Ok(elapsed >= max_age)
}

/// Attempts to atomically rename a path and then removes it.
fn remove_dir(path: &Path) -> std::io::Result<()> {
    // Avoid false positive in the validation logic when we delete
    // spooled data because sqlite deleted a test db.
    #[cfg(feature = "test_validate_reads")]
    return Ok(());

    let mut new_path = path.to_path_buf();
    new_path.set_extension(".del");

    match std::fs::rename(path, &new_path) {
        Ok(()) => std::fs::remove_dir_all(&new_path),
        Err(_) => std::fs::remove_dir_all(path),
    }
}

/// Attempts to delete all directories in the parent of `goal_path`
/// except `goal_path`.
#[instrument(err)]
fn delete_stale_directories(goal_path: &Path) -> Result<()> {
    let mut parent = goal_path.to_owned();
    if !parent.pop() {
        return Ok(());
    }

    let goal_filename = goal_path.file_name();
    let it = match std::fs::read_dir(&parent) {
        Ok(it) => it,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(chain_info!(e, "failed to list parent directory", path=?goal_path, ?parent))
        }
    };

    for subdir in it.flatten() {
        if Some(subdir.file_name().as_os_str()) != goal_filename {
            parent.push(subdir.file_name());
            if let Err(error) = remove_dir(&parent) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::info!(?parent, %error, "failed to remove sibling");
                }
            }
            parent.pop();
        }
    }

    Ok(())
}

/// Attempts to delete all directories in `parent` that refer to a file
/// that does not exist anymore.
#[instrument(err)]
fn delete_dangling_replication_directories(mut parent: PathBuf) -> Result<()> {
    let it = match std::fs::read_dir(&parent) {
        Ok(it) => it,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(chain_info!(e, "failed to list parent directory", ?parent)),
    };

    for subdir in it.flatten() {
        // We're only interested in percent-encoded paths, and those
        // are always(?) valid utf-8.
        let name = match subdir.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        // Skip dot files/directories.
        if name.starts_with('.') {
            continue;
        }

        // Skip non-directories.
        if !matches!(subdir.file_type(), Ok(typ) if typ.is_dir()) {
            continue;
        }

        let base_file = match restore_slashes(&name) {
            Ok(Some(base_file)) => base_file,
            // If we couldn't decode the file name, it's probably
            // not ours?
            Ok(None) | Err(_) => continue,
        };

        // If the file definitely does not exist, delete the
        // replication directory.
        if matches!(std::fs::symlink_metadata(&base_file),
                    Err(e) if e.kind() == ErrorKind::NotFound)
        {
            parent.push(subdir.file_name());
            tracing::info!(%base_file, victim=?parent,
                           "deleting dangling replication directory");
            drop_result!(remove_dir(&parent),
                         e => chain_info!(e, "failed to delete replication directory for missing db",
                                          %base_file, victim=?parent));
            parent.pop();
        }
    }

    Ok(())
}

/// Scans all manifest files in `spooling`'s `.tap` subdirectory.  If
/// they're old and refer to nonexistent db files on the current
/// hostname, delete them.
fn delete_orphan_tapped_manifest_files(spooling: PathBuf) -> Result<()> {
    let mut tap = spooling;
    tap.push(DOT_TAP);

    let it = match std::fs::read_dir(&tap) {
        Ok(it) => it,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(chain_info!(e, "failed to list .tap directory", ?tap)),
    };

    // This string looks like
    // `${hostname_hash}-verneuil:${hostname}:${path_hash}/${path}`,
    // except percent-encoded.
    let local_name = manifest_name_for_hostname_path(None, Path::new(""))
        .map_err(|e| chain_error!(e, "failed to generate local manifest name"))?;
    let decoded_name = percent_encoding::percent_decode_str(&local_name)
        .decode_utf8()
        .map_err(|e| chain_info!(e, "invalid utf-8 bytes in local manifest name"))?
        .to_string();

    // Find the hostname-specific prefix (up to and including the last colon).
    let local_prefix = match decoded_name.rsplit_once(':') {
        Some((prefix, _)) => format!("{}:", prefix),
        None => {
            return Err(fresh_error!(
            "unexpected pattern in local manifest name",
            %local_name))
        }
    };

    fn handle_tap_file(path: &Path, decoded_name: &str) -> Result<()> {
        if !file_is_stale(path, ORPHAN_TAP_FILE_GRACE_PERIOD)? {
            return Ok(());
        }

        // Extract the `{path_hash}/{path}` suffix, and, from that,
        // the final `path` component.
        let suffix = match decoded_name
            .rsplit_once(':')
            .and_then(|(_, hash_path)| hash_path.split_once('/'))
        {
            Some((_hash, path)) => path,
            None => return Ok(()),
        };

        // Only delete if we can positively tell that the target db
        // file does not exist.
        if !matches!(std::fs::metadata(Path::new(suffix)), Err(e) if e.kind() == ErrorKind::NotFound)
        {
            return Ok(());
        }

        match std::fs::remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(chain_error!(e, "failed to delete orphan tap file", ?path)),
        }
    }

    for child in it.flatten() {
        // We're only interested in percent-encoded paths, and those
        // are always(?) valid utf-8.
        let name = match child.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };

        let decoded = match percent_encoding::percent_decode_str(&name).decode_utf8() {
            Ok(decoded) => decoded.to_string(),
            // if we can't percent-decode the string, it's not ours.
            Err(_) => continue,
        };

        // Only look at files that match the current machine's prefix.
        if !decoded.starts_with(&local_prefix) {
            continue;
        }

        // Skip non-files.
        if !matches!(child.file_type(), Ok(typ) if typ.is_file()) {
            continue;
        }

        tap.push(child.file_name());
        drop_result!(handle_tap_file(&tap, &decoded),
                     e => chain_info!(e, "failed to maybe delete tapped manifest file", path=?tap));
        tap.pop();
    }

    Ok(())
}

/// Creates a temporary file, populates it with `worker`, and
/// publishes it to `target` on success.
///
/// Returns Ok(()) if the target file exists.
#[instrument(skip(worker), err)]
fn call_with_temp_file(
    spool_dir: PathBuf,
    target: &Path,
    worker: impl Fn(&mut File) -> Result<()>,
) -> Result<()> {
    if target.exists() {
        return Ok(());
    }

    let (mut temp, _) = create_scratch_file(spool_dir)
        .map_err(|e| chain_error!(e, "failed to prepare for publication", ?target))?;

    worker(temp.as_file_mut())?;
    match temp.persist_noclobber(target) {
        Ok(_) => Ok(()),
        Err(tempfile::PersistError { error: e, .. })
            if e.kind() == ErrorKind::AlreadyExists || target.exists() =>
        {
            Ok(())
        }
        Err(e) => Err(chain_error!(e, "failed to publish temporary file", ?target)),
    }
}

/// Returns a conservatively percent-encoded URI for `hostname_or` and
/// `path`.  `hostname_or` defaults to the local machine's hostname.
///
/// The URI is of the form
/// `${hostname_hash}-verneuil:${hostname}:${path_hash}/${path}`;
/// there will never be any double slash in `path` (it should be a
/// canonical path), so these URIs should not collide for different
/// hosts.
pub fn manifest_name_for_hostname_path(hostname_or: Option<&str>, path: &Path) -> Result<String> {
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
        static ref PARAMS: umash::Params = umash::Params::derive(0, b"verneuil path params");
    }

    #[allow(clippy::redundant_closure)] // we need to adjust hostname()'s lifetime
    let hostname: &str = hostname_or.unwrap_or_else(|| instance_id::hostname());

    let string = path
        .as_os_str()
        .to_str()
        .ok_or_else(|| fresh_error!("failed to convert path to string", ?path))?;
    let path_hash = PARAMS.hasher(0).write(string.as_bytes()).digest();

    let name = format!(
        "{}-verneuil:{}:{:04x}/{}",
        &instance_id::hostname_hash(hostname),
        hostname,
        path_hash % (1 << (4 * 4)),
        string
    );

    Ok(pseudo_uniquely_fit_string_in_filename(
        percent_encoding::utf8_percent_encode(&name, &ESCAPED).to_string(),
    ))
}

/// Returns a conservatively percent-encoded URI for `path`.
fn percent_encode_local_path_uri(path: &Path) -> Result<String> {
    manifest_name_for_hostname_path(None, path)
}

lazy_static::lazy_static! {
    // We separate the primary and secondary hashes with a
    // percent-encoded slash: we don't expect many collisions in the
    // primary hash, but some platforms seem to shard based on
    // everything left of the last slash.  Without that separator, our
    // data might end up hashing to the same shard, despite the high
    // amount of entropy spread everywhere.
    static ref CHUNK_HASH_SEPARATOR: String =
        percent_encoding::utf8_percent_encode("/", percent_encoding::NON_ALPHANUMERIC).to_string();
}

/// Returns the chunk name for a given fingerprint.  This name is
/// unambiguous and valid as both a POSIX filename and a S3 blob name.
pub(crate) fn fingerprint_chunk_name(fprint: &Fingerprint) -> String {
    format!(
        "{:016x}{}{:016x}",
        fprint.hash(),
        &*CHUNK_HASH_SEPARATOR,
        fprint.secondary()
    )
}

/// Converts a potential chunk name back to a fingerprint.
pub(crate) fn chunk_name_fingerprint(name: &str) -> Option<Fingerprint> {
    if name.len() != (2 * 16) + CHUNK_HASH_SEPARATOR.len() {
        return None;
    }

    let hash = u64::from_str_radix(&name[0..16], 16).ok()?;
    let secondary = u64::from_str_radix(&name[16 + CHUNK_HASH_SEPARATOR.len()..], 16).ok()?;

    Some(Fingerprint::new(hash, secondary))
}

/// Attempts to read a valid Manifest message from `file_path`.
/// Returns Ok(None) if the file does not exist.
///
/// Any chunk necessary to decode the Manifest will be fetched from
/// a cache built with the `cache_builder` and from the `targets`.
#[instrument(level = "trace", err)]
fn read_manifest_at_path(
    file_path: &Path,
    cache_builder: kismet_cache::CacheBuilder,
    targets: &[ReplicationTarget],
) -> Result<Option<(Manifest, Option<Arc<Chunk>>)>> {
    use std::io::Read;

    let mut file = match std::fs::File::open(file_path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(chain_error!(e, "failed to open manifest", ?file_path)),
    };

    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .map_err(|e| chain_error!(e, "failed to read manifest", ?file_path))?;

    match Manifest::decode_and_validate(&contents, cache_builder, Some(targets), file_path) {
        Ok(ret) => Ok(Some(ret)),
        Err(e) => {
            use std::os::unix::fs::MetadataExt;

            // If we failed to parse the manifest, but the manifest
            // file doesn't exist anymore or was overwritten, bubble
            // that up as a missing manifest file.
            let initial_meta = file
                .metadata()
                .map_err(|e| chain_error!(e, "failed to stat open manifest file", ?file_path))?;

            let current_meta = match std::fs::metadata(file_path) {
                Ok(meta) => meta,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(chain_error!(e, "failed to stat manifest file", ?file_path)),
            };

            if (initial_meta.dev(), initial_meta.ino()) == (current_meta.dev(), current_meta.ino())
            {
                Err(e)
            } else {
                Ok(None)
            }
        }
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

/// Returns the path to the consuming directory under `parent`; it may
/// be updated concurrently.
pub(crate) fn mutable_consuming_directory(parent: PathBuf) -> PathBuf {
    let mut ready = parent;
    ready.push(CONSUMING);
    ready
}

/// Attempts to open a snapshot of the "ready" subdirectory of
/// `parent`: renames "ready" to "consuming", and opens a snapshot
/// of that "consuming" directory.
///
/// On success, returns a path for the consuming directory's
/// pseudo-unique subdirectory (which contains the "chunks" and "meta"
/// subdirectories).
///
/// Returns Ok(None) if the directory does not exist.
#[instrument(level = "trace", err)]
pub(crate) fn snapshot_ready_directory(parent: PathBuf) -> Result<Option<PathBuf>> {
    let ready = mutable_ready_directory(parent.clone());
    let consuming = mutable_consuming_directory(parent);

    match std::fs::rename(ready, &consuming) {
        Ok(()) => {}
        // It's fine if `ready` does not exist: someone else might
        // already have renamed it to `consuming`.
        //
        // If also fine if we can't rename over `consuming`: that
        // means there's already something there waiting to be copied.
        Err(e) if matches!(e.kind(), ErrorKind::AlreadyExists | ErrorKind::NotFound) => {}
        // We can also get `ENOTEMPTY` if `consuming` isn't empty;
        // that's not currently exposed as an `ErrorKind`.
        Err(e) if e.raw_os_error() == Some(libc::ENOTEMPTY) => {}
        // Let the caller consume `consuming` if it exists, even if
        // `rename` failed: the caller could make progress.
        Err(e) => {
            if !consuming.is_dir() {
                return Err(chain_error!(e, "failed to acquire new consuming directory"));
            }
        }
    }

    let ret = append_subdirectory(consuming);
    if ret.is_dir() {
        Ok(Some(ret))
    } else {
        Ok(None)
    }
}

/// Attempts to remove the "consuming" subdirectory of `parent`, if it
/// is empty.
#[instrument(level = "trace")]
pub(crate) fn remove_consuming_directory_if_empty(parent: PathBuf) -> Result<()> {
    let mut consuming = parent;
    consuming.push(CONSUMING);

    match std::fs::remove_dir(&consuming) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::ENOTEMPTY) => Err(chain_info!(
            e,
            "found non-empty consuming directory",
            ?consuming
        )),
        Err(e) => Err(chain_error!(
            e,
            "failed to remove consuming directory",
            ?consuming
        )),
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

/// Appends the current instance id to the replication spooling
/// `prefix`.
pub(crate) fn current_spooling_dir(mut prefix: PathBuf) -> PathBuf {
    /// Constructs a subdirectory name that includes the instance id
    /// and the on-disk version suffix.
    fn dir_name(instance_id: &str) -> String {
        format!(
            "verneuil-{}-{}",
            replace_slashes(instance_id),
            ON_DISK_FORMAT_VERSION_SUFFIX,
        )
    }

    // Check for pre-existing subdirectories with slightly different
    // instance ids.
    for probe in instance_id::likely_instance_ids(INSTANCE_ID_PROBE_RANGE) {
        prefix.push(dir_name(&probe));

        if prefix.is_dir() {
            return prefix;
        }

        prefix.pop();
    }

    // Otherwise, create a new one from what we believe is the
    // actual instance id.
    prefix.push(dir_name(instance_id::instance_id()));
    prefix
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
    #[instrument(err)]
    pub fn new(db_path: &Path, fd: &File) -> Result<Option<ReplicationBuffer>> {
        use std::sync::atomic::AtomicBool;

        static CLEAN_ONCE_FLAG: AtomicBool = AtomicBool::new(true);

        let mut spooling = match DEFAULT_SPOOLING_DIRECTORY.read().unwrap().clone() {
            Some(dir) => dir,
            None => return Ok(None),
        };

        // Add an instance id.
        spooling = current_spooling_dir(spooling);

        // Once per process, try to scan for replication directories
        // and `.tap`ped manifest files that refer to inexistent
        // database files.  Obviously, this logic only works because
        // we only have the default spooling directory, but it's
        // better than nothing.
        if CLEAN_ONCE_FLAG.swap(false, std::sync::atomic::Ordering::Relaxed) {
            drop_result!(delete_dangling_replication_directories(spooling.clone()),
                         e => chain_info!(e, "failed to scan for dangling replication directories",
                                          ?spooling));

            drop_result!(delete_orphan_tapped_manifest_files(spooling.clone()),
                         e => chain_info!(e, "failed to scan for orphan manifest files",
                                          ?spooling));
        }

        // And now add the unique local key for the db file.
        spooling.push(mangle_path(db_path)?);
        spooling.push(db_file_key(fd)?);

        // Attempt to delete directories that refer to the same
        // path, but different inode.
        drop_result!(delete_stale_directories(&spooling),
                     e => chain_info!(e, "failed to delete stale directories", ?spooling));

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

            let json_bytes = serde_json::to_vec(&targets).expect("failed to serialize metadata");
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
    #[instrument(level = "trace", err)]
    pub fn stage_chunk(&self, fprint: Fingerprint, data: &[u8]) -> Result<()> {
        use std::io::Write;

        let chunk_name = fingerprint_chunk_name(&fprint);

        let mut target = self.spooling_directory.clone();
        target.push(STAGING);
        target.push(CHUNKS);
        target.push(&chunk_name);

        call_with_temp_file(self.spooling_directory.clone(), &target, |dst| {
            dst.write_all(data)
                .map_err(|e| chain_error!(e, "failed to write chunk", ?target, len = data.len()))
        })
    }

    /// Attempts to overwrite the manifest file for the replicated file.
    #[instrument(err)]
    pub fn publish_manifest(&self, db_path: &Path, manifest: &Manifest) -> Result<()> {
        use prost::Message;
        use std::io::Write;

        let mut encoded = Vec::<u8>::new();
        manifest
            .encode(&mut encoded)
            .map_err(|e| chain_error!(e, "failed to serialize manifest proto", ?manifest))?;

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
            &percent_encode_local_path_uri(db_path)
                .map_err(|e| chain_error!(e, "invalid manifest name", ?db_path))?,
        );
        temp.persist(&target)
            .map_err(|e| chain_error!(e, "failed to publish manifest", ?target))?;
        Ok(())
    }

    /// Returns true if there is definitely a ready manifest file for
    /// `db_path`.
    pub fn has_ready_manifest(&self, db_path: &Path) -> bool {
        let src = || -> Result<PathBuf> {
            let mut src = self.spooling_directory.clone();
            src.push(READY);
            src = append_subdirectory(src);
            src.push(META);
            src.push(&percent_encode_local_path_uri(db_path)?);

            Ok(src)
        };

        match src() {
            Ok(src) => std::fs::symlink_metadata(src).is_ok(),
            Err(_) => false,
        }
    }

    /// Attempts to parse the current "consuming" manifest file, and
    /// returns the current base chunk for the list of chunk
    /// fingerprints, if any.
    #[instrument(level = "trace", err)]
    pub fn read_consuming_manifest(
        &self,
        db_path: &Path,
        cache_builder: kismet_cache::CacheBuilder,
        targets: &[ReplicationTarget],
    ) -> Result<Option<(Manifest, Option<Arc<Chunk>>)>> {
        let mut src = self.spooling_directory.clone();
        src.push(CONSUMING);
        src = append_subdirectory(src);
        src.push(META);
        src.push(&percent_encode_local_path_uri(db_path)?);
        read_manifest_at_path(&src, cache_builder, targets)
    }

    /// Attempts to parse the current ready manifest file, and
    /// returns the current base chunk for the list of chunk
    /// fingerprints, if any.
    #[instrument(level = "trace", err)]
    pub fn read_ready_manifest(
        &self,
        db_path: &Path,
        cache_builder: kismet_cache::CacheBuilder,
        targets: &[ReplicationTarget],
    ) -> Result<Option<(Manifest, Option<Arc<Chunk>>)>> {
        let mut src = self.spooling_directory.clone();
        src.push(READY);
        src = append_subdirectory(src);
        src.push(META);
        src.push(&percent_encode_local_path_uri(db_path)?);
        read_manifest_at_path(&src, cache_builder, targets)
    }

    /// Attempts to parse the current staged manifest file, and
    /// returns the current base chunk for the list of chunk
    /// fingerprints, if any.
    #[instrument(level = "trace", err)]
    pub fn read_staged_manifest(
        &self,
        db_path: &Path,
        cache_builder: kismet_cache::CacheBuilder,
        targets: &[ReplicationTarget],
    ) -> Result<Option<(Manifest, Option<Arc<Chunk>>)>> {
        let mut src = self.spooling_directory.clone();
        src.push(STAGING);
        src.push(META);
        src.push(&percent_encode_local_path_uri(db_path)?);
        read_manifest_at_path(&src, cache_builder, targets)
    }

    /// Returns the path prefix for staged chunks.
    #[allow(dead_code)]
    pub fn staged_chunk_directory(&self) -> PathBuf {
        let mut dir = self.spooling_directory.clone();
        dir.push(STAGING);
        dir.push(CHUNKS);

        dir
    }

    /// Returns the path prefix for ready chunks.
    #[allow(dead_code)]
    pub fn ready_chunk_directory(&self) -> PathBuf {
        let mut dir = self.spooling_directory.clone();
        dir.push(READY);
        dir = append_subdirectory(dir);
        dir.push(CHUNKS);

        dir
    }

    /// Returns the path prefix for consuming chunks.
    #[allow(dead_code)]
    pub fn consuming_chunk_directory(&self) -> PathBuf {
        let mut dir = self.spooling_directory.clone();
        dir.push(CONSUMING);
        dir = append_subdirectory(dir);
        dir.push(CHUNKS);

        dir
    }

    /// Attempts to copy the current "staging" buffer to a temporary
    /// buffer directory.
    ///
    /// This function should fail eagerly: it's better to fail
    /// spuriously and retry than to publish a partial buffer.
    ///
    /// On success, returns the temporary directory.
    #[instrument(skip(chunks), err)]
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

        // Work inside a pseudo unique subdirectory to let copiers
        // consume files without worrying about ABA.
        temp_path.push(pseudo_unique_filename());
        std::fs::create_dir(&temp_path).map_err(|e| {
            filtered_io_error!(
                e,
                ErrorKind::NotFound => Level::DEBUG,
                "failed to create temporary pseudo-unique directory",
                ?temp_path
            )
        })?;
        std::fs::set_permissions(&temp_path, PermissionsExt::from_mode(0o777)).map_err(
            |e| chain_error!(e, "failed to set temporary pseudo-unique dir permissions", dir=?temp_path),
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
                    "failed to link ready manifest",
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
    #[instrument(err)]
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
    #[instrument(skip(chunks), err)]
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
    #[instrument(err)]
    pub fn cleanup_scratch_directory(&self) -> Result<()> {
        fn remove_file_if_stale(path: &Path) {
            if matches!(file_is_stale(path, SCRATCH_FILE_GRACE_PERIOD), Ok(false)) {
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
        percent_encode_local_path_uri(Path::new(SAMPLE_PATH)).expect("should convert"),
        // We assume the test host's name doesn't need escaping.
        format!(
            "{}-verneuil%3A{}%3A634c%2F%2Fa%40%3C%3Csd!%25-_%2F.asd%2F*\'fdg(g%2F)%5C%7E).db",
            instance_id::hostname_hash(instance_id::hostname()),
            instance_id::hostname()
        )
    );
}

#[test]
fn replace_slashes_invertible() {
    const HARD_STRING: &str = "/%asd#//%60";

    let converted = replace_slashes(HARD_STRING);
    // There must not be any slash in the result.
    assert_eq!(converted.find('/'), None);

    // And it must be invertible.
    assert_eq!(
        HARD_STRING,
        &restore_slashes(&converted)
            .expect("must decode")
            .expect("must be reversible")
    );
}

/// A short path should be invertible.
#[test]
fn mangle_path_short() {
    let path = format!("/foo/bar/{}", String::from_utf8(vec![b'X'; 220]).unwrap());

    let mangled = mangle_string(&path);

    assert!(mangled.as_bytes().len() < MAX_MANGLED_NAME_LENGTH);
    assert_eq!(
        path,
        restore_slashes(&mangled)
            .expect("must decode")
            .expect("must be reversible")
    );
}

/// A long path should be clearly not invertible.
#[test]
fn mangle_path_long() {
    let path = format!("/foo/bar/{}", String::from_utf8(vec![b'X'; 221]).unwrap());

    let mangled = mangle_string(&path);

    assert!(mangled.as_bytes().len() > MAX_MANGLED_NAME_LENGTH);
    assert_eq!(None, restore_slashes(&mangled).expect("must decode"));
}

/// A long path should mangle to comfortable less than 255 bytes
/// (NAME_MAX on most platforms).
#[test]
fn mangle_path_over_name_max() {
    let path = format!("/foo/bar/{}", String::from_utf8(vec![b'X'; 1000]).unwrap());

    let mangled = mangle_string(&path);

    assert!(mangled.as_bytes().len() > MAX_MANGLED_NAME_LENGTH);
    assert_eq!(240, mangled.as_bytes().len());
    assert_eq!(None, restore_slashes(&mangled).expect("must decode"));
}

/// The mapping function should be constant across versions.
#[test]
fn test_manifest_name_for_hostname_path() {
    assert_eq!(
        manifest_name_for_hostname_path(Some("test.com"), Path::new("/tmp/test.db")).unwrap(),
        "7cb1-verneuil%3Atest.com%3A80ad%2F%2Ftmp%2Ftest.db"
    );
}

#[test]
fn test_chunk_name_roundtrip() {
    let fprint1 = Fingerprint::new(1, 2);
    assert_eq!(
        fprint1,
        chunk_name_fingerprint(&fingerprint_chunk_name(&fprint1)).expect("should parse")
    );

    let fprint2 = Fingerprint::new(u64::MAX, u64::MAX - 1);
    assert_eq!(
        fprint2,
        chunk_name_fingerprint(&fingerprint_chunk_name(&fprint2)).expect("should parse")
    );
}
