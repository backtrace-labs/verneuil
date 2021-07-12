//! The replication buffer subsystem manages replication data and
//! ensures that consistent snapshots are regularly propagated to
//! remote object storage.
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
use crate::directory_schema::fingerprint_v1_chunk_list;
use crate::directory_schema::Directory;
use crate::instance_id;
use crate::process_id::process_id;
use crate::replication_target::ReplicationTargetList;

use std::collections::HashSet;
use std::ffi::CString;
use std::fs::File;
use std::fs::Permissions;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use tempfile::TempDir;
use umash::Fingerprint;

#[derive(Debug)]
pub(crate) struct ReplicationBuffer {
    spooling_directory: PathBuf,
}

lazy_static::lazy_static! {
    static ref DEFAULT_SPOOLING_DIRECTORY: RwLock<Option<PathBuf>> = Default::default();
}

/// The "staging" subdirectory is updated in-place to match the db.
const STAGING: &str = "staging";

/// When the "staging" subdirectory matches the db, it can be copied
/// atomically to an empty or missing "ready" subdirectory: the copy
/// logic clears the directory once everything has been copied.
const READY: &str = "ready";

/// The ".metadata" file, at the toplevel of the buffer directory,
/// tells copier where to copy ready and staged data.
const DOT_METADATA: &str = ".metadata";

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

/// Sets the default spooling directory for replication subdirectories,
/// if it isn't already set.
pub(crate) fn set_default_spooling_directory(
    path: &Path,
    permissions: std::fs::Permissions,
) -> Result<()> {
    let mut default = DEFAULT_SPOOLING_DIRECTORY.write().unwrap();

    if let Some(old_path) = &*default {
        if old_path == path {
            return Ok(());
        }

        return Err(Error::new(
            ErrorKind::InvalidInput,
            "default spooling directory already set",
        ));
    }

    // We perform this I/O with a lock held, but we expect
    // `set_default_spooling_directory` to be called early enough that
    // there is no contention.
    if !path.exists() {
        std::fs::create_dir_all(path)?;
        std::fs::set_permissions(path, permissions)?;
    }

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

/// Constructs a unique human-readable filename for `fd`: we derive a
/// unique key with the file's device and inode ids.
fn db_file_key(fd: &File) -> Result<String> {
    use std::os::unix::fs::MetadataExt;

    let meta = fd.metadata()?;
    Ok(format!("{}.{}", meta.dev(), meta.ino()))
}

/// Attempts to delete all directories in the parent of `goal_path`
/// except `goal_path`.
fn delete_stale_directories(goal_path: &Path) -> Result<()> {
    let mut parent = goal_path.to_owned();
    if !parent.pop() {
        return Ok(());
    }

    let goal_filename = goal_path.file_name();

    for subdir in std::fs::read_dir(&parent)?.flatten() {
        if Some(subdir.file_name().as_os_str()) != goal_filename {
            parent.push(subdir.file_name());
            let _ = std::fs::remove_dir_all(&parent);
            parent.pop();
        }
    }

    Ok(())
}

/// Creates a temporary file, populates it with `worker`, and
/// publishes it to `target` on success.
fn call_with_temp_file<T>(target: &Path, worker: impl Fn(&mut File) -> Result<T>) -> Result<T> {
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
        .ok_or_else(|| Error::new(ErrorKind::Other, "unable to convert path to string"))?;
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
fn read_directory_at_path(file_path: &Path) -> Result<Directory> {
    use prost::Message;

    let directory = Directory::decode(&*std::fs::read(file_path)?)
        .map_err(|_| Error::new(ErrorKind::Other, "failed to parse proto directory"))?;

    match &directory.v1 {
        Some(v1)
            // We must have a v1 entry, the chunk fingerprints must match, and there
            // must be an even number of u64 in the list (we need two per fingerprint).
            if Some(fingerprint_v1_chunk_list(&v1.chunks).into()) == v1.contents_fprint
                && (v1.chunks.len() % 2) == 0 =>
        {
            Ok(directory)
        },
        Some(_) => Err(Error::new(ErrorKind::Other, "invalid chunk list")),
        None => Err(Error::new(ErrorKind::Other, "v1 format not found")),
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
pub(crate) fn snapshot_ready_directory(parent: PathBuf) -> Result<(PathBuf, File)> {
    use std::os::unix::io::FromRawFd;

    // See c/file_ops.h
    extern "C" {
        fn verneuil__open_directory(path: *const c_char) -> i32;
    }

    let ready = mutable_ready_directory(parent);
    let ready_str = CString::new(ready.as_os_str().as_bytes())?;
    let fd = unsafe { verneuil__open_directory(ready_str.as_ptr()) };
    if fd < 0 {
        return Err(Error::last_os_error());
    }

    let file = unsafe { File::from_raw_fd(fd) };
    Ok((format!("/proc/self/fd/{}/", fd).into(), file))
}

/// Attempts to remove the "ready" subdirectory of `parent`, if it is
/// empty.
pub(crate) fn remove_ready_directory_if_empty(parent: PathBuf) -> Result<()> {
    let mut ready = parent;
    ready.push(READY);

    std::fs::remove_dir(ready)
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
                let _ = delete_stale_directories(&spooling);
            }

            std::fs::create_dir_all(&spooling)?;
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
    pub fn ensure_staging_dir(&self, targets: &ReplicationTargetList, overwrite_meta: bool) {
        let mut buf = self.spooling_directory.clone();
        buf.push(STAGING);

        let _ = std::fs::create_dir_all(&buf);
        for subdir in &SUBDIRS {
            buf.push(subdir);
            let _ = std::fs::create_dir_all(&buf);
            buf.pop();
        }

        buf.pop();
        // Make sure the metadata file exists.
        buf.push(DOT_METADATA);
        if overwrite_meta || !buf.exists() {
            use std::io::Write;

            let json_bytes = serde_json::to_vec(&targets).expect("failed to serialize metadata.");
            let _ = call_with_temp_file(&buf, |file| file.write_all(&json_bytes));
        }
    }

    /// Attempts to publish a chunk of data for `fprint`.  This file
    /// might already exist, in which case we don't have to do anything.
    pub fn stage_chunk(&self, fprint: Fingerprint, data: &[u8]) -> Result<()> {
        use std::io::Write;

        let chunk_name = fingerprint_chunk_name(&fprint);

        let mut target = self.spooling_directory.clone();
        target.push(STAGING);
        target.push(CHUNKS);
        target.push(&chunk_name);

        if target.exists() {
            return Ok(());
        }

        call_with_temp_file(&target, |dst| dst.write_all(data))
    }

    /// Attempts to overwrite the directory file for the replicated file.
    pub fn publish_directory(&self, db_path: &Path, directory: &Directory) -> Result<()> {
        use prost::Message;
        use std::io::Write;
        use tempfile::Builder;

        let mut encoded = Vec::<u8>::new();
        directory
            .encode(&mut encoded)
            .map_err(|_| Error::new(ErrorKind::Other, "failed to serialise directory proto"))?;

        let mut target = self.spooling_directory.clone();
        target.push(STAGING);
        target.push(SCRATCH);

        let temp = Builder::new()
            .prefix(&(process_id() + "."))
            .suffix(".tmp")
            .tempfile_in(&target)?;
        temp.as_file()
            .set_permissions(Permissions::from_mode(0o444))?;
        temp.as_file().write_all(&encoded)?;

        target.pop();
        target.push(META);
        target.push(&percent_encode_path_uri(db_path)?);
        temp.persist(&target)?;
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
    pub fn read_ready_directory(&self, db_path: &Path) -> Result<Directory> {
        let mut src = self.spooling_directory.clone();
        src.push(READY);
        src.push(META);
        src.push(&percent_encode_path_uri(db_path)?);
        read_directory_at_path(&src)
    }

    /// Attempts to parse the current staged directory file.
    pub fn read_staged_directory(&self, db_path: &Path) -> Result<Directory> {
        let mut src = self.spooling_directory.clone();
        src.push(STAGING);
        src.push(META);
        src.push(&percent_encode_path_uri(db_path)?);
        read_directory_at_path(&src)
    }

    /// Attempts to parse the current staged directory file.
    #[allow(dead_code)]
    pub fn read_staged_chunk(&self, fprint: &Fingerprint) -> Result<Vec<u8>> {
        let mut src = self.spooling_directory.clone();
        src.push(STAGING);
        src.push(CHUNKS);
        src.push(&fingerprint_chunk_name(fprint));

        std::fs::read(src)
    }

    /// Attempts to parse the current staged directory file.
    #[allow(dead_code)]
    pub fn read_ready_chunk(&self, fprint: &Fingerprint) -> Result<Vec<u8>> {
        let mut src = self.spooling_directory.clone();
        src.push(READY);
        src.push(CHUNKS);
        src.push(&fingerprint_chunk_name(fprint));

        std::fs::read(src)
    }

    /// Attempts to copy the current "staging" buffer to a temporary
    /// buffer directory.
    ///
    /// This function should fail eagerly: it's better to fail
    /// spuriously and retry than to publish a partial buffer.
    ///
    /// On success, returns the number of chunk files copied, and
    /// the temporary directory.
    pub fn prepare_ready_buffer(&self, chunks: &[Fingerprint]) -> Result<(usize, TempDir)> {
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
            .tempdir_in(&target)?;

        let mut temp_path = temp.path().to_owned();

        // hardlink relevant chunk files to `temp_path/chunks`.
        staging.push(CHUNKS);
        temp_path.push(CHUNKS);
        std::fs::create_dir(&temp_path)?;

        let mut count = 0;
        for file in std::fs::read_dir(&staging)? {
            if let Some(name) = file?.file_name().to_str() {
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
                        err => err?,
                    }
                    count += 1;

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
        std::fs::create_dir(&temp_path)?;

        for file_or in std::fs::read_dir(&staging)? {
            let file = file_or?;
            staging.push(file.file_name());
            temp_path.push(file.file_name());

            std::fs::hard_link(&staging, &temp_path)?;

            temp_path.pop();
            staging.pop();
        }

        Ok((count, temp))
    }

    /// Attempts to publish a temporary buffer directory to an empty
    /// or missing "ready" buffer.
    ///
    /// Returns `None` on success, the temporary buffer on failure.
    pub fn publish_ready_buffer(&self, ready: TempDir) -> Result<()> {
        let mut target = self.spooling_directory.clone();
        target.push(READY);

        std::fs::rename(ready.path(), &target)
    }

    /// Attempts to signal this new `ready` subdirectory to the `copier`.
    pub fn signal_copier(&self, copier: &crate::copier::Copier) {
        copier.signal_ready_buffer(self.spooling_directory.clone());
    }

    /// Attempts to clean up any chunk file that's not referred by the
    /// `chunks` list.
    pub fn gc_chunks(&self, chunks: &[Fingerprint]) -> Result<()> {
        let live: HashSet<String> = chunks.iter().map(fingerprint_chunk_name).collect();

        let mut chunks = self.spooling_directory.clone();
        chunks.push(STAGING);
        chunks.push(CHUNKS);

        for file in std::fs::read_dir(&chunks)?.flatten() {
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
            let _ = std::fs::remove_file(&chunks);
            chunks.pop();
        }

        Ok(())
    }

    /// Attempts to delete all temporary files and directory from "staging/scratch."
    pub fn cleanup_scratch_directory(&self) -> Result<()> {
        let mut scratch = self.spooling_directory.clone();
        scratch.push(STAGING);
        scratch.push(SCRATCH);

        for entry in std::fs::read_dir(&scratch)?.flatten() {
            let is_dir = match entry.file_type() {
                Ok(file_type) => file_type.is_dir(),
                Err(_) => false,
            };

            if !is_dir {
                // The entry isn't a directory.  We can unlink it
                // atomically, and there's nothing to do on failure:
                // we simply want to remove as many now-useless files
                // as possible.
                scratch.push(entry.file_name());
                let _ = std::fs::remove_file(&scratch);
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
                let _ = std::fs::rename(old_path, &scratch);
            }

            // We know `path` is marked for deletion with a ".del"
            // extension.  We can now delete it recursively.  Nothing
            // to do if that fails... it's probably a concurrent
            // deletion, and that we care about is that this
            // subdirectory eventually goes away, if possible.
            let _ = std::fs::remove_dir_all(&scratch);
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
