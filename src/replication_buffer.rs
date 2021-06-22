//! The replication buffer subsystem manages replication data and
//! ensures that consistent snapshots are regularly propagated to
//! remote object storage.
//!
//! Each buffer is a directory that includes a "staging" directory,
//! and a "ready" directory.
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
use crate::directory_schema::Directory;
use crate::instance_id;
use crate::process_id::process_id;

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
    buffer_directory: PathBuf,
}

lazy_static::lazy_static! {
    static ref DEFAULT_STAGING_DIRECTORY: RwLock<Option<PathBuf>> = Default::default();
}

const STAGING: &str = "staging";
const READY: &str = "ready";
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

    let string = path
        .as_os_str()
        .to_str()
        .ok_or_else(|| Error::new(ErrorKind::Other, "unable to convert path to string"))?;

    let name = format!("verneuil:{}/{}", instance_id::hostname(), string);
    Ok(percent_encoding::utf8_percent_encode(&name, &ESCAPED).to_string())
}

fn fingerprint_chunk_name(fprint: &Fingerprint) -> String {
    format!("{:016x}.{:016x}", fprint.hash[0], fprint.hash[1])
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

        let chunk_name = fingerprint_chunk_name(&fprint);

        let mut target = self.buffer_directory.clone();
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

        let mut target = self.buffer_directory.clone();
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
        let encoded = percent_encode_path_uri(db_path)?;
        target.push(&encoded);
        temp.persist(&target)?;
        Ok(())
    }

    /// Attempts to parse the current ready directory file.
    pub fn read_ready_directory(&self, db_path: &Path) -> Result<Directory> {
        use prost::Message;

        let mut src = self.buffer_directory.clone();
        src.push(READY);
        src.push(&percent_encode_path_uri(db_path)?);

        Directory::decode(&*std::fs::read(src)?)
            .map_err(|_| Error::new(ErrorKind::Other, "failed to parse proto directory"))
    }

    /// Attempts to parse the current staged directory file.
    pub fn read_staged_directory(&self, db_path: &Path) -> Result<Directory> {
        use prost::Message;

        let mut src = self.buffer_directory.clone();
        src.push(STAGING);
        src.push(&percent_encode_path_uri(db_path)?);

        Directory::decode(&*std::fs::read(src)?)
            .map_err(|_| Error::new(ErrorKind::Other, "failed to parse proto directory"))
    }

    /// Attempts to parse the current staged directory file.
    #[allow(dead_code)]
    pub fn read_staged_chunk(&self, fprint: &Fingerprint) -> Result<Vec<u8>> {
        let mut src = self.buffer_directory.clone();
        src.push(STAGING);
        src.push(CHUNKS);
        src.push(&fingerprint_chunk_name(fprint));

        std::fs::read(src)
    }

    /// Attempts to copy the current "staging" buffer to the "ready"
    /// buffer.
    pub fn prepare_ready_buffer(&self, db_path: &Path, chunks: &[Fingerprint]) -> Result<TempDir> {
        use tempfile::Builder;

        let live: HashSet<String> = chunks.iter().map(fingerprint_chunk_name).collect();

        let mut staging = self.buffer_directory.clone();
        staging.push(STAGING);

        let mut target = self.buffer_directory.clone();
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

        {
            for file in std::fs::read_dir(&staging)?.flatten() {
                if let Some(name) = file.file_name().to_str() {
                    if live.contains(name) {
                        staging.push(name);
                        temp_path.push(name);

                        std::fs::hard_link(&staging, &temp_path)?;

                        temp_path.pop();
                        staging.pop();
                    }
                }
            }
        }

        temp_path.pop();
        staging.pop();

        // Now hardlink the directory proto file.
        let directory_file = percent_encode_path_uri(db_path)?;
        staging.push(&directory_file);
        temp_path.push(&directory_file);
        std::fs::hard_link(staging, temp_path)?;

        Ok(temp)
    }

    /// Attempts to publish a temporary buffer directory to an empty
    /// or missing "ready" buffer.
    ///
    /// Returns `None` on success, the temporary buffer on failure.
    pub fn publish_ready_buffer_fast(&self, ready: TempDir) -> Option<TempDir> {
        let mut target = self.buffer_directory.clone();
        target.push(READY);

        match std::fs::rename(ready.path(), &target) {
            Err(_) => Some(ready),
            Ok(_) => None,
        }
    }

    /// Attempts to publish a temporary buffer directory to a potentially
    /// re-existing buffer.
    pub fn publish_ready_buffer_slow(&self, ready: TempDir) -> Result<()> {
        // See c/file_ops.h
        extern "C" {
            fn verneuil__exchange_paths(x: *const c_char, y: *const c_char) -> i32;
        }

        let mut target = self.buffer_directory.clone();
        target.push(READY);

        let ready_str = CString::new(ready.path().as_os_str().as_bytes())?;
        let target_str = CString::new(target.as_os_str().as_bytes())?;
        if unsafe { verneuil__exchange_paths(ready_str.as_ptr(), target_str.as_ptr()) } == 0 {
            Ok(())
        } else {
            // In the common case, `exchange_paths` fails because
            // `target` blinked out of existence.  In that case, `rename`
            // can only fail if `target` now exists, which should only
            // be possible if another thread or process has published
            // its own ready buffer, for the current db state.
            std::fs::rename(ready.path(), &target)
        }
    }

    /// Attempts to clean up any chunk file that's not referred by the
    /// `chunks` list.
    pub fn gc_chunks(&self, chunks: &[Fingerprint]) -> Result<()> {
        let live: HashSet<String> = chunks.iter().map(fingerprint_chunk_name).collect();

        let mut chunks = self.buffer_directory.clone();
        chunks.push(STAGING);
        chunks.push(CHUNKS);

        for file in std::fs::read_dir(chunks)?.flatten() {
            if let Some(name) = file.file_name().to_str() {
                if live.contains(name) {
                    continue;
                }
            }

            // Attempt to delete the file.  We don't guarantee
            // anything except that we keep everyting in `chunks`,
            // so eat failures, which could happen, e.g., with
            // concurrent GCs.
            let _ = std::fs::remove_file(file.path());
        }

        Ok(())
    }

    /// Attempts to delete all temporary files and directory from "staging/scratch."
    pub fn cleanup_scratch_directory(&self) -> Result<()> {
        let mut scratch = self.buffer_directory.clone();
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
                let _ = std::fs::remove_file(entry.path());
                continue;
            }

            // Temporary directories always have a ".tmp" suffix.
            // Atomically change that to ".del" to make sure all
            // further operations on that directory (including
            // publishing it as a new ready buffer) fail.
            let mut path = entry.path().to_owned();
            if path.extension() != Some(std::ffi::OsStr::new(".del")) {
                let old_path = path.clone();

                path.set_extension(".del");
                let _ = std::fs::rename(old_path, &path);
            }

            // We know `path` is marked for deletion with a ".del"
            // extension.  We can now delete it recursively.  Nothing
            // to do if that fails... it's probably a concurrent
            // deletion, and that we care about is that this
            // subdirectory eventually goes away, if possible.
            let _ = std::fs::remove_dir_all(path);
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
            "verneuil%3A{}%2F%2Fa%40%3C%3Csd!%25-_%2F.asd%2F*\'fdg(g%2F)%5C%7E).db",
            instance_id::hostname()
        )
    );
}
