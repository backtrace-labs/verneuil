//! Replicated sqlite DBs are represented as protobuf "manifest"
//! metadata that refer to content-addressed chunks by fingerprint.
use std::fs::File;

use tracing::instrument;
use umash::Fingerprint;
use uuid::Uuid;

use crate::chain_error;
use crate::drop_result;
use crate::fresh_error;
use crate::fresh_warn;
use crate::result::Result;
use crate::warn_from_os;

/// A umash fingerprint.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Fprint {
    #[prost(fixed64, tag = "1")]
    pub major: u64,
    #[prost(fixed64, tag = "2")]
    pub minor: u64,
}

impl From<Fingerprint> for Fprint {
    fn from(fp: Fingerprint) -> Fprint {
        (&fp).into()
    }
}

impl From<&Fingerprint> for Fprint {
    fn from(fp: &Fingerprint) -> Fprint {
        Fprint {
            major: fp.hash(),
            minor: fp.secondary(),
        }
    }
}

impl From<Fprint> for Fingerprint {
    fn from(fp: Fprint) -> Fingerprint {
        Fingerprint::new(fp.major, fp.minor)
    }
}

impl From<&Fprint> for Fingerprint {
    fn from(fp: &Fprint) -> Fingerprint {
        Fingerprint::new(fp.major, fp.minor)
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ManifestV1 {
    // The fingerprint for the file's 100-byte sqlite header.  There
    // may be some rare collisions over long time periods (> 4 billion
    // transactions), or when the file is deleted and re-created, but
    // it's fast to compute.
    #[prost(message, tag = "1")]
    pub header_fprint: Option<Fprint>,

    // A pseudo-unique id for the snapshotted version of the file.
    // This id is as good as possible at detecting potential changes
    // or divergences, at the expense of sometimes claiming that two
    // snapshots or a snapshot and a db file differ when they are
    // actually identical.
    //
    // If this field is missing (empty), the snapshot must not be
    // considered identical to anything.
    #[prost(bytes, tag = "2")]
    pub version_id: Vec<u8>,

    // The fingerprint for the file's list of chunk fingerprints, as
    // little-endian (major, minor) bytes.  Collisions are
    // astronomically unlikely.
    #[prost(message, tag = "3")]
    pub contents_fprint: Option<Fprint>,

    // The total length of the file, in bytes.
    #[prost(uint64, tag = "5")]
    pub len: u64,

    // The ctime of the file.  This information is advisory;
    // in particular, there is no guarantee that the field
    // is populated (non-zero) nor that it grows monotonically.
    #[prost(int64, tag = "6")]
    pub ctime: i64,

    // The fractional part of the file's ctime.
    #[prost(sfixed32, tag = "7")]
    pub ctime_ns: i32,

    // The fingerprints for each chunk as pairs of u64.  The first
    // chunk has fingerprint `chunks[0], chunks[1]`, the second
    // `chunks[2], chunks[3]`, etc.
    #[prost(fixed64, repeated, tag = "15")]
    pub chunks: Vec<u64>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Manifest {
    #[prost(message, tag = "1")]
    pub v1: Option<ManifestV1>,
}

impl Manifest {
    /// Attempts to decode the protobuf bytes in `buf` and
    /// rejects clearly invalid data.
    pub fn decode_and_validate(buf: &[u8], description: impl std::fmt::Debug) -> Result<Manifest> {
        use prost::Message;

        let manifest = Manifest::decode(buf)
            .map_err(|e| chain_error!(e, "failed to parse proto manifest", ?description))?;

        let v1 = match &manifest.v1 {
            Some(v1) => v1,
            None => return Err(fresh_error!("v1 format not found", ?manifest)),
        };

        if v1.contents_fprint.is_none() {
            return Err(fresh_error!("missing contents_fprint", ?manifest));
        }

        if v1.header_fprint.is_none() {
            return Err(fresh_error!("missing header_fprint", ?manifest));
        }

        if v1.ctime <= 0 {
            return Err(fresh_error!("missing or negative ctime", ?manifest));
        }

        // Check the chunk fingerprints: their fingerprint must match, and
        // there must be an even number of u64s (two per fingerprint).
        if Some(fingerprint_v1_chunk_list(&v1.chunks).into()) != v1.contents_fprint
            || (v1.chunks.len() % 2) != 0
        {
            return Err(fresh_error!("invalid chunk list", ?manifest));
        }

        Ok(manifest)
    }
}

/// Computes the `header_fprint` for a sqlite database.  The 100-byte header
/// (https://www.sqlite.org/fileformat.html#:~:text=1.3.%20the%20database%20header)
/// includes a "file change counter" field at offset 24; that field is updated
/// as part of every transaction commit . Fingerprinting the first 100 bytes
/// of a sqlite database should thus give us something that reliably changes
/// whenever the file's contents are modified.
#[instrument]
pub(crate) fn fingerprint_sqlite_header(file: &File) -> Option<Fingerprint> {
    use std::os::unix::fs::FileExt;

    const HEADER_SIZE: usize = 100;

    lazy_static::lazy_static! {
        static ref HEADER_PARAMS: umash::Params = umash::Params::derive(0, b"verneuil sqlite header params");
    }

    let mut buf = [0u8; HEADER_SIZE];
    match file.read_exact_at(&mut buf, 0) {
        Err(error) => {
            // Don't whine if we're trying to fingerprint a fresh
            // empty db file.
            if let Ok(meta) = file.metadata() {
                if meta.len() == 0 {
                    return None;
                }
            }

            tracing::info!(%error, "failed to read sqlite header");
            None
        }
        Ok(_) => Some(HEADER_PARAMS.fingerprinter(0).write(&buf).digest()),
    }
}

lazy_static::lazy_static! {
    static ref XATTR_NAME: std::ffi::CString = std::ffi::CString::new("user.verneuil.version_id").expect("should be a valid cstr");
}

const XATTR_MAX_VALUE_SIZE: usize = 128;

/// Finds the unique id for a sqlite database file.  If possible, we
/// use the verneuil xattr; if the filesystem does not support xattrs,
/// we make do with ctime and the sqlite header fingerprint.
///
/// The fingerprint may be provided by passing it as `fprint_or`; if
/// that argument is `None`, the fingerprint will be computed lazily.
///
/// An empty return value means we failed to extract a version id, and
/// must assume nothing matches.
#[instrument]
pub(crate) fn extract_version_id(
    file: &File,
    mut fprint_or: Option<Fingerprint>,
    // Pass in a mutable buffer to enable reuse: this code touches
    // operations that are timing sensitive for sqlite's tests.
    mut buf: Vec<u8>,
) -> Vec<u8> {
    use std::os::unix::io::AsRawFd;

    extern "C" {
        fn verneuil__getxattr(fd: i32, name: *const i8, buf: *mut u8, bufsz: usize) -> isize;
    }

    buf.resize(XATTR_MAX_VALUE_SIZE, 0u8);

    #[cfg(feature = "no_xattr")]
    let ret = -1;
    #[cfg(not(feature = "no_xattr"))]
    let ret = unsafe {
        verneuil__getxattr(
            file.as_raw_fd(),
            XATTR_NAME.as_ptr() as *const _,
            buf.as_mut_ptr(),
            buf.len(),
        )
    };

    // xattrs work, but we can't get one.  Assume the worst.
    if ret == 0 {
        use std::time::Duration;

        #[cfg(target_os = "linux")]
        const ENODATA: i32 = 61;

        #[cfg(not(target_os = "linux"))]
        const ENODATA: i32 = 0;

        /// Determines if `file` was definitely created recently (less
        /// than two seconds ago).  If so returns the file's age.
        ///
        /// Otherwise (the file is old or we couldn't find its age),
        /// returns `None`.
        fn file_is_recent(file: &File) -> Option<Duration> {
            const MAX_AGE: Duration = Duration::from_secs(2);

            let age = file.metadata().ok()?.created().ok()?.elapsed().ok()?;
            if age < MAX_AGE {
                Some(age)
            } else {
                None
            }
        }

        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound
            && error.raw_os_error() != Some(0)
            && error.raw_os_error() != Some(ENODATA)
        {
            if let Some(age) = file_is_recent(file) {
                tracing::debug!(
                    ?error,
                    ?age,
                    "failed to read version xattr on newly-created file"
                );
            } else {
                tracing::warn!(?error, "failed to read version xattr");
            }
        }

        buf.clear();
        return buf;
    }

    // Otherwise, take what we can get.
    buf.resize(ret.max(0) as usize, 0u8);

    if fprint_or.is_none() {
        fprint_or = fingerprint_sqlite_header(file);

        // If we don't have an xattr and we also don't have a header
        // fprint, we don't want to *only* rely on ctime, even if
        // enabled: it's too lossy.  Instead return an empty version
        // id, which will be treated as different from every version
        // id, including other empty ones.
        if buf.is_empty() && fprint_or.is_none() {
            return buf;
        }
    }

    // Add a high resolution ctime if we can find it.  Unfortunately,
    // while the interface goes down to nanoseconds, reality is much
    // coarser, so the ctime by itself cannot suffice.
    #[cfg(feature = "mix_ctime_in_version_id")]
    match file.metadata() {
        Ok(meta) => {
            use std::os::unix::fs::MetadataExt;

            buf.extend(&meta.ctime().to_le_bytes());
            buf.extend(&meta.ctime_nsec().to_le_bytes());
        }
        Err(error) => tracing::error!(%error, "failed to read file metadata"),
    }

    // Finally, always append the sqlite fingerprint.  This way we
    // never do worse at change tracking than by using the
    // fingerprint.
    if let Some(fprint) = fprint_or {
        buf.extend(&fprint.hash[0].to_le_bytes());
        buf.extend(&fprint.hash[1].to_le_bytes());
    }

    buf
}

/// Updates the version id for `file`.
///
/// Updates the file's xattr when possible, otherwise no-ops: code
/// that reads the version id always combines it with the file's ctime
/// and sqlite header, which can mostly be trusted to change whenever
/// sqlite writes to the file.
#[instrument(err)]
pub(crate) fn update_version_id(file: &File, cached_uuid: Option<Uuid>) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    use uuid::adapter::Hyphenated;

    extern "C" {
        fn verneuil__setxattr(fd: i32, name: *const i8, buf: *const u8, bufsz: usize) -> isize;
    }

    // We serialise UUIDs with the low-level interface to minimise
    // performance overhead: some sqlite tests flake when this is too
    // slow.
    //
    // We store a human-readable string because people expect
    // to be able to list file attributes at the command line,
    // and binary noise isn't very friendly.
    let mut buf = [0u8; Hyphenated::LENGTH];
    let tag = cached_uuid
        .unwrap_or_else(Uuid::new_v4)
        .to_hyphenated()
        .encode_lower(&mut buf);

    #[cfg(feature = "no_xattr")]
    let ret = 1;
    #[cfg(not(feature = "no_xattr"))]
    let ret = unsafe {
        verneuil__setxattr(
            file.as_raw_fd(),
            XATTR_NAME.as_ptr() as *const _,
            tag.as_ptr(),
            tag.len(),
        )
    };

    if ret >= 0 {
        Ok(())
    } else {
        Err(warn_from_os!("failed to update version xattr"))
    }
}

/// Erases the version id on `file`.  Change `Tracker`s will have to
/// rebuild the replication state from scratch.
#[instrument(err)]
pub(crate) fn clear_version_id(file: &File) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    extern "C" {
        fn verneuil__setxattr(fd: i32, name: *const i8, buf: *const u8, bufsz: usize) -> isize;
        fn verneuil__touch(fd: i32) -> i32;
    }

    if unsafe { verneuil__touch(file.as_raw_fd()) } < 0 {
        let _ = warn_from_os!("failed to touch file");
    }

    let ret = unsafe {
        verneuil__setxattr(
            file.as_raw_fd(),
            XATTR_NAME.as_ptr() as *const _,
            [].as_ptr(),
            0,
        )
    };
    let result = if ret >= 0 {
        Ok(())
    } else {
        Err(warn_from_os!("failed to clear version xattr"))
    };

    // An fsync failure really means badness.  In theory, the only way
    // to recover is to abort the process.
    drop_result!(file.sync_all(),
                 e => chain_error!(e, "failed to fsync updated version id"));

    result
}

lazy_static::lazy_static! {
    static ref CHUNK_PARAMS: umash::Params = umash::Params::derive(0, b"verneuil db chunk params");
}

/// Computes the fingerprint for a chunk of sqlite db file.
pub(crate) fn fingerprint_file_chunk(bytes: &[u8]) -> Fingerprint {
    CHUNK_PARAMS.fingerprinter(0).write(bytes).digest()
}

/// Computes the first half of the fingerprint for a chunk of sqlite db file.
pub(crate) fn hash_file_chunk(bytes: &[u8]) -> u64 {
    CHUNK_PARAMS.hasher(0).write(bytes).digest()
}

/// Computes the `contents_fprint` for a given `chunks` array of u64.
/// We assume the `chunks` array was generated by extracting the
/// first (major) and second (minor) hash of each fingerprint in order;
/// each is converted to little-endian bytes, and the
/// result is fingerprinted.
pub(crate) fn fingerprint_v1_chunk_list(chunks: &[u64]) -> Fingerprint {
    lazy_static::lazy_static! {
    // Manifest files used to be called "directory" files.
        static ref MANIFEST_PARAMS: umash::Params = umash::Params::derive(0, b"verneuil db directory params");
    }

    let mut fingerprinter = MANIFEST_PARAMS.fingerprinter(0);

    if cfg!(target_endian = "little") {
        let slice = unsafe {
            std::slice::from_raw_parts(
                chunks.as_ptr() as *const u8,
                chunks.len() * std::mem::size_of::<u64>(),
            )
        };

        return fingerprinter.write(&slice).digest();
    }

    let mut bytes = Vec::with_capacity(chunks.len() * 8);

    for word in chunks {
        bytes.extend(&word.to_le_bytes());
    }

    fingerprinter.write(&bytes).digest()
}

/// Returns the header Fingerprint and ctime stored in the manifest
/// proto at `path`, or `(None, UNIX_EPOCH)` if there is no such file.
pub(crate) fn parse_manifest_info(
    path: &std::path::Path,
) -> Result<(Option<Fingerprint>, std::time::SystemTime)> {
    use prost::Message;
    use std::time::SystemTime;

    match std::fs::read(path) {
        Ok(contents) => {
            let manifest = Manifest::decode(&*contents)
                .map_err(|e| chain_error!(e, "failed to parse proto manifest", ?path))?;
            if let Some(v1) = manifest.v1 {
                Ok((
                    v1.header_fprint.map(Into::into),
                    SystemTime::UNIX_EPOCH
                        + std::time::Duration::new(v1.ctime as u64, v1.ctime_ns as u32),
                ))
            } else {
                Ok((None, SystemTime::UNIX_EPOCH))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((None, SystemTime::UNIX_EPOCH)),
        Err(e) => Err(chain_error!(e, "failed to open manifest proto file", ?path)),
    }
}

/// Returns the list of chunks in `manifest`.
pub(crate) fn extract_manifest_chunks(manifest: &Manifest) -> Result<Vec<Fingerprint>> {
    let v1 = if let Some(v1) = &manifest.v1 {
        v1
    } else {
        return Err(fresh_warn!("manifest proto v1 not found", ?manifest));
    };

    let mut ret = Vec::with_capacity(v1.chunks.len() / 2);
    for i in 0..v1.chunks.len() / 2 {
        ret.push(Fingerprint::new(v1.chunks[2 * i], v1.chunks[2 * i + 1]));
    }

    Ok(ret)
}

pub(crate) fn extract_manifest_len(manifest: &Manifest) -> Result<u64> {
    if let Some(v1) = &manifest.v1 {
        Ok(v1.len)
    } else {
        Err(fresh_warn!("invalid manifest proto v1", ?manifest))
    }
}

/// Returns the list of chunks in the manifest proto at `Path`, or
/// an empty list if there is no such file.
#[instrument]
pub(crate) fn parse_manifest_chunks(path: &std::path::Path) -> Result<Vec<Fingerprint>> {
    match std::fs::read(path) {
        Ok(contents) => extract_manifest_chunks(&Manifest::decode_and_validate(&*contents, path)?),

        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(chain_error!(e, "failed to open manifest proto file", ?path)),
    }
}

#[test]
fn check_fingerprint_v1_reference() {
    // The parameters are part of the wire format, and should never change for v1.
    assert_eq!(
        fingerprint_v1_chunk_list(&[1, 516]),
        Fingerprint::new(7575684803259252638, 13253811199699765610)
    );
}

/// The chunk fingerprint / hash are part of the wire format, and
/// should not change (for v1, at least).
#[test]
fn check_chunk_fingerprint() {
    let zeros = vec![0u8; 1usize << 16];

    assert_eq!(
        fingerprint_file_chunk(&zeros),
        Fingerprint::new(8155395758008617606, 2728302605148947890)
    );

    assert_eq!(hash_file_chunk(&zeros), 8155395758008617606);
}
