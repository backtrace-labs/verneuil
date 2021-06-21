//! A `Tracker` is responsible for determining the byte ranges that
//! should be synchronised for a given file.

use std::ffi::CStr;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use umash::Fingerprint;

use crate::directory_schema::Directory;
use crate::directory_schema::DirectoryV1;
use crate::replication_buffer::ReplicationBuffer;

/// We snapshot db files in 64KB content-addressed chunks.
const SNAPSHOT_GRANULARITY: u64 = 1 << 16;

#[derive(Debug)]
pub(crate) struct Tracker {
    // The C-side actually owns the file descriptor, but we can share
    // it with Rust: our C code doesn't use the FD's internal cursor.
    file: ManuallyDrop<File>,
    // Canonical path for the tracked file.
    path: PathBuf,
    buffer: Option<ReplicationBuffer>,
}

/// Computes the fingerprint for a sqlite database.  The 100-byte header
/// (https://www.sqlite.org/fileformat.html#:~:text=1.3.%20the%20database%20header)
/// includes a "file change counter" field at offset 24; that field is updated
/// as part of every transaction commit . Fingerprinting the first 100 bytes
/// of a sqlite database should thus give us something that reliably changes
/// whenever the file's contents are modified.
fn fingerprint_sqlite_header(file: &File) -> Option<Fingerprint> {
    const HEADER_SIZE: usize = 100;

    lazy_static::lazy_static! {
        static ref HEADER_PARAMS: umash::Params = umash::Params::derive(0, "verneuil sqlite header params");
    }

    let mut buf = [0u8; HEADER_SIZE];
    match file.read_exact_at(&mut buf, 0) {
        Err(_) => None,
        Ok(_) => Some(Fingerprint::generate(&HEADER_PARAMS, 0, &buf)),
    }
}

/// Computes the fingerprint for a list of fingerprints.  Each
/// fingerprint is converted to little-endian bytes, and the
/// result is fingerprinted.
fn fingerprint_file_directory(chunks: &[Fingerprint]) -> Fingerprint {
    lazy_static::lazy_static! {
        static ref DIRECTORY_PARAMS: umash::Params = umash::Params::derive(0, "verneuil db directory params");
    }

    let mut bytes = Vec::with_capacity(chunks.len() * 16);

    for fprint in chunks {
        bytes.extend(&fprint.hash[0].to_le_bytes());
        bytes.extend(&fprint.hash[1].to_le_bytes());
    }

    Fingerprint::generate(&DIRECTORY_PARAMS, 0, &bytes)
}

/// Computes the fingerprint for a chunk of sqlite db file.
fn fingerprint_file_chunk(bytes: &[u8]) -> Fingerprint {
    lazy_static::lazy_static! {
        static ref CHUNK_PARAMS: umash::Params = umash::Params::derive(0, "verneuil db chunk params");
    }

    Fingerprint::generate(&CHUNK_PARAMS, 0, bytes)
}

fn flatten_chunk_fprints(fprints: &[Fingerprint]) -> Vec<u64> {
    let mut ret = Vec::with_capacity(fprints.len() * 2);

    for fprint in fprints {
        ret.extend(&fprint.hash);
    }

    ret
}

impl Tracker {
    pub fn new(c_path: *const c_char, fd: i32) -> Result<Tracker, &'static str> {
        use std::os::unix::io::FromRawFd;

        if fd < 0 {
            return Err("received negative fd");
        }

        let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
        let string = unsafe { CStr::from_ptr(c_path) }
            .to_str()
            .map_err(|_| "path is not valid utf-8")?
            .to_owned();

        let path = std::fs::canonicalize(string).map_err(|_| "failed to canonicalize path")?;

        assert_ne!(
            path.as_os_str().to_str(),
            None,
            "A path generated from a String should be convertible back to a String."
        );

        let buffer = ReplicationBuffer::new(&path, &file)
            .map_err(|_| "failed to create replication buffer")?;

        Ok(Tracker { file, path, buffer })
    }

    /// Snapshots all the 64KB chunks in the tracked file, and returns
    /// the list of chunk fingerprints.
    fn snapshot_chunks(
        &self,
        repl: &ReplicationBuffer,
    ) -> std::io::Result<(u64, Vec<Fingerprint>)> {
        let len = self.file.metadata()?.len();
        let num_chunks = len / SNAPSHOT_GRANULARITY
            + (if (len % SNAPSHOT_GRANULARITY) > 0 {
                1
            } else {
                0
            });
        let mut chunk_fprints = Vec::with_capacity(num_chunks as usize);

        for i in 0..num_chunks {
            let begin = i * SNAPSHOT_GRANULARITY;
            let end = if (len - begin) > SNAPSHOT_GRANULARITY {
                begin + SNAPSHOT_GRANULARITY
            } else {
                len
            };

            let mut buf = [0u8; SNAPSHOT_GRANULARITY as usize];
            let slice = &mut buf[0..(end - begin) as usize];
            self.file.read_exact_at(slice, begin)?;

            let fprint = fingerprint_file_chunk(slice);

            repl.stage_chunk(fprint, slice)?;
            chunk_fprints.push(fprint);
        }

        Ok((len, chunk_fprints))
    }

    /// Snapshots the contents of the tracked file to its replication
    /// buffer.  Concurrent threads or processes may be doing the same,
    /// but the contents of the file can't change, since we still hold
    /// a sqlite read lock on the db file.
    fn snapshot_file_contents(&self, buf: &ReplicationBuffer) -> Result<(), &'static str> {
        let header_fprint = fingerprint_sqlite_header(&self.file).ok_or("invalid db file")?;

        buf.ensure_staging_dir();
        let (len, chunks) = self
            .snapshot_chunks(&buf)
            .map_err(|_| "failed to snapshot chunks")?;
        let directory_fprint = fingerprint_file_directory(&chunks);

        let directory = Directory {
            v1: Some(DirectoryV1 {
                header_fprint: Some(header_fprint.into()),
                contents_fprint: Some(directory_fprint.into()),
                chunks: flatten_chunk_fprints(&chunks),
                len,
            }),
        };

        buf.publish_directory(&self.path, &directory)
            .map_err(|_| "failed to publish directory file")?;
        Ok(())
    }

    /// Publishes a snapshot of `file` in the replication buffer, if
    /// it exists.
    ///
    /// Must be called with a read lock held on the underlying file.
    pub fn snapshot(&self) -> Result<(), &'static str> {
        if let Some(buffer) = &self.buffer {
            self.snapshot_file_contents(&buffer)
        } else {
            Ok(())
        }
    }
}
