//! A `Tracker` is responsible for determining the byte ranges that
//! should be synchronised for a given file.

use std::ffi::CStr;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use umash::Fingerprint;

use crate::copier::Copier;
use crate::directory_schema::fingerprint_file_chunk;
use crate::directory_schema::fingerprint_sqlite_header;
use crate::directory_schema::fingerprint_v1_chunk_list;
use crate::directory_schema::Directory;
use crate::directory_schema::DirectoryV1;
use crate::replication_buffer::ReplicationBuffer;
use crate::replication_target::ReplicationTargetList;

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
    copier: Copier,
    replication_targets: ReplicationTargetList,
}

fn flatten_chunk_fprints(fprints: &[Fingerprint]) -> Vec<u64> {
    let mut ret = Vec::with_capacity(fprints.len() * 2);

    for fprint in fprints {
        ret.extend(&fprint.hash);
    }

    ret
}

fn rebuild_chunk_fprints(flattened: &[u64]) -> Vec<Fingerprint> {
    let mut ret = Vec::with_capacity(flattened.len() / 2);

    for i in 0..flattened.len() / 2 {
        ret.push(Fingerprint {
            hash: [flattened[2 * i], flattened[2 * i + 1]],
        });
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

        let copier = Copier::get_global_copier();

        // Let the copier pick up any ready snapshot left behind, e.g,
        // by an older crashed process.
        if let Some(buf) = &buffer {
            buf.signal_copier(&copier);
        }

        let replication_targets = crate::replication_target::get_default_replication_targets();

        Ok(Tracker {
            file,
            path,
            buffer,
            copier,
            replication_targets,
        })
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

    /// Attempts to assert that the snapshot's contents match that of
    /// our db file.
    #[cfg(feature = "verneuil_test_vfs")]
    fn compare_snapshot(&self, buf: &ReplicationBuffer) -> std::io::Result<()> {
        use blake2b_simd::Params;
        use std::os::unix::io::AsRawFd;

        let self_path = format!("/proc/self/fd/{}", self.file.as_raw_fd());
        let expected = match File::open(&self_path) {
            // If we can't open the DB file, this isn't a
            // replication problem.
            Err(_) => return Ok(()),
            Ok(mut file) => {
                let mut hasher = Params::new().hash_length(32).to_state();
                std::io::copy(&mut file, &mut hasher)?;
                hasher.finalize()
            }
        };

        let mut hasher = Params::new().hash_length(32).to_state();
        let directory = buf
            .read_staged_directory(&self.path)
            .expect("directory must parse")
            .v1
            .expect("v1 component must be populated.");

        // The header fingerprint must match the current header.
        assert_eq!(
            directory.header_fprint,
            fingerprint_sqlite_header(&File::open(&self_path).expect("must open"))
                .map(|fp| fp.into())
        );

        // If the ready file still exists, it must match the staged
        // one.  Otherwise, the directory must have been consumed by
        // the copier.
        if let Ok(ready) = buf.read_ready_directory(&self.path) {
            assert_eq!(directory, ready.v1.expect("v1 component must be populated"));
        } else {
            assert!(!buf.ready_directory_present());
        }

        for i in 0..directory.chunks.len() / 2 {
            let fprint = Fingerprint {
                hash: [directory.chunks[2 * i], directory.chunks[2 * i + 1]],
            };

            let contents = buf.read_staged_chunk(&fprint)?;
            // The contents of a content-addressed chunk must have the same
            // fingerprint as the chunk's name.
            assert_eq!(fprint, fingerprint_file_chunk(&contents));

            // If the ready chunk still exists, it must match the
            // staged one.
            if let Ok(ready) = buf.read_ready_chunk(&fprint) {
                assert_eq!(contents, ready);
            } else {
                assert!(!buf.ready_directory_present());
            }

            if i + 1 < directory.chunks.len() / 2 {
                assert_eq!(contents.len(), SNAPSHOT_GRANULARITY as usize);
            }

            hasher.update(&contents);
        }

        assert_eq!(expected, hasher.finalize());
        Ok(())
    }

    /// Snapshots the contents of the tracked file to its replication
    /// buffer.  Concurrent threads or processes may be doing the same,
    /// but the contents of the file can't change, since we still hold
    /// a sqlite read lock on the db file.
    fn snapshot_file_contents(&self, buf: &ReplicationBuffer) -> Result<(), &'static str> {
        let header_fprint = fingerprint_sqlite_header(&self.file).ok_or("invalid db file")?;

        // Returns the argument's DirectoryV1 if the contents of that
        // directory proto most likely match that of our sqlite db
        // file.
        //
        // Returns None on mismatch or if the argument is Err: callers
        // only want to skip work if data is definitely up to date, but
        // it's never wrong to perform redundant work.
        let up_to_date = |proto_or: std::io::Result<Directory>| {
            if let Ok(directory) = proto_or {
                if let Some(v1) = directory.v1 {
                    if v1.header_fprint == Some(header_fprint.into()) {
                        return Some(v1);
                    }
                }
            }

            None
        };

        // If the ready directory is already up to date, there's
        // nothing to do.
        if up_to_date(buf.read_ready_directory(&self.path)).is_some() {
            return Ok(());
        }

        buf.ensure_staging_dir();

        // Find the list of chunk fingerprints we care about, either
        // by parsing an up-to-date "staged" directory file, or
        // creating one.
        let chunks = if let Some(directory) = up_to_date(buf.read_staged_directory(&self.path)) {
            rebuild_chunk_fprints(&directory.chunks)
        } else {
            let (len, chunks) = self
                .snapshot_chunks(&buf)
                .map_err(|_| "failed to snapshot chunks")?;

            let flattened = flatten_chunk_fprints(&chunks);
            let directory_fprint = fingerprint_v1_chunk_list(&flattened);
            let directory = Directory {
                v1: Some(DirectoryV1 {
                    header_fprint: Some(header_fprint.into()),
                    contents_fprint: Some(directory_fprint.into()),
                    chunks: flattened,
                    len,
                }),
            };

            buf.publish_directory(&self.path, &directory)
                .map_err(|_| "failed to publish directory file")?;

            chunks
        };

        // If the ready directory still isn't up to date, make it so.
        if up_to_date(buf.read_ready_directory(&self.path)).is_none() {
            let ready = buf
                .prepare_ready_buffer(&chunks, &self.replication_targets)
                .map_err(|_| "failed to prepare ready buffer")?;

            if let Some(failed_ready) = buf.publish_ready_buffer_fast(ready) {
                if up_to_date(buf.read_ready_directory(&self.path)).is_none() {
                    buf.publish_ready_buffer_slow(failed_ready)
                        .map_err(|_| "failed to update ready buffer")?;
                }
            }

            // The buffer is newly ready and updated.  Tell the copier.
            buf.signal_copier(&self.copier);

            // GC is opportunistic, failure is OK.
            let _ = buf.gc_chunks(&chunks);
            // There is no further work to do while the sqlite read
            // lock is held; any temporary file or directory that's
            // still in flight either was left behind by a crashed
            // process, or belongs to a thread that will soon discover
            // it has no work to do.
            //
            // Either way, we can delete everything; clean up is
            // also opportunistic, so failure is OK.
            let _ = buf.cleanup_scratch_directory();
        }

        #[cfg(feature = "verneuil_test_vfs")]
        self.compare_snapshot(&buf).expect("snapshots must match");
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
