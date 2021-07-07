//! A `Tracker` is responsible for determining the byte ranges that
//! should be synchronised for a given file.

use std::collections::BTreeSet;
use std::ffi::CStr;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use umash::Fingerprint;
use uuid::Uuid;

use crate::copier::Copier;
use crate::directory_schema::clear_version_id;
use crate::directory_schema::extract_version_id;
use crate::directory_schema::fingerprint_file_chunk;
use crate::directory_schema::fingerprint_sqlite_header;
use crate::directory_schema::fingerprint_v1_chunk_list;
use crate::directory_schema::update_version_id;
use crate::directory_schema::Directory;
use crate::directory_schema::DirectoryV1;
use crate::replication_buffer::ReplicationBuffer;
use crate::replication_target::ReplicationTargetList;

/// We snapshot db files in 64KB content-addressed chunks.
const SNAPSHOT_GRANULARITY: u64 = 1 << 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationState {
    Clean,   // No mutation since the last snapshot
    Unknown, // Unknown (we just opened the file)
    Dirty,   // Some mutation since the last snapshot.
}

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

    // We cache up to one v4 uuid, to speed up calls to
    // `update_version_id`.
    cached_uuid: Option<Uuid>,

    // The base offset for all chunks that we know have been mutated
    // since the last snapshot.
    dirty_chunks: BTreeSet<u64>,

    // Do we think the backing (replication source) file is clean or
    // dirty, or do we just not know?
    backing_file_state: MutationState,

    // The version id we observed at the beginning of the write
    // transaction, if any.
    previous_version_id: Vec<u8>,
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
        let replication_targets = crate::replication_target::get_default_replication_targets();
        let copier = Copier::get_global_copier();

        // Let the copier pick up any ready snapshot left behind, e.g,
        // by an older crashed process.
        if let Some(buf) = &buffer {
            // But first, make sure to overwrite the replication config with our own.
            buf.ensure_staging_dir(&replication_targets, /*overwrite_meta=*/ true);
            buf.signal_copier(&copier);
        }

        Ok(Tracker {
            file,
            path,
            buffer,
            copier,
            cached_uuid: Some(Uuid::new_v4()),
            replication_targets,
            dirty_chunks: BTreeSet::new(),
            backing_file_state: MutationState::Unknown,
            previous_version_id: Vec::new(),
        })
    }

    /// Remembers the file's state.  This function is called
    /// immediately after acquiring an exclusive lock on the tracked
    /// file, and lets us remember the file's version before we change
    /// it.
    pub fn note_exclusive_lock(&mut self) {
        if !self.previous_version_id.is_empty() {
            return;
        }

        // Read into `self.previous_version_id` in place.
        let mut buf = Vec::new();
        std::mem::swap(&mut buf, &mut self.previous_version_id);
        self.previous_version_id = extract_version_id(&self.file, None, buf);
    }

    /// Notes that we are about to update the tracked file, and
    /// that bytes in [offset, offset + count) are about to change.
    #[inline]
    pub fn flag_write(&mut self, offset: u64, count: u64) {
        // Attempt to update the version id xattr if this is the
        // first write since our last snapshot.
        if self.backing_file_state != MutationState::Dirty {
            // Any error isn't fatal: we can still use the header
            // fingerprint.
            let _ = update_version_id(&self.file, self.cached_uuid.take());
        }

        self.backing_file_state = MutationState::Dirty;

        if count > 0 {
            let min = offset / SNAPSHOT_GRANULARITY;
            let max = offset.saturating_add(count - 1) / SNAPSHOT_GRANULARITY;

            for chunk_index in min..=max {
                self.dirty_chunks.insert(SNAPSHOT_GRANULARITY * chunk_index);
            }
        }
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
        // If we're snapshotting after a write, we always want to go
        // through the whole process.  We made some changes, let's
        // guarantee we try and publish them.  That's important because,
        // if xattrs are missing, we can sometimes observe an unchanged
        // file header after physical writes to the db file that aren't
        // yet relevant for readers (e.g., after a page cache flush).
        let after_write = self.backing_file_state == MutationState::Dirty;
        let header_fprint = fingerprint_sqlite_header(&self.file).ok_or("invalid db file")?;
        let version_id = extract_version_id(&self.file, Some(header_fprint), Vec::new());

        // If we're doing this opportunistically (not after a write)
        // and the staging directory seems up to date, there's nothing
        // to do.  We don't even have to update "ready": the `Copier`
        // reads from quiescent staging directories.
        //
        // We special-case empty version ids: they never match
        // anything, much like NaNs.
        if !after_write && !version_id.is_empty() {
            if let Ok(directory) = buf.read_staged_directory(&self.path) {
                if matches!(directory.v1, Some(v1) if v1.version_id == version_id) {
                    return Ok(());
                }
            }
        }

        // We don't *have* to overwrite the .metadata file, but we
        // should create it if it's missing: without that file, the
        // copier can't make progress.
        buf.ensure_staging_dir(&self.replication_targets, /*overwrite_meta=*/ false);

        // Publish an updated snapshot, and remember the chunks we
        // care about.
        let (copied, chunks) = {
            let (len, chunks) = self
                .snapshot_chunks(&buf)
                .map_err(|_| "failed to snapshot chunks")?;

            let flattened = flatten_chunk_fprints(&chunks);
            let directory_fprint = fingerprint_v1_chunk_list(&flattened);
            let directory = Directory {
                v1: Some(DirectoryV1 {
                    header_fprint: Some(header_fprint.into()),
                    version_id,
                    contents_fprint: Some(directory_fprint.into()),
                    chunks: flattened,
                    len,
                }),
            };

            buf.publish_directory(&self.path, &directory)
                .map_err(|_| "failed to publish directory file")?;

            (chunks.len(), chunks)
        };

        let mut published = false;
        // If we can publish a new ready directory, try to do so.
        if matches!(buf.read_ready_directory(&self.path), Err(e) if e.kind() == std::io::ErrorKind::NotFound)
        {
            let ready = buf
                .prepare_ready_buffer(&chunks)
                .map_err(|_| "failed to prepare ready buffer")?;

            published = buf.publish_ready_buffer(ready).is_ok();
        }

        #[cfg(feature = "verneuil_test_validate_reads")]
        self.validate_all_snapshots(buf);

        // We did something.  Tell the copier.
        buf.signal_copier(&self.copier);

        // GC is opportunistic, failure is OK.  It's important to
        // the copier that we only remove chunks after attempting
        // to publish the ready buffer.
        if published {
            // If we just published our snapshot to the ready
            // buffer, we can delete all chunks.
            let _ = buf.gc_chunks(&[]);
        } else {
            use rand::Rng;

            let mut rng = rand::thread_rng();

            // If the ready buffer is stale, we can only remove
            // now-useless chunks, with probability slightly
            // greater than copied / chunks.len(): a GC wastes
            // time linear in `chunks.len()` (the time it takes to
            // scan chunks we don't want to delete), so we
            // amortise that with randomised counting.
            //
            // We trigger a gc with low probability even when
            // `copied == 0` to help eventually clear up unused
            // chunks.  The probability is low enough that we can
            // amortise the wasted work as constant overhead for
            // each call to `snapshot_file_contents`.
            if copied >= rng.gen_range(0..=chunks.len()) {
                let _ = buf.gc_chunks(&chunks);
            }
        }

        // There is no further work to do while the sqlite read
        // lock is held; any temporary file or directory that's
        // still in flight either was left behind by a crashed
        // process, or belongs to a thread that will soon discover
        // it has no work to do.
        //
        // Either way, we can delete everything; clean up is
        // also opportunistic, so failure is OK.
        let _ = buf.cleanup_scratch_directory();

        #[cfg(feature = "verneuil_test_validate_writes")]
        self.compare_snapshot(&buf).expect("snapshots must match");
        Ok(())
    }

    /// Publishes a snapshot of `file` in the replication buffer, if
    /// it exists.
    ///
    /// Must be called with a read lock held on the underlying file.
    pub fn snapshot(&mut self) -> Result<(), &'static str> {
        let ret = (|| {
            if let Some(buffer) = &self.buffer {
                #[cfg(feature = "verneuil_test_validate_reads")]
                self.validate_all_snapshots(&buffer);
                // Nothing to do if we know we're clean.
                if self.backing_file_state != MutationState::Clean {
                    return self.snapshot_file_contents(&buffer);
                }
            }

            Ok(())
        })();

        // Always reset our state after a snapshot attempt.
        self.cached_uuid.get_or_insert_with(Uuid::new_v4);
        self.backing_file_state = MutationState::Clean;
        self.previous_version_id.clear();
        self.dirty_chunks.clear();

        // If this attempt failed, force the next one to recompute the
        // state from scratch: we *know* the current replication data
        // is currently out of sync.
        if ret.is_err() {
            let _ = clear_version_id(&self.file);
        }

        ret
    }

    /// Performs test-only checks before a transaction's initial lock
    /// acquisition.
    #[inline]
    pub fn pre_lock_checks(&self) {
        #[cfg(feature = "verneuil_test_validate_reads")]
        if let Some(buffer) = &self.buffer {
            self.validate_all_snapshots(&buffer);
        }
    }
}

/// Returns a zero-filled chunk of `SNAPSHOT_GRANULARITY` bytes.
#[cfg(feature = "verneuil_test_vfs")]
fn zero_filled_chunk() -> &'static [u8] {
    lazy_static::lazy_static! {
        static ref BUF: Vec<u8> = {
            let mut buf = Vec::new();

            buf.resize(SNAPSHOT_GRANULARITY as usize, 0u8);
            buf
        };
    }

    &BUF
}

/// Returns the fingerprint for a zero-filled chunk of `SNAPSHOT_GRANULARITY` bytes.
#[cfg(feature = "verneuil_test_vfs")]
fn fingerprint_for_zero_filled_chunk() -> Fingerprint {
    lazy_static::lazy_static! {
        static ref FPRINT: Fingerprint = fingerprint_file_chunk(zero_filled_chunk());
    }

    // This should not change.
    assert_eq!(
        *FPRINT,
        Fingerprint {
            hash: [8155395758008617606, 2728302605148947890]
        }
    );
    *FPRINT
}

#[cfg(feature = "verneuil_test_vfs")]
impl Tracker {
    /// Fetches the contents of the chunk for `fprint`, or dies
    /// trying.
    fn fetch_chunk_or_die(
        &self,
        buf: &ReplicationBuffer,
        fprint: &Fingerprint,
        from_staging: bool,
    ) -> Vec<u8> {
        let mut contents: Option<Vec<u8>> = None;

        let mut update_contents = |new_contents: Vec<u8>| {
            // If we already know the chunk's contents, the new ones must match.
            if let Some(old) = &contents {
                assert_eq!(old, &new_contents);
                return;
            }

            // The contents of a content-addressed chunk must have the same
            // fingerprint as the chunk's name.
            assert_eq!(fprint, &fingerprint_file_chunk(&new_contents));
            contents = Some(new_contents);
        };

        // Chunks move from staging to ready to replication targets.
        // Reading in the same order guarantees we will not miss
        // a chunk that was deleted after successful replication.
        if from_staging {
            if let Ok(staged) = buf.read_staged_chunk(&fprint) {
                update_contents(staged);
            }
        }

        // If the ready chunk exists, it must match the staged one.
        if let Ok(ready) = buf.read_ready_chunk(&fprint) {
            update_contents(ready);
        }

        for blob_value in crate::copier::fetch_chunk_from_targets(
            &self.replication_targets.replication_targets,
            &crate::replication_buffer::fingerprint_chunk_name(fprint),
        ) {
            if let Some(data) = blob_value.expect("target must be reachable") {
                update_contents(data);
            }
        }

        contents.expect("chunk data must exist")
    }

    /// If the snapshot directory exists, confirms that we can get
    /// every chunk in that snapshot.
    fn validate_snapshot(
        &self,
        buf: &ReplicationBuffer,
        directory_or: std::io::Result<Directory>,
        from_staging: bool,
    ) -> std::io::Result<()> {
        let directory = match directory_or {
            // If the directory file can't be found, assume it was
            // replicated correctly, and checked earlier.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            result => result?,
        };

        let zero_fprint = fingerprint_for_zero_filled_chunk();
        let v1 = directory.v1.expect("v1 must exist");
        let mut len = 0;
        for i in 0..v1.chunks.len() / 2 {
            let fprint = Fingerprint {
                hash: [v1.chunks[2 * i], v1.chunks[2 * i + 1]],
            };

            // Don't bother fetching the zero-filled chunk: we know
            // what it is.
            if fprint == zero_fprint {
                len += zero_filled_chunk().len() as u64;
                continue;
            }

            let contents = self.fetch_chunk_or_die(buf, &fprint, from_staging);
            len += contents.len() as u64;
            if i + 1 < v1.chunks.len() / 2 {
                assert_eq!(contents.len(), SNAPSHOT_GRANULARITY as usize);
            }
        }

        assert_eq!(len, v1.len);
        Ok(())
    }

    /// Assert that the contents of ready and staged snapshots make
    /// sense (if they exist).
    #[cfg(feature = "verneuil_test_validate_reads")]
    fn validate_all_snapshots(&self, buf: &ReplicationBuffer) {
        self.validate_snapshot(buf, buf.read_ready_directory(&self.path), false)
            .expect("ready snapshot must be valid");
        self.validate_snapshot(buf, buf.read_staged_directory(&self.path), true)
            .expect("staged snapshot must be valid");
    }

    /// Attempts to assert that the snapshot's contents match that of
    /// our db file, and that the ready snapshot is valid.
    fn compare_snapshot(&self, buf: &ReplicationBuffer) -> std::io::Result<()> {
        use blake2b_simd::Params;
        use std::os::unix::io::AsRawFd;

        self.validate_snapshot(buf, buf.read_ready_directory(&self.path), false)
            .expect("ready snapshot must be valid");

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

        let zero_fprint = fingerprint_for_zero_filled_chunk();
        let mut len = 0;
        for i in 0..directory.chunks.len() / 2 {
            let fprint = Fingerprint {
                hash: [directory.chunks[2 * i], directory.chunks[2 * i + 1]],
            };

            // Fast-path the zero-filled chunk: some tests create multi-GB
            // sparse DB file.
            if fprint == zero_fprint {
                let chunk = zero_filled_chunk();
                len += chunk.len() as u64;
                hasher.update(chunk);
                continue;
            }

            let contents = self.fetch_chunk_or_die(buf, &fprint, true);
            if i + 1 < directory.chunks.len() / 2 {
                assert_eq!(contents.len(), SNAPSHOT_GRANULARITY as usize);
            }

            len += contents.len() as u64;
            hasher.update(&contents);
        }

        assert_eq!(directory.len, len);
        assert_eq!(expected, hasher.finalize());
        Ok(())
    }
}
