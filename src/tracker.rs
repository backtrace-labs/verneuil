//! A `Tracker` is responsible for determining the byte ranges that
//! should be synchronised for a given file.

use std::collections::BTreeMap;
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

    // Set of "base" (SNAPSHOT_GRANULARITY-aligned) offsets for all
    // chunks that that we know have been mutated since the last
    // snapshot.
    //
    // If we have already staged a file for that chunk, the value is
    // that chunk's fingerprint.  If `None`, we have yet to stage a
    // file for that dirty chunk.
    dirty_chunks: BTreeMap<u64, Option<Fingerprint>>,

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

fn rebuild_chunk_fprints(flattened: &[u64]) -> Option<Vec<Fingerprint>> {
    if (flattened.len() % 2) != 0 {
        return None;
    }

    let mut ret = Vec::with_capacity(flattened.len() / 2);

    for i in 0..flattened.len() / 2 {
        ret.push(Fingerprint {
            hash: [flattened[2 * i], flattened[2 * i + 1]],
        });
    }

    Some(ret)
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
        let copier;

        // Let the copier pick up any ready snapshot left behind, e.g,
        // by an older crashed process.
        if let Some(buf) = &buffer {
            // But first, make sure to overwrite the replication config with our own.
            buf.ensure_staging_dir(&replication_targets, /*overwrite_meta=*/ true);
            copier = Copier::get_global_copier(Some(buf.spooling_directory().to_owned()));
            copier.signal_ready_buffer();
        } else {
            copier = Copier::get_global_copier(None)
        }

        Ok(Tracker {
            file,
            path,
            buffer,
            copier,
            cached_uuid: Some(Uuid::new_v4()),
            replication_targets,
            dirty_chunks: BTreeMap::new(),
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
    pub fn flag_write(&mut self, buf: *const u8, offset: u64, count: u64) {
        // Attempt to update the version id xattr if this is the
        // first write since our last snapshot.
        if self.backing_file_state != MutationState::Dirty {
            // Any error isn't fatal: we can still use the header
            // fingerprint.
            let _ = update_version_id(&self.file, self.cached_uuid.take());
        }

        self.backing_file_state = MutationState::Dirty;

        if !buf.is_null() && count == SNAPSHOT_GRANULARITY && (offset % SNAPSHOT_GRANULARITY) == 0 {
            // When sqlite fires off a writes that's exactly
            // chunk-aligned, stage it directly for replication.  We
            // expect this to happen most of the time, when the DB is
            // configured with 64 KB pages.
            let slice = unsafe { std::slice::from_raw_parts(buf, count as usize) };
            let value = if let Some(repl) = &self.buffer {
                let fprint = fingerprint_file_chunk(&slice);

                // Remember the chunk's fingerprint if it's now staged.
                match repl.stage_chunk(fprint, slice) {
                    Ok(_) => Some(fprint),
                    Err(_) => None,
                }
            } else {
                None
            };

            self.dirty_chunks.insert(offset, value);
        } else if count > 0 {
            let min = offset / SNAPSHOT_GRANULARITY;
            let max = offset.saturating_add(count - 1) / SNAPSHOT_GRANULARITY;

            for chunk_index in min..=max {
                self.dirty_chunks
                    .insert(SNAPSHOT_GRANULARITY * chunk_index, None);
            }
        }
    }

    /// Snapshots all the 64KB chunks in the tracked file, and returns
    /// the file's size as well, a list of chunk fingerprints, and the
    /// number of chunks that were actually snapshotted.
    fn snapshot_chunks(
        &self,
        repl: &ReplicationBuffer,
        mut base: Option<Vec<Fingerprint>>,
    ) -> std::io::Result<(u64, Vec<Fingerprint>, usize)> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let len = self.file.metadata()?.len();
        let num_chunks = len / SNAPSHOT_GRANULARITY
            + (if (len % SNAPSHOT_GRANULARITY) > 0 {
                1
            } else {
                0
            });
        // Always copy everything in [backfill_begin, num_chunks),
        // in addition to any known dirty chunk.
        let mut backfill_begin: u64;

        // We expect to copy ~2 chunks per offset in
        // `self.dirty_chunks`.  Pseudorandomly compute a full
        // snapshot with probability `self.dirty_chunks.len() /
        // num_chunks`; that still averages to a constant number of
        // copies per dirty chunks.
        //
        // Computing a full snapshot from time to time helps us
        // recover from silent desynchronisation.  That's good for
        // robustness in production, but we should disable that in
        // tests to catch logic bugs.
        if cfg!(not(feature = "verneuil_test_vfs"))
            && (self.dirty_chunks.len() as u64) >= rng.gen_range(0..=num_chunks / 2)
        {
            base = None;
        }

        // Setup the initial chunk fingerprint vector.
        let mut chunk_fprints = if let Some(fprints) = base {
            // If we're doing incremental snapshotting and the file
            // has grown, make sure to also diff what used to be the
            // last chunk: it can grow implicitly after a write.
            //
            // The same thing could also happen if we wrote to an
            // offset >= the last chunk's first byte.
            //
            // When we observe either situation, we ignore the last
            // chunk in `fprints`.
            let grown = (fprints.len() as u64) < num_chunks;
            let wrote_past_end = self
                .dirty_chunks
                .range(fprints.len() as u64 * SNAPSHOT_GRANULARITY..=u64::MAX)
                .next()
                .is_some();
            let delta = (grown || wrote_past_end) as u64;

            // We definitely don't know anything about what's at or
            // after chunk index `fprints.len()`.  We also don't
            // want to go out of bounds if the new file shrunk.
            backfill_begin = (fprints.len() as u64)
                .clamp(0, num_chunks)
                .saturating_sub(delta);
            fprints
        } else {
            backfill_begin = 0;
            Vec::with_capacity(num_chunks as usize)
        };

        // And make sure list's size matches the file's.
        chunk_fprints.resize(num_chunks as usize, Fingerprint { hash: [0, 0] });

        // Box this allocation to avoid a 64KB stack allocation.
        let mut buf = Box::new([0u8; SNAPSHOT_GRANULARITY as usize]);

        let mut num_snapshotted: usize = 0;

        // Updates the snapshot to take into account the data in chunk
        // `chunk_index`.
        //
        // Returns true if the new chunk fprint differs from the old one.
        let update = &mut |chunk_index, expected_fprint| -> std::io::Result<bool> {
            num_snapshotted += 1;

            // Fast-path in non-test mode.  In test mode, we always
            // go through every step to confirm that the expected
            // fingerprint matches the actual chunk fingerprint.
            #[cfg(not(feature = "verneuil_test_validate_writes"))]
            if let Some(fprint) = expected_fprint {
                // We had the write lock when we preemptively wrote the
                // chunk file.  No other process could have GCed chunks
                // until we released the write lock... at which time the
                // chunk was clearly useful.
                //
                // The chunk file might be missing, but, if so, that's
                // because the copier has already uploaded it.
                let ret = fprint != chunk_fprints[chunk_index as usize];
                chunk_fprints[chunk_index as usize] = fprint;
                return Ok(ret);
            }

            let begin = chunk_index * SNAPSHOT_GRANULARITY;
            let end = if (len - begin) > SNAPSHOT_GRANULARITY {
                begin + SNAPSHOT_GRANULARITY
            } else {
                len
            };

            let slice = &mut buf[0..(end - begin) as usize];
            self.file.read_exact_at(slice, begin)?;

            let fprint = fingerprint_file_chunk(slice);

            #[cfg(feature = "verneuil_test_validate_writes")]
            if let Some(expected) = expected_fprint {
                assert_eq!(fprint, expected);
            }

            repl.stage_chunk(fprint, slice)?;
            let ret = fprint != chunk_fprints[chunk_index as usize];
            chunk_fprints[chunk_index as usize] = fprint;
            Ok(ret)
        };

        for (base, expected_fprint) in &self.dirty_chunks {
            let chunk_index = base / SNAPSHOT_GRANULARITY;

            // Everything greater than or equal to `backfill_begin`
            // will be handled by the loop below.  This avoids
            // snapshotting chunks twice after growing a db file.
            if chunk_index >= backfill_begin {
                // Dirty chunks is a sorted map.  We're not going
                // to do anything with the remaining entries.
                break;
            }

            update(chunk_index, *expected_fprint)?;

            // Now do the same for a random chunk index, as a
            // background scan for any desynchronisation we might
            // have missed.
            //
            // This is optional, so don't do it in tests: we don't
            // want to randomly paper over test failures.
            let random_index = rng.gen_range(0..backfill_begin);
            if cfg!(not(feature = "verneuil_test_vfs"))
                && !self
                    .dirty_chunks
                    .contains_key(&(random_index * SNAPSHOT_GRANULARITY))
            {
                // We don't *have* to get these additional chunks, so
                // we don't want to bubble up errors.
                //
                // However, if we successfully find out that the new
                // chunk is actually dirty, force a full scan.
                if let Ok(true) = update(random_index, None) {
                    backfill_begin = 0;
                }
            }
        }

        for chunk_index in backfill_begin..num_chunks {
            update(chunk_index, None)?;
        }

        Ok((len, chunk_fprints, num_snapshotted))
    }

    /// Snapshots the contents of the tracked file to its replication
    /// buffer.  Concurrent threads or processes may be doing the same,
    /// but the contents of the file can't change, since we still hold
    /// a sqlite read lock on the db file.
    fn snapshot_file_contents(&self, buf: &ReplicationBuffer) -> Result<(), &'static str> {
        let header_fprint = fingerprint_sqlite_header(&self.file).ok_or("invalid db file")?;
        let version_id = extract_version_id(&self.file, Some(header_fprint), Vec::new());

        let mut current_directory: Option<Directory> =
            buf.read_staged_directory(&self.path).ok().flatten();

        // If we're snapshotting after a write, we always want to go
        // through the whole process.  We made some changes, let's
        // guarantee we try and publish them.  That's important because,
        // if xattrs are missing, we can sometimes observe an unchanged
        // file header after physical writes to the db file that aren't
        // yet relevant for readers (e.g., after a page cache flush).
        //
        // We must also figure out whether we trust `current_directory`
        // enough to build our snapshot as a diff on top of that file.
        if self.backing_file_state == MutationState::Dirty {
            let mut up_to_date = false;

            if let Some(directory) = &current_directory {
                if let Some(v1) = &directory.v1 {
                    // The current snapshot seems to have everything
                    // up to our last write transaction.  We can use that!
                    //
                    // Remember to special-case empty version strings: they
                    // never match anything, much like NaNs.
                    if !self.previous_version_id.is_empty()
                        && v1.version_id == self.previous_version_id
                    {
                        up_to_date = true;
                    }

                    // The current snapshot seems to also include
                    // write transaction.  Use it as a base, but
                    // don't bail out, just in case this is a
                    // spurious match.
                    if !version_id.is_empty() && v1.version_id == version_id {
                        up_to_date = true;
                    }
                }
            }

            // If the version ids are empty, something's wrong with
            // xattrs.  If we know we wrote something, but the
            // versions match, our id system must be wrong (probably
            // because xattrs don't work).  Finally, if we know
            // we wrote something but our list of dirty chunks is
            // empty, we probably missed an update.
            //
            // In all cases, we want to force a full snapshot.
            if self.previous_version_id.is_empty()
                || version_id.is_empty()
                || self.previous_version_id == version_id
                || self.dirty_chunks.is_empty()
            {
                // TODO: log here.
                up_to_date = false;
            }

            // If the directory isn't up to date, we can't use it.
            if !up_to_date {
                current_directory = None;
            }
        } else {
            // If we're doing this opportunistically (not after a write)
            // and the staging directory seems up to date, there's nothing
            // to do.  We don't even have to update "ready": the `Copier`
            // will read from the staging directory when we don't change it.
            if !version_id.is_empty() {
                if let Some(directory) = &current_directory {
                    if matches!(&directory.v1, Some(v1) if v1.version_id == version_id) {
                        return Ok(());
                    }
                }
            }

            // If we think there's work to do after a read
            // transaction, assume the worst, and rebuild
            // the snapshot from scratch.
            current_directory = None;
        }

        // We don't *have* to overwrite the .metadata file, but we
        // should create it if it's missing: without that file, the
        // copier can't make progress.
        buf.ensure_staging_dir(&self.replication_targets, /*overwrite_meta=*/ false);

        // Publish an updated snapshot, and remember the chunks we
        // care about.

        // Try to get an initial list of chunks to work off.
        let mut base_fprints = None;
        if let Some(directory) = current_directory {
            if let Some(v1) = directory.v1 {
                base_fprints = rebuild_chunk_fprints(&v1.chunks);
            }
        }

        let (copied, chunks) = {
            let (len, chunks, copied) = self
                .snapshot_chunks(&buf, base_fprints)
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

            (copied, chunks)
        };

        let mut published = false;
        // If we can publish a new ready directory, try to do so.
        if matches!(buf.read_ready_directory(&self.path), Ok(None)) {
            let ready = buf
                .prepare_ready_buffer(&chunks)
                .map_err(|_| "failed to prepare ready buffer")?;

            published = buf.publish_ready_buffer(ready).is_ok();
        }

        #[cfg(feature = "verneuil_test_validate_reads")]
        self.validate_all_snapshots(buf);

        // We did something.  Tell the copier.
        self.copier.signal_ready_buffer();

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
        directory_or: std::io::Result<Option<Directory>>,
        from_staging: bool,
    ) -> std::io::Result<()> {
        let directory = match directory_or {
            // If the directory file can't be found, assume it was
            // replicated correctly, and checked earlier.
            Ok(None) => return Ok(()),
            Ok(Some(directory)) => directory,
            Err(err) => return Err(err),
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
            .expect("directory must exist")
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
