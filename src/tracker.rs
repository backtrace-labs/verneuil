//! A `Tracker` is responsible for determining the byte ranges that
//! should be synchronised for a given file.

use std::collections::BTreeMap;
use std::ffi::CStr;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::instrument;
use umash::Fingerprint;
use uuid::Uuid;

use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::copier::Copier;
use crate::drop_result;
use crate::fresh_error;
use crate::fresh_warn;
use crate::manifest_schema::clear_version_id;
use crate::manifest_schema::extract_version_id;
use crate::manifest_schema::fingerprint_file_chunk;
use crate::manifest_schema::fingerprint_sqlite_header;
use crate::manifest_schema::fingerprint_v1_chunk_list;
use crate::manifest_schema::update_version_id;
use crate::manifest_schema::Manifest;
use crate::manifest_schema::ManifestV1;
use crate::replication_buffer::ReplicationBuffer;
use crate::replication_target::ReplicationTargetList;
use crate::result::Result;

/// We snapshot db files in 64KB content-addressed chunks.
pub(crate) const SNAPSHOT_GRANULARITY: u64 = 1 << 16;

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
    buffer: ReplicationBuffer,
    copier: Copier,
    replication_targets: ReplicationTargetList,

    // Counts the number of chunks (slightly more than that in fact,
    // to account for constant overhead) published by this tracker
    // since its last GC.
    chunk_counter: AtomicU64,

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
        ret.push(Fingerprint::new(flattened[2 * i], flattened[2 * i + 1]));
    }

    Some(ret)
}

impl Tracker {
    /// Attempts to create a fresh tracker for `fd` at `c_path`.
    ///
    /// Returns Ok(None) if there is no replication spooling dir.
    #[instrument(err)]
    pub fn new(c_path: *const c_char, fd: i32) -> Result<Option<Tracker>> {
        use std::os::unix::io::FromRawFd;

        if fd < 0 {
            return Err(fresh_error!("received negative fd", fd));
        }

        let file = ManuallyDrop::new(unsafe { File::from_raw_fd(fd) });
        let string = unsafe { CStr::from_ptr(c_path) }
            .to_str()
            .map_err(|e| chain_error!(e, "path is not valid utf-8"))?
            .to_owned();

        let path = std::fs::canonicalize(&string)
            .map_err(|e| chain_error!(e, "failed to canonicalize path", %string))?;

        assert_ne!(
            path.as_os_str().to_str(),
            None,
            "A path generated from a String should be convertible back to a String."
        );

        let buffer = match ReplicationBuffer::new(&path, &file)
            .map_err(|e| chain_error!(e, "failed to create replication buffer", ?path))?
        {
            Some(buf) => buf,
            None => return Ok(None),
        };
        let replication_targets = crate::replication_target::get_default_replication_targets();
        let copier;

        // Let the copier pick up any ready snapshot left behind, e.g,
        // by an older crashed process.

        // But first, make sure to overwrite the replication config with our own.
        buffer.ensure_staging_dir(&replication_targets, /*overwrite_meta=*/ true);
        copier = Copier::get_global_copier().with_spool_path(
            Arc::new(buffer.spooling_directory().to_owned()),
            path.clone(),
        );
        copier.signal_ready_buffer();

        Ok(Some(Tracker {
            file,
            path,
            buffer,
            copier,
            cached_uuid: Some(Uuid::new_v4()),
            replication_targets,
            chunk_counter: AtomicU64::new(0),
            dirty_chunks: BTreeMap::new(),
            backing_file_state: MutationState::Unknown,
            previous_version_id: Vec::new(),
        }))
    }

    /// Remembers the file's state.  This function is called
    /// immediately after acquiring an exclusive lock on the tracked
    /// file, and lets us remember the file's version before we change
    /// it.
    #[instrument(skip(self))]
    pub fn note_exclusive_lock(&mut self) {
        if !self.previous_version_id.is_empty() {
            return;
        }

        // Read into `self.previous_version_id` in place.
        let mut buf = Vec::new();
        std::mem::swap(&mut buf, &mut self.previous_version_id);
        self.previous_version_id = extract_version_id(&self.file, None, buf);
    }

    /// Returns whether the sqlite db definitely has a hot rollback
    /// journal: when sqlite releases the exclusive/reserved lock on a
    /// db while the db still has a rollback journal, the write
    /// transaction has failed, and we don't want to replicate the
    /// current state of the db.  A temporary mismatch (until the
    /// rollback goes through) is fine.
    ///
    /// This should only happen in extreme failure conditions.
    pub fn has_hot_journal(&self) -> bool {
        use std::io::ErrorKind;
        use std::io::Read;

        // https://www.sqlite.org/fileformat.html#:~:text=3.%20The%20Rollback%20Journal
        // The rollback journal file is always located in the same
        // directory as the database file and has the same name as the
        // database file but with the string "-journal" appended.
        const JOURNAL_SUFFIX: &str = "-journal";
        let mut path = self.path.clone();
        let mut name = match path.file_name() {
            Some(name) => name.to_owned(),
            None => return false,
        };

        path.pop();
        name.push(JOURNAL_SUFFIX);
        path.push(name);

        let mut file = match File::open(&path) {
            Ok(file) => file,
            // If the file doesn't exist, there's no journal
            Err(e) if e.kind() == ErrorKind::NotFound => return false,
            Err(e) => {
                // If we failed to open the journal file we don't
                // *definitely* know whether there is a hot journal.
                chain_warn!(e, "failed to open journal path", ?path);
                return false;
            }
        };

        // Read the first 8 bytes of the potential journal file.  If
        // there are at least 8 such bytes and they match the journal
        // header string, we have what looks like a valid journal.
        //
        // https://www.sqlite.org/fileformat.html#:~:text=A%20valid%20rollback%20journal%20begins%20with%20a%20header%20in%20the%20following%20format%3A
        //
        // Offset 0, Size 8: Header string: 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7
        const SQLITE_JOURNAL_HEADER: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];
        let mut buf = [0u8; SQLITE_JOURNAL_HEADER.len()];

        match file.read_exact(&mut buf) {
            // If the header matches, we most likely have a valid hot
            // journal.
            Ok(()) => buf == SQLITE_JOURNAL_HEADER,
            // If the file is too short (likely truncated to 0), it's
            // not a hot journal.
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => false,
            Err(e) => {
                // Any other error means we don't know for sure
                // whether the journal is hot.
                chain_warn!(e, "failed to read journal file", ?path);
                false
            }
        }
    }

    /// Notes that we are about to update the tracked file, and
    /// that bytes in [offset, offset + count) are about to change.
    #[instrument(level = "trace", skip(self))]
    pub fn flag_write(&mut self, buf: *const u8, offset: u64, count: u64) {
        // Attempt to update the version id xattr if this is the
        // first write since our last snapshot.
        if self.backing_file_state != MutationState::Dirty {
            // Any error isn't fatal: we can still use the header
            // fingerprint.
            drop_result!(update_version_id(&self.file, self.cached_uuid.take()),
                         e => chain_warn!(e, "failed to update version id", path=?self.path));
        }

        self.backing_file_state = MutationState::Dirty;

        if !buf.is_null() && count == SNAPSHOT_GRANULARITY && (offset % SNAPSHOT_GRANULARITY) == 0 {
            // When sqlite fires off a writes that's exactly
            // chunk-aligned, stage it directly for replication.  We
            // expect this to happen most of the time, when the DB is
            // configured with 64 KB pages.
            let slice = unsafe { std::slice::from_raw_parts(buf, count as usize) };
            let value = {
                let fprint = fingerprint_file_chunk(slice);

                // Remember the chunk's fingerprint if it's now staged.
                match self.buffer.stage_chunk(fprint, slice) {
                    Ok(_) => Some(fprint),
                    Err(e) => {
                        let _ = chain_warn!(e, "failed to stage chunk preemptively",
                                            path=?self.path, count, offset, ?fprint);
                        None
                    }
                }
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
    #[instrument(skip(self, base), err)]
    fn snapshot_chunks(
        &self,
        base: Option<Vec<Fingerprint>>,
    ) -> Result<(u64, Vec<Fingerprint>, usize)> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let repl = &self.buffer;
        let len = self
            .file
            .metadata()
            .map_err(|e| chain_error!(e, "failed to stat file", ?self.path))?
            .len();
        let num_chunks = len / SNAPSHOT_GRANULARITY
            + (if (len % SNAPSHOT_GRANULARITY) > 0 {
                1
            } else {
                0
            });
        // Always copy everything in [backfill_begin, num_chunks),
        // in addition to any known dirty chunk.
        let mut backfill_begin: u64;

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
        chunk_fprints.resize(num_chunks as usize, Fingerprint::new(0, 0));

        // Box this allocation to avoid a 64KB stack allocation.
        let mut buf = Box::new([0u8; SNAPSHOT_GRANULARITY as usize]);

        let mut num_snapshotted: usize = 0;

        // Updates the snapshot to take into account the data in chunk
        // `chunk_index`.
        //
        // If `expected_fprint` is provided, the corresponding chunk
        // file must have already been staged.
        //
        // Returns true if the new chunk fprint differs from the old one.
        let update = &mut |chunk_index, expected_fprint| -> Result<bool> {
            num_snapshotted += 1;

            let begin = chunk_index * SNAPSHOT_GRANULARITY;
            let end = if (len - begin) > SNAPSHOT_GRANULARITY {
                begin + SNAPSHOT_GRANULARITY
            } else {
                len
            };

            let slice = &mut buf[0..(end - begin) as usize];
            self.file.read_exact_at(slice, begin).map_err(
                |e| chain_error!(e, "failed to read chunk", path=?self.path, begin, end),
            )?;

            let fprint = fingerprint_file_chunk(slice);
            if let Some(expected) = expected_fprint {
                // Our incremental change tracking should work.
                #[cfg(feature = "test_validate_writes")]
                assert_eq!(fprint, expected);

                chunk_fprints[chunk_index as usize] = expected;
            }

            if fprint == chunk_fprints[chunk_index as usize] {
                // Nothing to do, it's all clean
                Ok(false)
            } else {
                // Only stage the chunk if it has changed: we don't
                // want our background scans to create useless copy
                // work.
                repl.stage_chunk(fprint, slice)?;
                chunk_fprints[chunk_index as usize] = fprint;
                Ok(true)
            }
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

            if update(chunk_index, *expected_fprint)? && expected_fprint.is_some() {
                // `update` found a mismatch, our metadata said we
                // already knew what was there.  Trigger a full
                // rescan.
                tracing::error!(path=?self.path, chunk_index,
                                "forcing resynchronisation scan");
                backfill_begin = 0;
                break;
            }

            // Now do the same for a random chunk index, as a
            // background scan for any desynchronisation we might
            // have missed.
            //
            // This is optional, so don't do it in tests: we don't
            // want to randomly paper over test failures.
            let random_index = rng.gen_range(0..backfill_begin);
            if cfg!(not(feature = "test_vfs"))
                && !self
                    .dirty_chunks
                    .contains_key(&(random_index * SNAPSHOT_GRANULARITY))
            {
                // We don't *have* to get these additional chunks, so
                // we don't want to bubble up errors.
                //
                // However, if we can't confirm that the chunk is
                // clean, force a full scan.
                let result = update(random_index, None).map_err(|e| {
                    chain_error!(e, "failed to scrub random clean chunk",
                                 path=?self.path, random_index)
                });
                if !matches!(result, Ok(false)) {
                    tracing::error!(path=?self.path, random_index, ?result,
                                    "forcing resynchronisation scan");
                    backfill_begin = 0;
                }
            }
        }

        for chunk_index in backfill_begin..num_chunks {
            update(chunk_index, None)?;
        }

        Ok((len, chunk_fprints, num_snapshotted))
    }

    /// Reads the current staged manifest, if any.
    fn read_current_manifest(&self) -> Result<Option<Manifest>> {
        let buf = &self.buffer;
        let mut builder = kismet_cache::CacheBuilder::new();

        // We assume any chunk needed to parse the manifest is always
        // available in the staged chunk directory, and make sure to
        // save such chunks during GC.
        //
        // We do that instead of using the test-only
        // `cache_builder_for_source` because the latter does a lot
        // more work in order to look into `ready` and `consuming`
        // subdirectories for data chunk.  However, we (should) always
        // keep the current base chunk in `staged`, so that extra work
        // is useless and could hide logic bugs.
        builder.plain_reader(buf.staged_chunk_directory());
        buf.read_staged_manifest(
            &self.path,
            builder,
            &self.replication_targets.replication_targets,
        )
    }

    /// Snapshots the contents of the tracked file to its replication
    /// buffer.  Concurrent threads or processes may be doing the same,
    /// but the contents of the file can't change, since we still hold
    /// a sqlite read lock on the db file.
    #[instrument(skip(self), err)]
    fn snapshot_file_contents(&self) -> Result<()> {
        use std::os::unix::fs::MetadataExt;

        let buf = &self.buffer;
        let header_fprint = match fingerprint_sqlite_header(&self.file) {
            Some(fprint) => fprint,
            None => {
                if let Ok(meta) = self.file.metadata() {
                    if meta.len() == 0 {
                        // If the file is empty, the failure is
                        // benign.  Make sure the next snapshot
                        // definitely starts from scratch.
                        clear_version_id(&self.file).map_err(|e| {
                            chain_error!(e, "failed to clear version xattr on empty db file")
                        })?;
                        return Ok(());
                    }
                }

                return Err(fresh_warn!("invalid db file", path=?self.path));
            }
        };

        let version_id = extract_version_id(&self.file, Some(header_fprint), Vec::new());
        // If we can't find any version id, try to set one for the next run.
        if version_id.is_empty() {
            drop_result!(update_version_id(&self.file, None),
                         e => chain_warn!(e, "failed to force populate version xattr", path=?self.path));
        }

        let mut current_manifest: Option<Manifest> = self
            .read_current_manifest()
            .map_err(|e| chain_info!(e, "failed to read staged manifest file"))
            .ok()
            .flatten();

        // If we're snapshotting after a write, we always want to go
        // through the whole process.  We made some changes, let's
        // guarantee we try and publish them.  That's important because,
        // if xattrs are missing, we can sometimes observe an unchanged
        // file header after physical writes to the db file that aren't
        // yet relevant for readers (e.g., after a page cache flush).
        //
        // We must also figure out whether we trust `current_manifest`
        // enough to build our snapshot as a diff on top of that file.
        if self.backing_file_state == MutationState::Dirty {
            let mut up_to_date = false;

            if let Some(manifest) = &current_manifest {
                if let Some(v1) = &manifest.v1 {
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
            if self.previous_version_id.is_empty() || version_id.is_empty() {
                // Empty version ids are expected: that's what happens
                // when we create a new db, and when we force a full
                // snapshot.
                tracing::debug!(path=?self.path, ?self.previous_version_id,
                               ?version_id, num_dirty=self.dirty_chunks.len(),
                               "forcing a full snapshot due to empty version ids");
                up_to_date = false;
            } else if self.previous_version_id == version_id || self.dirty_chunks.is_empty() {
                tracing::info!(path=?self.path, ?self.previous_version_id,
                               ?version_id, num_dirty=self.dirty_chunks.len(),
                               "forcing a full snapshot due to invalid version ids");
                up_to_date = false;
            }

            // If the manifest isn't up to date, we can't use it.
            if !up_to_date {
                current_manifest = None;
            }
        } else {
            // If we're doing this opportunistically (not after a write)
            // and the staging manifest seems up to date, there's nothing
            // to do.  We don't even have to update "ready": the `Copier`
            // will read from the staging directory when we don't change it.
            if !version_id.is_empty() {
                if let Some(manifest) = &current_manifest {
                    if matches!(&manifest.v1, Some(v1) if v1.version_id == version_id) {
                        return Ok(());
                    }
                }
            }

            // If we think there's work to do after a read
            // transaction, assume the worst, and rebuild
            // the snapshot from scratch.
            current_manifest = None;
        }

        // We don't *have* to overwrite the .metadata file, but we
        // should create it if it's missing: without that file, the
        // copier can't make progress.
        buf.ensure_staging_dir(&self.replication_targets, /*overwrite_meta=*/ false);

        // Publish an updated snapshot, and remember the chunks we
        // care about.

        // Try to get an initial list of chunks to work off.
        let mut base_fprints = None;
        if let Some(manifest) = current_manifest {
            if let Some(v1) = manifest.v1 {
                base_fprints = rebuild_chunk_fprints(&v1.chunks);
            }
        }

        let (copied, chunks) = {
            let (len, chunks, copied) = self.snapshot_chunks(base_fprints)?;

            let (ctime, ctime_ns) = match self.file.metadata() {
                Ok(meta) => (meta.ctime(), meta.ctime_nsec() as i32),
                Err(e) => {
                    let _ = chain_warn!(e, "failed to fetch file metadata", ?self.path);
                    (0, 0)
                }
            };

            let flattened = flatten_chunk_fprints(&chunks);
            let manifest_fprint = fingerprint_v1_chunk_list(&flattened);
            let manifest = Manifest {
                v1: Some(ManifestV1 {
                    header_fprint: Some(header_fprint.into()),
                    version_id,
                    contents_fprint: Some(manifest_fprint.into()),
                    len,
                    ctime,
                    ctime_ns,
                    base_chunks_fprint: None,
                    chunks: flattened,
                }),
            };

            buf.publish_manifest(&self.path, &manifest)?;

            (copied, chunks)
        };

        let mut published = false;
        // Unless there obviously is a ready manifest, try to publish our own.
        if !buf.has_ready_manifest(&self.path) {
            let ready = buf.prepare_ready_buffer(&chunks)?;

            published = buf
                .publish_ready_buffer(ready)
                .map_err(|e| chain_info!(e, "failed to publish ready buffer", path=?self.path))
                .is_ok();
        }

        #[cfg(feature = "test_validate_reads")]
        self.validate_all_snapshots();

        // We did something.  Tell the copier.
        self.copier.signal_ready_buffer();

        // GC is opportunistic, failure is OK.  It's important to
        // the copier that we only remove chunks after attempting
        // to publish the ready buffer.
        if published {
            // If we just published our snapshot to the ready
            // buffer, we can delete all chunks.
            drop_result!(buf.gc_chunks(&[]),
                         e => chain_info!(e, "failed to clear all staged chunks", path=?self.path));
            self.chunk_counter.store(0, Ordering::Relaxed);
        } else {
            use rand::Rng;

            let mut rng = rand::thread_rng();
            // Always increment the chunk counter by at least one:
            // we did *some* work here.
            let work = (copied as u64).saturating_add(1);
            let total_count = self
                .chunk_counter
                .fetch_add(work, Ordering::Relaxed)
                .saturating_add(work);

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
            //
            // We also trigger based on a deterministic counter, but
            // hopefully much more rarely than via the random
            // criterion: we want to avoid really long delays between
            // GCs, however unlikely they may be.
            if copied >= rng.gen_range(0..=chunks.len() / 4)
                || total_count / 2 >= chunks.len() as u64
            {
                drop_result!(buf.gc_chunks(&chunks),
                             e => chain_info!(e, "failed to gc staged chunks", path=?self.path));
                self.chunk_counter.store(0, Ordering::Relaxed);
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
        drop_result!(buf.cleanup_scratch_directory(),
                     e => chain_info!(e, "failed to clear scratch directory", path=?self.path));

        #[cfg(feature = "test_validate_writes")]
        self.compare_snapshot().expect("snapshots must match");
        Ok(())
    }

    /// Publishes a snapshot of `file` in the replication buffer, if
    /// it exists.
    ///
    /// Must be called with a read lock held on the underlying file.
    #[instrument(skip(self), err)]
    pub fn snapshot(&mut self) -> Result<()> {
        let ret = (|| {
            #[cfg(feature = "test_validate_reads")]
            self.validate_all_snapshots();
            // Nothing to do if we know we're clean.
            if self.backing_file_state == MutationState::Clean {
                return Ok(());
            }

            // We also don't want to replicate if there's a hot
            // journal: a hot journal can only be present when sqlite
            // releases its lock if the lock protected a failed
            // transaction that will be rolled back.
            if self.has_hot_journal() {
                // Return an error to make sure we force a full snapshot
                // for the next transaction on the db file.
                return Err(fresh_warn!(
                    "found hot journal while releasing write lock",
                    path=?self.path, ?self.backing_file_state));
            }

            let _span = tracing::info_span!(
                "snapshot",
                path=?self.path, ?self.backing_file_state);
            self.snapshot_file_contents()
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
            drop_result!(clear_version_id(&self.file),
                         e => chain_error!(e, "failed to clear version xattr after failed snapshot",
                                           snapshot_err=?ret));
        }

        ret
    }

    /// Performs test-only checks before a transaction's initial lock
    /// acquisition.
    #[inline]
    #[instrument(skip(self))]
    pub fn pre_lock_checks(&self) {
        #[cfg(feature = "test_validate_reads")]
        self.validate_all_snapshots();
    }
}

#[cfg(feature = "test_vfs")]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ChunkSource {
    Consuming = 0,
    Ready = 1,
    Staged = 2,
}

#[cfg(feature = "test_vfs")]
impl Tracker {
    fn cache_builder_for_source(&self, source: ChunkSource) -> kismet_cache::CacheBuilder {
        let buf = &self.buffer;
        let mut cache_builder = kismet_cache::CacheBuilder::new();

        if source >= ChunkSource::Staged {
            cache_builder.plain_reader(buf.staged_chunk_directory());
        }

        if source >= ChunkSource::Ready {
            let mut ready = buf.ready_chunk_directory();

            cache_builder.plain_reader(&ready);

            // Also derive what the path will become once it's moved
            // to `consuming`.
            //
            // That's a bit of an abstraction violation, but this is
            // test-only code.
            let pseudo_unique = ready
                .file_name()
                .expect("must have pseudo-unique")
                .to_owned();
            ready.pop();
            let chunks = ready.file_name().expect("must have `chunks`").to_owned();
            ready.pop();
            ready.pop();

            ready.push("consuming");
            ready.push(chunks);
            ready.push(pseudo_unique);

            cache_builder.plain_reader(&ready);
        }

        if source >= ChunkSource::Consuming {
            cache_builder.plain_reader(buf.consuming_chunk_directory());
        }

        cache_builder
    }

    fn fetch_snapshot_or_die(
        &self,
        manifest: &Manifest,
        source: ChunkSource,
    ) -> crate::snapshot::Snapshot {
        crate::snapshot::Snapshot::new(
            self.cache_builder_for_source(source),
            &self.replication_targets.replication_targets,
            manifest,
        )
        .expect("failed to instantiate snapshot")
    }

    /// If the snapshot manifest exists, confirms that we can get
    /// every chunk in that snapshot.
    fn validate_snapshot(
        &self,
        manifest_or: crate::result::Result<Option<Manifest>>,
        source: ChunkSource,
    ) -> Result<()> {
        let manifest = match manifest_or {
            // If the manifest file can't be found, assume it was
            // replicated correctly, and checked earlier.
            Ok(None) => return Ok(()),
            Ok(Some(manifest)) => manifest,
            Err(err) => return Err(err),
        };

        self.fetch_snapshot_or_die(&manifest, source);
        Ok(())
    }

    /// Assert that the contents of ready and staged snapshots make
    /// sense (if they exist).
    #[cfg(feature = "test_validate_reads")]
    fn validate_all_snapshots(&self) {
        let buf = &self.buffer;
        let targets = &self.replication_targets.replication_targets;

        self.validate_snapshot(
            buf.read_consuming_manifest(
                &self.path,
                self.cache_builder_for_source(ChunkSource::Consuming),
                targets,
            ),
            ChunkSource::Consuming,
        )
        .expect("consuming snapshot must be valid");
        self.validate_snapshot(
            buf.read_ready_manifest(
                &self.path,
                self.cache_builder_for_source(ChunkSource::Ready),
                targets,
            ),
            ChunkSource::Ready,
        )
        .expect("ready snapshot must be valid");

        self.validate_snapshot(self.read_current_manifest(), ChunkSource::Staged)
            .expect("staged snapshot must be valid");
    }

    /// Attempts to assert that the snapshot's contents match that of
    /// our db file, and that the ready snapshot is valid.
    fn compare_snapshot(&self) -> Result<()> {
        use blake2b_simd::Params;

        let buf = &self.buffer;
        let targets = &self.replication_targets.replication_targets;

        self.validate_snapshot(
            buf.read_consuming_manifest(
                &self.path,
                self.cache_builder_for_source(ChunkSource::Consuming),
                targets,
            ),
            ChunkSource::Consuming,
        )
        .expect("consuming snapshot must be valid");
        self.validate_snapshot(
            buf.read_ready_manifest(
                &self.path,
                self.cache_builder_for_source(ChunkSource::Ready),
                targets,
            ),
            ChunkSource::Ready,
        )
        .expect("ready snapshot must be valid");

        // The VFS layer doesn't do anything with the file's offset,
        // and all locking uses OFD locks, so this `dup(2)` is fine.
        let expected = match self.file.try_clone() {
            // If we can't dup the DB file, this isn't a replication
            // problem.
            Err(_) => return Ok(()),
            Ok(mut file) => {
                use std::io::Seek;
                use std::io::SeekFrom;

                let mut hasher = Params::new().hash_length(32).to_state();
                file.seek(SeekFrom::Start(0)).expect("seek should succeed");
                std::io::copy(&mut file, &mut hasher)
                    .map_err(|e| chain_error!(e, "failed to hash base file", path=?self.path))?;
                hasher.finalize()
            }
        };

        let mut hasher = Params::new().hash_length(32).to_state();
        let manifest = self
            .read_current_manifest()
            .expect("manifest must parse")
            .expect("manifest must exist");

        let manifest_v1 = manifest
            .v1
            .as_ref()
            .expect("v1 component must be populated.");

        // The header fingerprint must match the current header.
        assert_eq!(
            manifest_v1.header_fprint,
            fingerprint_sqlite_header(&self.file).map(|fp| fp.into())
        );

        let snapshot = self.fetch_snapshot_or_die(&manifest, ChunkSource::Staged);
        std::io::copy(&mut snapshot.as_read(0, u64::MAX), &mut hasher).expect("should hash");
        assert_eq!(expected, hasher.finalize());
        Ok(())
    }
}
