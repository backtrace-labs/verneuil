//! A `Tracker` is responsible for determining the byte ranges that
//! should be synchronised for a given file.
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::fs::File;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::instrument;
use umash::Fingerprint;
use uuid::Uuid;

use crate::chain_error;
use crate::chain_warn;
use crate::copier::Copier;
use crate::drop_result;
use crate::fresh_error;
use crate::fresh_warn;
use crate::manifest_schema::clear_version_id;
use crate::manifest_schema::extract_version_id;
use crate::manifest_schema::fingerprint_file_chunk;
use crate::manifest_schema::update_version_id;
use crate::replication_buffer::ReplicationBuffer;
use crate::replication_target::ReplicationTargetList;
use crate::result::Result;

#[cfg(feature = "test_vfs")]
mod invariants;
mod snapshot_file_contents;

/// We snapshot db files in 64KB content-addressed chunks.
pub(crate) const SNAPSHOT_GRANULARITY: u64 = 1 << 16;

/// Don't generate a base fingerprint chunk for a list of fingerprints
/// shorter than `BASE_CHUNK_MIN_LENGTH`.
///
/// Production experience shows the base chunk system is effective in
/// practice.  Enabling that system for db files than span 600 chunks
/// means that, in the worst case (a series of write transactions that
/// each change one chunk, without repetition), we can expect to
/// generate 5.5% more chunks (for base fingerprint lists) than the
/// changed chunks themselves.  That seems acceptable.
const BASE_CHUNK_MIN_LENGTH: usize = 600;

/// Always bundle chunks at these offsets with the manifest.
///
/// Offset 0 is special because it contains sqlite's 0 page, and that
/// page contains a small header that's modified after every write
/// transaction.  There's no point trying to deduplicate it, it's
/// essentially never the same.
const BUNDLED_CHUNK_OFFSETS: [u64; 1] = [0];

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

    // A shared mutable cell to retain the most recent base chunk for
    // this db's manifest.  Keeping this chunk alive guarantees we can
    // find it in the global cache, and thus avoids useless GETs.
    recent_base_chunk: Option<Arc<crate::loader::Chunk>>,
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

        // Let the copier pick up any ready snapshot left behind, e.g,
        // by an older crashed process.
        //
        // But first, make sure to overwrite the replication config with our own.
        buffer.ensure_staging_dir(&replication_targets, /*overwrite_meta=*/ true);
        let copier = Copier::get_global_copier().with_spool_path(
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
            recent_base_chunk: None,
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

    /// Determines whether the chunk data at `offset` byte in the file
    /// should be bundled with the manifest, rather than published as
    /// a standalone content-addressed chunk.
    fn should_bundle_chunk_at_offset(&self, offset: u64) -> bool {
        BUNDLED_CHUNK_OFFSETS.contains(&offset)
    }

    /// Returns a vector of offsets for chunks we wish to bundle with
    /// the manifest.
    fn bundled_chunks_offset_list(&self) -> Vec<u64> {
        BUNDLED_CHUNK_OFFSETS.to_vec()
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

        if !buf.is_null()
            && count == SNAPSHOT_GRANULARITY
            && (offset % SNAPSHOT_GRANULARITY) == 0
            && !self.should_bundle_chunk_at_offset(offset)
        {
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

    /// Attempts to synchronously flush all spooled data for the
    /// tracked database.
    #[instrument(skip(self))]
    pub fn flush_spooled_data(&self) -> Result<()> {
        let to_flush = self.buffer.spooling_directory();

        if tokio::runtime::Handle::try_current().is_ok() {
            // We use our own Tokio runtime in `copy_spool_path`.
            // Avoid panics due to nested runtimes by flushing in a
            // temporary thread.  Flushes have to upload data over the
            // network, so they're not exactly fast anyway.
            let to_flush = to_flush.to_owned();
            std::thread::spawn(move || crate::copier::copy_spool_path(&to_flush))
                .join()
                .map_err(|e| chain_error!(e, "copy_spool_path thread failed"))?
        } else {
            crate::copier::copy_spool_path(to_flush)
        }
    }
}
