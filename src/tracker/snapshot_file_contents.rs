//! This module implements the snapshot generation logic
//! (`Tracker::snapshot_file_contents`) of `Tracker`s.
use std::os::unix::fs::FileExt;
use std::os::unix::io::OwnedFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::instrument;
use umash::Fingerprint;

use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::drop_result;
use crate::fresh_warn;
use crate::loader::is_well_known_fingerprint;
use crate::loader::Chunk;
use crate::manifest_schema::clear_version_id;
use crate::manifest_schema::extract_version_id;
use crate::manifest_schema::fingerprint_file_chunk;
use crate::manifest_schema::fingerprint_sqlite_header;
use crate::manifest_schema::fingerprint_v1_chunk_list;
use crate::manifest_schema::update_version_id;
use crate::manifest_schema::BundledChunk;
use crate::manifest_schema::Manifest;
use crate::manifest_schema::ManifestV1;
use crate::replication_buffer::FileIdentifier;
use crate::replication_buffer::PublishedManifest;
use crate::replication_buffer::ReplicationBuffer;
use crate::result::Result;

use super::MutationState;
use super::Tracker;
use super::BASE_CHUNK_MIN_LENGTH;
use super::SNAPSHOT_GRANULARITY;

/// What should we do with the current base chunk fingerprint list?
enum BaseChunkAction {
    /// Don't use a base chunk at all.
    None,
    /// Reuse the current base chunk.
    Reuse,
    /// Generate a new base chunk.
    Refresh,
}

impl BaseChunkAction {
    /// Determines what to do with a previous inherited base chunk
    /// list.  The `matches` parameter counts the number of
    /// fingerprints that match between the current chunk fingerprint
    /// list and the inherited base chunk (i.e., the number of 0
    /// fingerprints once the two are xor-ed together), and
    /// `total_size` is the size of the current chunk fingerprint
    /// list.
    fn decide(matches: usize, total_size: usize) -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // With this feature, we always return a random action to
        // exercise the rest of the snapshotting code.
        #[cfg(feature = "test_random_chunk_action")]
        return match rng.gen_range(0..3) {
            0 => BaseChunkAction::None,
            1 => BaseChunkAction::Reuse,
            _ => BaseChunkAction::Refresh,
        };

        // If there are enough matches that the random uniform integer
        // is less than the number of matches, keep the old base chunk.
        //
        // We sample from an inclusive range to guarantee there's a
        // small chance of recreating the base chunk even when
        // `matches == total_size`.
        if rng.gen_range(0..=total_size) < matches {
            BaseChunkAction::Reuse
        } else if total_size < BASE_CHUNK_MIN_LENGTH {
            // The list of chunk fingerprints is too short to warrant
            // a base chunk.  Regenerate to nothing.
            BaseChunkAction::None
        } else {
            BaseChunkAction::Refresh
        }
    }
}

/// Re-encodes `chunks` to expose a more compressible list of `u64`s
/// in the manifest.
///
/// On entry, `chunks` contains the real chunk fingerprints (before
/// any xor-ing).  The return value holds the chunk fingerprints,
/// potentially xor-ed with a base chunk.  If a base chunk was xor-ed
/// in, it's the second return value.
fn reencode_flattened_chunks(
    repl: &ReplicationBuffer,
    prev_base: Option<Arc<Chunk>>,
    mut chunks: Vec<u64>,
) -> Result<(Vec<u64>, Option<Arc<Chunk>>)> {
    use std::convert::TryInto;

    let apply_prev_base = |chunks: &mut Vec<u64>| {
        let mut zero = 0;
        // Apply `prev_base`, the previous base chunk, to see what's left.
        if let Some(prev) = prev_base.as_ref() {
            let base_words = prev
                .payload()
                .chunks_exact(std::mem::size_of::<u64>())
                // Safe to unwrap: we get slices of exactly 8 bytes.
                .map(|s| u64::from_le_bytes(s.try_into().unwrap()));

            for (word, base) in chunks.iter_mut().zip(base_words) {
                let result = *word ^ base;
                *word = result;

                zero += (result == 0) as usize;
            }
        }

        zero
    };

    let matches = apply_prev_base(&mut chunks);
    match BaseChunkAction::decide(matches, chunks.len()) {
        BaseChunkAction::None => {
            // Apply `prev_base` again to undo the xors.
            apply_prev_base(&mut chunks);
            return Ok((chunks, None));
        }
        BaseChunkAction::Reuse => return Ok((chunks, prev_base)),
        BaseChunkAction::Refresh => {}
    }

    // Copy the original value of `chunks` in `base`, as little-endian
    // bytes.
    let mut base: Vec<u8> = Vec::with_capacity(chunks.len() * std::mem::size_of::<u64>());

    for chunk in chunks.iter() {
        base.extend(chunk.to_le_bytes());
    }

    // And now un-apply `prev_base`.
    if let Some(prev) = prev_base {
        for (base, prev) in base.iter_mut().zip(prev.payload()) {
            *base ^= prev;
        }
    }

    chunks.fill(0);

    let base_chunk = Chunk::arc_from_bytes(&base);
    std::mem::drop(base);

    if !is_well_known_fingerprint(base_chunk.fprint()) {
        repl.stage_chunk(base_chunk.fprint(), base_chunk.payload())?;
    }

    Ok((chunks, Some(base_chunk)))
}

// These methods are used by the snapshotting logic.
impl Tracker {
    /// Computes the new fingerprint for the sqlite header.
    ///
    /// Returns Ok(None) if the sqlite file is empty (i.e., not created
    /// yet).
    fn generate_header_fprint(&self) -> Result<Option<Fingerprint>> {
        match fingerprint_sqlite_header(&self.file) {
            Some(fprint) => Ok(Some(fprint)),
            None => {
                if let Ok(meta) = self.file.metadata() {
                    if meta.len() == 0 {
                        // If the file is empty, the failure is
                        // benign.  Make sure the next snapshot
                        // definitely starts from scratch.
                        clear_version_id(&self.file).map_err(|e| {
                            chain_error!(e, "failed to clear version xattr on empty db file")
                        })?;
                        return Ok(None);
                    }
                }

                Err(fresh_warn!("invalid db file", path=?self.path))
            }
        }
    }

    /// Reads the current staged manifest, if any.
    pub(super) fn read_current_manifest(&self) -> Result<Option<(Manifest, Option<Arc<Chunk>>)>> {
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
}

/// Everything a new snapshot needs from the manifest it is about to
/// replace.
///
/// This deliberately isn't a `Manifest`.  A staged manifest holds
/// `v1.chunks` in its xor-differenced form, with `base_chunks_fprint`
/// set; `Manifest::decode_and_validate` xors the base chunk back in
/// and clears `base_chunks_fprint` before handing the manifest to a
/// reader.  Only that decoded form means "the chunk fingerprints of
/// the previous snapshot", which is the only thing the consumer
/// wants.  Keeping the fingerprints rather than a `Manifest` removes
/// any chance of confusing the two, and lets us cache the value
/// without retaining the manifest's bundled chunk payloads.
pub(super) struct SnapshotBase {
    /// The manifest's `v1.version_id`.
    version_id: Vec<u8>,

    /// Byte offsets of the chunks bundled with the manifest.  A new
    /// snapshot must treat those as dirty: it doesn't matter that
    /// they didn't change, we can't refer to them without making
    /// sure they're available for readers.
    bundled_chunk_offsets: Vec<u64>,

    /// The manifest's chunk fingerprints, with every bundled chunk
    /// overwritten by the zero fingerprint; the list a new snapshot
    /// starts from.  `None` if the manifest has no usable chunk list.
    chunk_fprints: Option<Vec<Fingerprint>>,

    /// The base chunk the manifest xor-diffs against, if any.
    base_chunk: Option<Arc<Chunk>>,
}

/// Reinterprets a flat list of `u64` words as pairs of fingerprints.
///
/// Returns `None` if the list holds an odd number of words.
fn unflatten_chunk_fprints(flattened: &[u64]) -> Option<Vec<Fingerprint>> {
    if (flattened.len() % 2) != 0 {
        return None;
    }

    let mut ret = Vec::with_capacity(flattened.len() / 2);

    for i in 0..flattened.len() / 2 {
        ret.push(Fingerprint::new(flattened[2 * i], flattened[2 * i + 1]));
    }

    Some(ret)
}

impl SnapshotBase {
    /// Builds the base from a manifest as returned by
    /// `read_manifest_at_path`, i.e., one that
    /// `Manifest::decode_and_validate` has already decoded: its
    /// `v1.chunks` holds real fingerprints, not the on-disk
    /// xor-differenced form.
    fn from_decoded_manifest(manifest: &Manifest, base_chunk: Option<Arc<Chunk>>) -> Self {
        let chunk_fprints = manifest
            .v1
            .as_ref()
            .and_then(|v1| unflatten_chunk_fprints(&v1.chunks));

        Self::new(manifest.v1.as_ref(), chunk_fprints, base_chunk)
    }

    /// Builds the base for the manifest `stage_new_snapshot` just
    /// published.
    ///
    /// `chunk_fprints` must be the fingerprint list from *before*
    /// `reencode_flattened_chunks` xor-differenced it into
    /// `manifest.v1.chunks`: that is exactly what
    /// `Manifest::decode_and_validate` reconstructs for a reader.
    /// Taking `manifest.v1.chunks` instead would seed the next
    /// snapshot with the residual of the diff -- in steady state, all
    /// zeroes, and `Fingerprint::new(0, 0)` is not
    /// `zero_fingerprint()`, so it names chunks that exist nowhere.
    fn from_published_manifest(
        manifest: &Manifest,
        chunk_fprints: &[Fingerprint],
        base_chunk: Option<Arc<Chunk>>,
    ) -> Self {
        Self::new(
            manifest.v1.as_ref(),
            Some(chunk_fprints.to_vec()),
            base_chunk,
        )
    }

    fn new(
        v1: Option<&ManifestV1>,
        mut chunk_fprints: Option<Vec<Fingerprint>>,
        base_chunk: Option<Arc<Chunk>>,
    ) -> Self {
        let v1 = match v1 {
            Some(v1) => v1,
            None => {
                return SnapshotBase {
                    version_id: Vec::new(),
                    bundled_chunk_offsets: Vec::new(),
                    chunk_fprints: None,
                    base_chunk,
                }
            }
        };

        // If a chunk is bundled with the manifest, we can't assume
        // it's available in the content-addressed store for us to
        // reuse.  Overwrite it with the zero fingerprint: the loader
        // special-cases that fingerprint and always knows what it
        // corresponds to, even when it's not in the
        // content-addressed store.  If we happen to find that the new
        // chunk at that location matches the zero fingerprint, we
        // don't have to upload anything.
        //
        // The consumer (`stage_new_snapshot`) also knows to consider
        // the data at `bundled_chunk_offsets` dirty, and probably
        // stage fresh data for upload.
        let zero_fp = crate::loader::zero_fingerprint();
        if let Some(fprints) = chunk_fprints.as_mut() {
            for bundled in &v1.bundled_chunks {
                let index = bundled.chunk_index;

                if index < fprints.len() as u64 {
                    fprints[index as usize] = zero_fp;
                }
            }
        }

        SnapshotBase {
            version_id: v1.version_id.clone(),
            bundled_chunk_offsets: v1.bundled_chunks.iter().map(|x| x.chunk_offset).collect(),
            chunk_fprints,
            base_chunk,
        }
    }
}

impl PartialEq for SnapshotBase {
    fn eq(&self, other: &Self) -> bool {
        // A `Chunk` isn't `PartialEq`, but its fingerprint identifies
        // its contents.
        self.version_id == other.version_id
            && self.bundled_chunk_offsets == other.bundled_chunk_offsets
            && self.chunk_fprints == other.chunk_fprints
            && self.base_chunk.as_ref().map(|x| x.fprint())
                == other.base_chunk.as_ref().map(|x| x.fprint())
    }
}

impl Eq for SnapshotBase {}

/// Hand-written to keep the fingerprint list out of the output: it
/// holds one entry per 64KB of database, and a `SnapshotBase` is
/// reachable from a `Tracker`.  `#[instrument]` records arguments
/// eagerly with their `Debug` impl, and walking that list is the
/// mistake 8e83bde had to fix in `publish_manifest`.
impl std::fmt::Debug for SnapshotBase {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("SnapshotBase")
            .field("version_id", &self.version_id)
            .field("bundled_chunk_offsets", &self.bundled_chunk_offsets)
            .field("chunks", &self.chunk_fprints.as_ref().map(|x| x.len()))
            .field("base_chunk", &self.base_chunk.as_ref().map(|x| x.fprint()))
            .finish()
    }
}

/// The manifest this `Tracker` last published, in the form the next
/// snapshot wants to read it back in, with the identity of the file
/// we published it to.
///
/// What it costs: one open descriptor and the fingerprint list (16
/// bytes per 64KB of database -- ~14MB for a 57GB one), held for the
/// *connection's* life, not the snapshot's.  A connection that stops
/// writing never re-examines its entry, so it keeps both -- and pins
/// a manifest inode that may already be unlinked -- until it closes.
#[derive(Debug)]
pub(super) struct StagedManifest {
    /// The manifest file we published.  Held open for exactly as long
    /// as we trust `id`: an open descriptor pins the inode, which is
    /// what turns `(dev, ino)` into an identity instead of a guess.
    ///
    /// An `OwnedFd` rather than a `File`, deliberately.  Scratch files
    /// are opened read-write (`tempfile` forces that, and the 0444
    /// `set_permissions` in `create_scratch_file` lands after the
    /// open, so it doesn't demote the descriptor), and `persist`
    /// hands that same descriptor back.  Writing through it would
    /// break the "staging files are write-once" invariant from inside
    /// the process, and a same-length write would be invisible to
    /// `equal_except_ctime`.  `OwnedFd` has no I/O methods at all, so
    /// the pin cannot become a write.
    ///
    /// The staged manifest really can go away under us --
    /// `copier::force_full_snapshot` deletes every file in
    /// "staging/meta" outright, and the stale/dangling directory
    /// sweeps remove the whole subtree -- after which a republish
    /// recreates the file in the same directory, and could be handed
    /// the same inode number.  `CopierWorker::handle_staging_directory`
    /// holds its manifest handles open for the same reason.
    _fd: OwnedFd,

    /// The identity `_fd`'s file had when we published it.
    id: FileIdentifier,

    /// What the next snapshot would get out of that file.
    base: Arc<SnapshotBase>,
}

/// How do we want to treat the current manifest (that we're about to
/// replace)?
enum CurrentManifest {
    // Don't generate a new snapshot at all.
    UpToDate,
    // Snapshot from scratch.
    Desync,
    // Use the previous manifest as an initial snapshot.
    Initial(Arc<SnapshotBase>),
}

// This impl block has all the snapshot update logic.
//
// While that logic is broken up in named methods, their correctness
// relies on the state (in the `Tracker`, but also in the replication
// buffer stored on the filesystem) set up by prior method calls in
// `snapshot_file_contents`, and should not be invoked in any
// different context.
impl Tracker {
    /// Returns the cached base for the staged manifest, if we
    /// published that manifest ourselves and the file on disk is
    /// still the one we wrote.  Leaves the entry in place on a hit --
    /// it stays valid until somebody republishes.
    ///
    /// A hit also skips the read that used to prove the base *chunk*
    /// file still exists -- `reencode_flattened_chunks`'s `Reuse` arm
    /// republishes `base_chunks_fprint` without re-staging it.  That
    /// is sound because chunk GC only ever runs immediately after its
    /// caller's own `publish_manifest` (`full_gc` and
    /// `maybe_partial_gc` both), which replaced the file at our path
    /// and so already invalidated this entry; and both keep every
    /// chunk the manifest they just published names.  No one can
    /// delete our base chunk while leaving our manifest current.
    ///
    /// Clears it on a miss: a stale entry is useless, and pins both
    /// an inode and a fingerprint list for the rest of the
    /// connection's life.
    fn cached_base_if_current(&mut self) -> Option<Arc<SnapshotBase>> {
        if !self.staged_manifest_is_current() {
            self.staged_manifest_cache = None;
            return None;
        }

        self.staged_manifest_cache
            .as_ref()
            .map(|cached| cached.base.clone())
    }

    /// Returns whether the staged manifest file is still the one our
    /// cache entry describes.  Does not mutate the cache, so it can
    /// be asked more than once.
    ///
    /// We hold that file open, so its inode can't be recycled: a
    /// different `(dev, ino)` at the path means someone else
    /// republished, or the file was deleted and recreated, and our
    /// value is no longer the base for the next snapshot.
    ///
    /// This is the gate, not a redundant double-check, and deleting
    /// it would be a correctness bug.  `judge_current_manifest` does
    /// also discard a base whose `version_id` doesn't match the db
    /// file's, but a republish need not move that id: a snapshot taken
    /// when `backing_file_state` isn't `Dirty` -- a freshly opened
    /// connection, say -- reaches `Desync` whenever the staged
    /// manifest is missing or unreadable, and then publishes carrying
    /// the db's *existing* version id.  `copier::force_full_snapshot`
    /// gets there by emptying "staging/meta" even when clearing the db
    /// file's version xattr failed, as does anyone who clears the
    /// spool directory by hand.  After that another connection's
    /// cached entry still matches on version id while the staged
    /// manifest is no longer its own, and reusing it would publish a
    /// manifest naming chunks the other connection's `full_gc` has
    /// already deleted.  This check is what stops that.
    fn staged_manifest_is_current(&self) -> bool {
        // Nothing to check -- and nothing to stat -- without an entry.
        let cached = match self.staged_manifest_cache.as_ref() {
            Some(cached) => cached,
            None => return false,
        };

        let current = self
            .buffer
            .staged_manifest_path(&self.path)
            .map_err(|e| chain_info!(e, "failed to derive staged manifest path", path=?self.path))
            .ok()
            .and_then(|path| std::fs::metadata(path).ok())
            .map(|meta| FileIdentifier::from_metadata(&meta));

        matches!(current, Some(current) if cached.id.equal_except_ctime(&current))
    }

    /// Reads and decodes the staged manifest, and distills it down to
    /// what a new snapshot needs from it.
    fn read_snapshot_base(&self) -> Option<Arc<SnapshotBase>> {
        self.read_current_manifest()
            .map_err(|e| chain_info!(e, "failed to read staged manifest file"))
            .ok()
            .flatten()
            .map(|(manifest, base_chunk)| {
                Arc::new(SnapshotBase::from_decoded_manifest(&manifest, base_chunk))
            })
    }

    /// Returns what a new snapshot needs from the currently staged
    /// manifest, if any.
    ///
    /// Decoding that manifest is linear in the size of the database
    /// (seconds, on the largest ones we replicate) and happens inside
    /// the sqlite commit, with the db's write lock held, so we skip
    /// the read whenever we published the manifest ourselves and can
    /// still prove the file on disk is the one we wrote.
    ///
    /// All of it opt-in: unless `Options::cache_staged_manifest` is
    /// set, the cache is never populated and this always falls
    /// through to the read.
    #[cfg(not(feature = "test_vfs"))]
    fn current_snapshot_base(&mut self) -> Option<Arc<SnapshotBase>> {
        if let Some(base) = self.cached_base_if_current() {
            return Some(base);
        }

        self.read_snapshot_base()
    }

    /// Same, except that under `test_vfs` we always re-read the
    /// staged manifest and use the cached value only to check itself.
    ///
    /// The read isn't merely value-producing in that configuration.
    /// `Loader::fetch_chunk`'s fast path is disabled under `test_vfs`,
    /// so the base chunk is fetched from "staging/chunks" rather than
    /// from the in-process cache `recent_base_chunk` keeps it in --
    /// and `validate_all_snapshots` turns a staged manifest that can
    /// no longer be decoded into a panic under `test_validate_reads`.
    /// (The fetch itself does not assert: a missing chunk comes back
    /// as `Ok(None)` and degrades to a full resnapshot.)  Skipping the
    /// read
    /// would also skip `decode_and_validate`'s `contents_fprint` match
    /// and chunk-list parity check.  Asserting here instead means that every
    /// snapshot a `test_vfs` build takes against a database with a
    /// base chunk also checks that the value we *would* have served
    /// is the value a reader gets back.  `t/test.sh` is where that
    /// runs in anger, over real databases;
    /// `tracker_caches_the_manifest_it_published` also reaches it
    /// under `--features test_validate_all`.
    ///
    /// The assertion itself is pinned by
    /// `the_test_vfs_self_check_rejects_a_poisoned_cache`, which
    /// writes a wrong value into the cache directly, and the serving
    /// order by `the_test_vfs_arm_serves_the_cached_value_it_would_have_served`.  And the check only
    /// discriminates on a database that has a base chunk, which for
    /// `t/test.sh`'s small databases depends on `test_random_chunk_action`
    /// being in its default `FEATURES` -- `BaseChunkAction::decide`
    /// ignores the length threshold under that feature.  The
    /// commented-out alternative feature sets in `t/test.sh` all drop
    /// it, and under those this assertion still runs but can only ever
    /// compare undifferenced chunk lists.
    /// `tracker_caches_the_manifest_it_published` does feed it a
    /// differenced one under `--features test_validate_all`.
    ///
    /// And all of that needs the cache to be populated at all:
    /// `Options::cache_staged_manifest` is opt-in, and `t/test.sh`
    /// sets it in both of its configs, so the integration suite runs
    /// with this check live rather than silently comparing nothing.
    #[cfg(feature = "test_vfs")]
    fn current_snapshot_base(&mut self) -> Option<Arc<SnapshotBase>> {
        let cached = self.cached_base_if_current();
        let fresh = self.read_snapshot_base();

        // Only compare if the staged manifest was ours for the whole
        // read.  `cached_base_if_current` proved that before the read;
        // another process publishing, or `force_full_snapshot` emptying
        // "staging/meta", in the window between the stat and the open
        // would leave us comparing our manifest against someone else's
        // and panicking inside a sqlite commit.  Re-checking after the
        // read settles it: once the path leaves our inode it can never
        // return to it, since every publish renames a fresh inode into
        // place and ours is pinned.
        //
        // A read that produced nothing is not evidence against the
        // cache either -- that is the pre-existing "give up and
        // resnapshot from scratch" path, and asserting on it would turn
        // a transient fetch failure into a panic.
        if let (Some(cached), Some(fresh)) = (&cached, &fresh) {
            if self.staged_manifest_is_current() {
                assert_eq!(
                    &**cached, &**fresh,
                    "cached snapshot base must match the staged manifest"
                );
            }
        }

        // Return what production would, not what we just read: every
        // `FEATURES` line in `t/test.sh` pulls in `test_vfs`, so
        // returning `fresh` would mean the integration suite never
        // once exercises the path this whole change adds.  The orders
        // differ exactly when the two values do *not* both exist, and
        // on a failed re-check -- where returning `cached` is still
        // what production serves under the same interleaving, since
        // production stats once, earlier, and never reads.
        cached.or(fresh)
    }

    /// Loads the current manifest and figures out what to do with it.
    fn judge_current_manifest(&mut self, version_id: &[u8]) -> CurrentManifest {
        let mut current_manifest: Option<Arc<SnapshotBase>> = self.current_snapshot_base();

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

            if let Some(current) = &current_manifest {
                // The current snapshot seems to have everything
                // up to our last write transaction.  We can use that!
                //
                // Remember to special-case empty version strings: they
                // never match anything, much like NaNs.
                if !self.previous_version_id.is_empty()
                    && current.version_id == self.previous_version_id
                {
                    up_to_date = true;
                }

                // The current snapshot seems to also include
                // write transaction.  Use it as a base, but
                // don't bail out, just in case this is a
                // spurious match.
                if !version_id.is_empty() && current.version_id == version_id {
                    up_to_date = true;
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
                if let Some(current) = &current_manifest {
                    if current.version_id == version_id {
                        return CurrentManifest::UpToDate;
                    }
                }
            }

            // If we think there's work to do after a read
            // transaction, assume the worst, and rebuild
            // the snapshot from scratch.
            current_manifest = None;
        }

        match current_manifest {
            None => CurrentManifest::Desync,
            Some(current) => CurrentManifest::Initial(current),
        }
    }

    /// Snapshots all the 64KB chunks in the tracked file, and returns
    /// the file's size as well, a list of chunk fingerprints, a list
    /// of bundled chunks for the manifest, and the number of chunks
    /// that were actually snapshotted.
    #[instrument(skip(self, base), err)]
    fn snapshot_chunks(
        &self,
        base: Option<Vec<Fingerprint>>,
    ) -> Result<(u64, Vec<Fingerprint>, Vec<BundledChunk>, usize)> {
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
        let mut bundled_chunks = Vec::new();

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

            // When a chunk is bundled, we must always send it in.
            let ret = if self.should_bundle_chunk_at_offset(begin) {
                bundled_chunks.push(BundledChunk {
                    chunk_index,
                    chunk_offset: begin,
                    chunk_fprint: Some(fprint.into()),
                    chunk_data: slice.to_vec(),
                });
                Ok(true)
            } else if fprint == chunk_fprints[chunk_index as usize] {
                // Nothing to do, it's all clean
                Ok(false)
            } else {
                // Only stage the chunk if it has changed and isn't
                // statically known to the loader: we don't want our
                // background scans to create useless copy work.
                if !is_well_known_fingerprint(fprint) {
                    repl.stage_chunk(fprint, slice)?;
                }

                Ok(true)
            };

            chunk_fprints[chunk_index as usize] = fprint;
            ret
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

        Ok((len, chunk_fprints, bundled_chunks, num_snapshotted))
    }

    /// Stages a new snapshot for the db file's current contents,
    /// based off the current manifest (soon to be previous), if
    /// applicable.
    ///
    /// On success, returns the number of new chunks staged, the list
    /// of chunks referenced by the new manifest, and the new
    /// manifest's base chunk (for xor-diffing), if any.
    fn stage_new_snapshot(
        &mut self,
        header_fprint: Fingerprint,
        version_id: Vec<u8>,
        current: Option<Arc<SnapshotBase>>,
    ) -> Result<(usize, Vec<Fingerprint>, Option<Fingerprint>)> {
        use std::os::unix::fs::MetadataExt;

        fn flatten_chunk_fprints(fprints: &[Fingerprint]) -> Vec<u64> {
            let mut ret = Vec::with_capacity(fprints.len() * 2);

            for fprint in fprints {
                ret.extend(&fprint.hash);
            }

            ret
        }

        // If the current manifest has any bundled chunks, we must
        // mark them as dirty: it doesn't matter that we didn't change
        // them, we can't refer to them without making sure they're
        // available for readers.
        if let Some(current) = current.as_ref() {
            for offset in &current.bundled_chunk_offsets {
                self.dirty_chunks.insert(*offset, None);
            }
        }

        // If we want to bundle something, we should always bundle it.
        // That's the expected steady state, and avoids potential
        // state transition bugs when the previous manifest did not
        // bundle the same set of chunks we want to.
        for offset in self.bundled_chunks_offset_list() {
            self.dirty_chunks.insert(offset, None);
        }

        // Try to get an initial list of chunks to work off.
        let base_fprints = current
            .as_ref()
            .and_then(|current| current.chunk_fprints.clone());

        let (len, mut chunks, bundled_chunks, mut copied) = self.snapshot_chunks(base_fprints)?;

        let (ctime, ctime_ns) = match self.file.metadata() {
            Ok(meta) => (meta.ctime(), meta.ctime_nsec() as i32),
            Err(e) => {
                let _ = chain_warn!(e, "failed to fetch file metadata", ?self.path);
                (0, 0)
            }
        };

        let flattened = flatten_chunk_fprints(&chunks);
        let manifest_fprint = fingerprint_v1_chunk_list(&flattened);
        let prev_base_chunk = current.and_then(|current| current.base_chunk.clone());
        let (compressible, base_chunk) =
            reencode_flattened_chunks(&self.buffer, prev_base_chunk, flattened)?;

        let base_fprint = base_chunk.as_ref().map(|x| x.fprint());

        let manifest = Manifest {
            v1: Some(ManifestV1 {
                header_fprint: Some(header_fprint.into()),
                version_id,
                contents_fprint: Some(manifest_fprint.into()),
                len,
                ctime,
                ctime_ns,
                base_chunks_fprint: base_fprint.map(|fp| fp.into()),
                generated_by: crate::manifest_schema::generator_version_bytes(),
                chunks: compressible,
                bundled_chunks,
            }),
        };

        let published = self.buffer.publish_manifest(&self.path, &manifest)?;

        // We published our new manifest, so we'll probably find a
        // reference to `base_chunk` the next time we load the
        // staged manifest.  Let's make sure it remains cached.
        self.recent_base_chunk = base_chunk.clone();

        // Remember what the next snapshot would read back out of the
        // file we just published, so that it doesn't have to -- if
        // this connection opted into the cache
        // (`Options::cache_staged_manifest`).  When it didn't, this
        // assignment is what drops `published`'s descriptor, and the
        // cache stays empty for the connection's whole life.
        //
        // `chunks` is the list from before `reencode_flattened_chunks`
        // xor-differenced it into `manifest.v1.chunks`, which is what
        // a reader gets back out of the published manifest; see
        // `SnapshotBase::from_published_manifest`.
        // `None` only if we could not stat what we just published; the
        // manifest is out there either way, we simply don't get to
        // reuse it.  In that case the `map` also *clears* the previous
        // entry -- which is right, since we just replaced the file it
        // described.
        let PublishedManifest { file, id } = published;
        self.staged_manifest_cache = if self.cache_staged_manifest {
            id.map(|id| StagedManifest {
                base: Arc::new(SnapshotBase::from_published_manifest(
                    &manifest, &chunks, base_chunk,
                )),
                id,
                _fd: file.into(),
            })
        } else {
            None
        };

        if let Some(base_fprint) = base_fprint {
            copied += 1;
            chunks.push(base_fprint);
        }

        Ok((copied, chunks, base_fprint))
    }

    /// Attempt to publish a new ready manifest if possible.
    ///
    /// The current staged manifest must only refer to chunks in
    /// `chunks`.
    ///
    /// Returns whether a new ready manifest was published.
    fn maybe_update_ready_buffer(&self, chunks: &[Fingerprint]) -> Result<bool> {
        let buf = &self.buffer;

        // Can't publish a new ready manifest if one already exists.
        if buf.has_ready_manifest(&self.path) {
            return Ok(false);
        }

        let ready = buf.prepare_ready_buffer(chunks)?;
        Ok(buf
            .publish_ready_buffer(ready)
            .map_err(|e| chain_info!(e, "failed to publish ready buffer", path=?self.path))
            .is_ok())
    }

    /// Performs a full chunk GC: we only want to keep the
    /// `base_chunk` for the manifest we just published.
    fn full_gc(&self, base_chunk: Option<Fingerprint>) {
        let chunks = base_chunk.iter().copied().collect::<Vec<_>>();
        drop_result!(self.buffer.gc_chunks(&chunks),
                     e => chain_info!(e, "failed to clear all staged chunks", path=?self.path));
        self.chunk_counter.store(0, Ordering::Relaxed);
    }

    /// Potentially performs a GC that keeps around all the `chunks`
    /// used in the manifest we just staged.
    ///
    /// We only perform that GC from time to time to amortise the cost
    /// of listing all the staged chunks, the chunks we won't delete,
    /// in particular.
    fn maybe_partial_gc(&self, copied: usize, chunks: Vec<Fingerprint>) {
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
        if copied >= rng.gen_range(0..=chunks.len() / 4) || total_count / 2 >= chunks.len() as u64 {
            drop_result!(self.buffer.gc_chunks(&chunks),
                         e => chain_info!(e, "failed to gc staged chunks", path=?self.path));
            self.chunk_counter.store(0, Ordering::Relaxed);
        }
    }

    /// Snapshots the contents of the tracked file to its replication
    /// buffer.  Concurrent threads or processes may be doing the same,
    /// but the contents of the file can't change, since we still hold
    /// a sqlite read lock on the db file.
    #[instrument(skip(self), err)]
    pub(super) fn snapshot_file_contents(&mut self) -> Result<()> {
        let header_fprint = match self.generate_header_fprint()? {
            Some(fprint) => fprint,
            // If there's no header, we don't have a sqlite DB file to
            // snapshot yet.
            None => return Ok(()),
        };

        let version_id = extract_version_id(&self.file, Some(header_fprint), Vec::new());
        // If we can't find any version id, try to set one for the next run.
        if version_id.is_empty() {
            drop_result!(update_version_id(&self.file, None),
                         e => chain_warn!(e, "failed to force populate version xattr", path=?self.path));
        }

        let current_manifest: Option<Arc<SnapshotBase>> =
            match self.judge_current_manifest(&version_id) {
                CurrentManifest::UpToDate => return Ok(()),
                CurrentManifest::Desync => None,
                CurrentManifest::Initial(current) => Some(current),
            };

        // We don't *have* to overwrite the .metadata file, but we
        // should create it if it's missing: without that file, the
        // copier can't make progress.
        self.buffer
            .ensure_staging_dir(&self.replication_targets, /*overwrite_meta=*/ false);

        // Publish an updated snapshot, and remember the chunks we
        // care about.
        let (copied, chunks, base_chunk) =
            self.stage_new_snapshot(header_fprint, version_id, current_manifest)?;

        let published = self.maybe_update_ready_buffer(&chunks)?;

        #[cfg(feature = "test_validate_reads")]
        self.validate_all_snapshots();

        // We did something.  Tell the copier.
        self.copier.signal_ready_buffer();

        // GC is opportunistic, failure is OK.  What's important to
        // the copier is that we only remove chunks after attempting
        // to publish the ready buffer.
        if published {
            self.full_gc(base_chunk);
        } else {
            self.maybe_partial_gc(copied, chunks);
        }

        // There is no further work to do while the sqlite read
        // lock is held; any temporary file or directory that's
        // still in flight either was left behind by a crashed
        // process, or belongs to a thread that will soon discover
        // it has no work to do.
        //
        // Either way, we can delete everything; clean up is
        // also opportunistic, so failure is OK.
        drop_result!(self.buffer.cleanup_scratch_directory(),
                     e => chain_info!(e, "failed to clear scratch directory", path=?self.path));

        #[cfg(feature = "test_validate_writes")]
        self.compare_snapshot().expect("snapshots must match");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use umash::Fingerprint;

    use super::reencode_flattened_chunks;
    use super::unflatten_chunk_fprints;
    use super::SnapshotBase;
    use crate::loader::Chunk;
    use crate::manifest_schema::fingerprint_v1_chunk_list;
    use crate::manifest_schema::BundledChunk;
    use crate::manifest_schema::Manifest;
    use crate::manifest_schema::ManifestV1;
    use crate::replication_buffer::ReplicationBuffer;
    use crate::replication_target::ReplicationTargetList;
    use crate::tracker::Tracker;

    /// Enough chunks that `reencode_flattened_chunks` generates a
    /// base chunk and the published `v1.chunks` is the
    /// xor-differenced form.  `BASE_CHUNK_MIN_LENGTH` is compared
    /// against the flattened *word* count, two per fingerprint, so
    /// the real threshold is half what it reads like; this
    /// clears either reading.
    const NUM_CHUNKS: usize = super::BASE_CHUNK_MIN_LENGTH + 100;

    fn fake_fprints(seed: u64, count: usize) -> Vec<Fingerprint> {
        (0..count as u64)
            .map(|i| {
                let x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(seed);
                Fingerprint::new(x | 1, !x | 1)
            })
            .collect()
    }

    fn flatten(fprints: &[Fingerprint]) -> Vec<u64> {
        let mut ret = Vec::with_capacity(fprints.len() * 2);

        for fprint in fprints {
            ret.extend(&fprint.hash);
        }

        ret
    }

    fn empty_targets() -> ReplicationTargetList {
        ReplicationTargetList {
            replication_targets: Vec::new(),
        }
    }

    /// Stages and publishes a manifest for `chunks` on top of
    /// `prev_base`, exactly like `stage_new_snapshot` does, and
    /// returns the manifest and its new base chunk.
    fn publish(
        buf: &ReplicationBuffer,
        db_path: &Path,
        version_id: &[u8],
        chunks: &[Fingerprint],
        bundled_chunks: Vec<BundledChunk>,
        prev_base: Option<Arc<Chunk>>,
    ) -> (Manifest, Option<Arc<Chunk>>) {
        let flattened = flatten(chunks);
        let manifest_fprint = fingerprint_v1_chunk_list(&flattened);
        let (compressible, base_chunk) =
            reencode_flattened_chunks(buf, prev_base, flattened).expect("must reencode");

        let manifest = Manifest {
            v1: Some(ManifestV1 {
                header_fprint: Some(Fingerprint::new(1, 2).into()),
                version_id: version_id.to_vec(),
                contents_fprint: Some(manifest_fprint.into()),
                len: chunks.len() as u64 * super::SNAPSHOT_GRANULARITY,
                ctime: 0,
                ctime_ns: 0,
                base_chunks_fprint: base_chunk.as_ref().map(|x| x.fprint().into()),
                generated_by: crate::manifest_schema::generator_version_bytes(),
                chunks: compressible,
                bundled_chunks,
            }),
        };

        buf.publish_manifest(db_path, &manifest)
            .expect("must publish");
        (manifest, base_chunk)
    }

    /// Publishes until `reencode_flattened_chunks` really does emit a
    /// base chunk.
    ///
    /// With no previous base and a list this long `BaseChunkAction::decide`
    /// always refreshes -- except under `test_random_chunk_action`, where it
    /// picks uniformly at random.  Retrying there keeps the xor-differenced
    /// case covered instead of letting the test quietly degrade into the
    /// no-base-chunk case, which is the one configuration in which the bug
    /// these tests exist for is invisible.  That configuration is the default
    /// in `t/test.sh`.
    fn publish_with_base_chunk(
        buf: &ReplicationBuffer,
        db_path: &Path,
        version_id: &[u8],
        chunks: &[Fingerprint],
        bundled_chunks: Vec<BundledChunk>,
        prev_base: Option<Arc<Chunk>>,
    ) -> (Manifest, Arc<Chunk>) {
        let mut prev_base = prev_base;

        for _ in 0..64 {
            let (manifest, base_chunk) = publish(
                buf,
                db_path,
                version_id,
                chunks,
                bundled_chunks.clone(),
                prev_base.clone(),
            );

            if let Some(base_chunk) = base_chunk {
                return (manifest, base_chunk);
            }

            prev_base = None;
        }

        panic!("never generated a base chunk");
    }

    /// Reads the staged manifest back the way `read_current_manifest`
    /// does, and distills it the way a cache miss would.
    fn read_back(buf: &ReplicationBuffer, db_path: &Path) -> SnapshotBase {
        let mut builder = kismet_cache::CacheBuilder::new();
        builder.plain_reader(buf.staged_chunk_directory());

        let (manifest, base_chunk) = buf
            .read_staged_manifest(db_path, builder, &empty_targets().replication_targets)
            .expect("must read")
            .expect("must exist");

        SnapshotBase::from_decoded_manifest(&manifest, base_chunk)
    }

    fn inode_of(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;

        std::fs::metadata(path).expect("must stat").ino()
    }

    fn fresh_buffer(dir: &tempfile::TempDir) -> (ReplicationBuffer, PathBuf) {
        let buf = ReplicationBuffer::new_for_test(dir.path().to_owned());
        buf.ensure_staging_dir(&empty_targets(), /*overwrite_meta=*/ true);
        (buf, PathBuf::from("/tmp/verneuil_test.db"))
    }

    /// The value we cache when we publish a manifest must be the
    /// value a reader gets back out of that same manifest.
    ///
    /// This is the check that the first (reverted) attempt at caching
    /// failed: it cached the published `v1.chunks`, which is
    /// xor-differenced against the base chunk, while
    /// `Manifest::decode_and_validate` xors that difference back out.
    #[test]
    fn cached_base_matches_decoded_manifest() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let (buf, db_path) = fresh_buffer(&dir);

        let bundled = vec![BundledChunk {
            chunk_index: 0,
            chunk_offset: 0,
            chunk_fprint: Some(Fingerprint::new(7, 8).into()),
            chunk_data: vec![0u8; 16],
        }];

        // First snapshot: no previous base chunk to diff against.
        let chunks = fake_fprints(1, NUM_CHUNKS);
        let (manifest, base_chunk) = publish_with_base_chunk(
            &buf,
            &db_path,
            b"v1",
            &chunks,
            bundled.clone(),
            /*prev_base=*/ None,
        );

        let cached =
            SnapshotBase::from_published_manifest(&manifest, &chunks, Some(base_chunk.clone()));
        assert_eq!(cached, read_back(&buf, &db_path));

        // Second snapshot, on top of the first one's base chunk: the
        // steady state, where the published `v1.chunks` is mostly
        // zeroes and a naive cache would be seeded with the residual.
        let mut next = chunks;
        next[42] = Fingerprint::new(1234, 5678);

        let (manifest, base_chunk) = publish_with_base_chunk(
            &buf,
            &db_path,
            b"v2",
            &next,
            bundled,
            /*prev_base=*/ Some(base_chunk),
        );
        let cached = SnapshotBase::from_published_manifest(&manifest, &next, Some(base_chunk));
        assert_eq!(cached, read_back(&buf, &db_path));
    }

    /// The same, for a manifest with no base chunk at all.
    ///
    /// This is the configuration in which the naive cache happened to
    /// be correct, so a test that only covers it proves nothing; it is
    /// here to pin the other side of `BASE_CHUNK_MIN_LENGTH`.  Which
    /// means it has to actually *get* the other side: a short list is
    /// enough under the default features, but `BaseChunkAction::decide`
    /// ignores the length threshold under `test_random_chunk_action`
    /// and picks uniformly, so without the retry this test silently
    /// becomes a duplicate of the one above about a third of the time
    /// -- in the feature set `t/test.sh` uses by default.
    #[test]
    fn cached_base_matches_decoded_manifest_without_base_chunk() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let (buf, db_path) = fresh_buffer(&dir);

        let chunks = fake_fprints(2, 4);

        for attempt in 0..64 {
            let (manifest, base_chunk) = publish(
                &buf,
                &db_path,
                b"v1",
                &chunks,
                Vec::new(),
                /*prev_base=*/ None,
            );

            if base_chunk.is_some() {
                assert!(attempt < 63, "never published without a base chunk");
                continue;
            }

            let cached = SnapshotBase::from_published_manifest(&manifest, &chunks, None);
            assert_eq!(cached, read_back(&buf, &db_path));
            return;
        }
    }

    /// Caching the published chunk list verbatim, rather than the
    /// pre-encoding one, is wrong whenever there is a base chunk.
    /// Guard the distinction so nobody "simplifies" the two together.
    #[test]
    fn published_chunk_list_differs_from_cached_one() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let (buf, db_path) = fresh_buffer(&dir);

        let chunks = fake_fprints(3, NUM_CHUNKS);
        let (manifest, base_chunk) = publish_with_base_chunk(
            &buf,
            &db_path,
            b"v1",
            &chunks,
            Vec::new(),
            /*prev_base=*/ None,
        );

        let v1 = manifest.v1.as_ref().expect("must have v1");
        let published = unflatten_chunk_fprints(&v1.chunks).expect("even number of words");
        let cached = SnapshotBase::from_published_manifest(&manifest, &chunks, Some(base_chunk));

        assert_ne!(Some(published), cached.chunk_fprints);
        assert_eq!(Some(chunks), cached.chunk_fprints);

        // An odd word count can't describe fingerprint pairs, and must
        // be rejected rather than silently truncated.
        assert_eq!(unflatten_chunk_fprints(&[1, 2, 3]), None);
        assert_eq!(
            unflatten_chunk_fprints(&[1, 2]),
            Some(vec![Fingerprint::new(1, 2)])
        );
    }

    /// `SnapshotBase::new` is shared by both constructors, so a test
    /// that only compares one against the other cannot see anything
    /// that happens inside it.  Pin the fields directly.
    #[test]
    fn snapshot_base_fields_from_decoded_manifest() {
        let chunks = fake_fprints(5, 8);
        let v1 = ManifestV1 {
            header_fprint: Some(Fingerprint::new(1, 2).into()),
            version_id: b"a-version-id".to_vec(),
            contents_fprint: Some(Fingerprint::new(3, 4).into()),
            len: 0,
            ctime: 0,
            ctime_ns: 0,
            base_chunks_fprint: None,
            generated_by: crate::manifest_schema::generator_version_bytes(),
            chunks: flatten(&chunks),
            // Two of them, so the loop has to actually iterate, and
            // the second one past the end of the chunk list, so the
            // bounds guard has to actually guard.
            bundled_chunks: vec![
                BundledChunk {
                    chunk_index: 3,
                    chunk_offset: 3 * super::SNAPSHOT_GRANULARITY,
                    chunk_fprint: Some(Fingerprint::new(7, 8).into()),
                    chunk_data: vec![0u8; 16],
                },
                BundledChunk {
                    chunk_index: 5,
                    chunk_offset: 5 * super::SNAPSHOT_GRANULARITY,
                    chunk_fprint: Some(Fingerprint::new(9, 10).into()),
                    chunk_data: vec![0u8; 16],
                },
                BundledChunk {
                    chunk_index: chunks.len() as u64,
                    chunk_offset: chunks.len() as u64 * super::SNAPSHOT_GRANULARITY,
                    chunk_fprint: Some(Fingerprint::new(11, 12).into()),
                    chunk_data: vec![0u8; 16],
                },
            ],
        };

        let chunk = Chunk::arc_from_bytes(&[3u8; 16]);
        let base =
            SnapshotBase::from_decoded_manifest(&Manifest { v1: Some(v1) }, Some(chunk.clone()));

        assert_eq!(base.version_id, b"a-version-id".to_vec());
        assert_eq!(
            base.bundled_chunk_offsets,
            vec![
                3 * super::SNAPSHOT_GRANULARITY,
                5 * super::SNAPSHOT_GRANULARITY,
                chunks.len() as u64 * super::SNAPSHOT_GRANULARITY,
            ]
        );
        assert_eq!(
            base.base_chunk.as_ref().map(|x| x.fprint()),
            Some(chunk.fprint())
        );

        // A bundled chunk isn't necessarily in the content-addressed
        // store, so its slot must come back as the zero fingerprint
        // and every other slot must be untouched.  The out-of-range
        // entry must be ignored rather than panicking or wrapping.
        let mut expected = chunks;
        expected[3] = crate::loader::zero_fingerprint();
        expected[5] = crate::loader::zero_fingerprint();
        assert_eq!(base.chunk_fprints, Some(expected));
    }

    /// `SnapshotBase` is reachable from a `Tracker`, and
    /// `Tracker::snapshot` logs `tracker=?self` when a snapshot
    /// fails.  Its `Debug` must stay
    /// O(1) in the size of the database: printing the fingerprint list
    /// is the mistake 8e83bde had to fix in `publish_manifest`.
    #[test]
    fn snapshot_base_debug_does_not_walk_the_chunk_list() {
        let chunks = fake_fprints(7, 100_000);
        let base = SnapshotBase::from_decoded_manifest(
            &Manifest {
                v1: Some(ManifestV1 {
                    header_fprint: Some(Fingerprint::new(1, 2).into()),
                    version_id: b"v1".to_vec(),
                    contents_fprint: Some(Fingerprint::new(3, 4).into()),
                    len: 0,
                    ctime: 0,
                    ctime_ns: 0,
                    base_chunks_fprint: None,
                    generated_by: crate::manifest_schema::generator_version_bytes(),
                    chunks: flatten(&chunks),
                    bundled_chunks: Vec::new(),
                }),
            },
            /*base_chunk=*/ None,
        );

        // Printing the list itself would be megabytes.  The exact
        // bound doesn't matter; that it is independent of the database
        // size does.
        let rendered = format!("{:?}", base);
        assert!(
            rendered.len() < 512,
            "Debug output must not grow with the chunk list, got {} bytes: {:.200}...",
            rendered.len(),
            rendered
        );
        // An upper bound alone would be satisfied by a `Debug` that
        // printed nothing useful; keep the summary honest too.
        assert!(rendered.contains("chunks: Some(100000)"), "{}", rendered);
        assert_eq!(base.chunk_fprints.map(|x| x.len()), Some(chunks.len()));
    }

    /// `PartialEq for SnapshotBase` is the only oracle the `test_vfs`
    /// self-check has, so a field dropped from it silently disarms
    /// that check.  Make every field load-bearing.
    #[test]
    fn snapshot_base_equality_covers_every_field() {
        let chunks = fake_fprints(6, 4);
        let bundled = vec![BundledChunk {
            chunk_index: 1,
            chunk_offset: super::SNAPSHOT_GRANULARITY,
            chunk_fprint: Some(Fingerprint::new(7, 8).into()),
            chunk_data: vec![0u8; 16],
        }];
        let v1 = |version_id: &[u8], chunks: &[Fingerprint], bundled: Vec<BundledChunk>| Manifest {
            v1: Some(ManifestV1 {
                header_fprint: Some(Fingerprint::new(1, 2).into()),
                version_id: version_id.to_vec(),
                contents_fprint: Some(Fingerprint::new(3, 4).into()),
                len: 0,
                ctime: 0,
                ctime_ns: 0,
                base_chunks_fprint: None,
                generated_by: crate::manifest_schema::generator_version_bytes(),
                chunks: flatten(chunks),
                bundled_chunks: bundled,
            }),
        };

        let chunk = Chunk::arc_from_bytes(&[0u8; 16]);
        let other_chunk = Chunk::arc_from_bytes(&[1u8; 16]);
        let base = |manifest: &Manifest, c: Option<Arc<Chunk>>| {
            SnapshotBase::from_decoded_manifest(manifest, c)
        };

        let reference = v1(b"v1", &chunks, bundled.clone());
        let original = base(&reference, Some(chunk.clone()));

        assert_eq!(original, base(&reference, Some(chunk.clone())));

        // version_id
        assert_ne!(
            original,
            base(&v1(b"v2", &chunks, bundled.clone()), Some(chunk.clone()))
        );

        // chunk_fprints
        let mut other_chunks = chunks.clone();
        other_chunks[0] = Fingerprint::new(99, 100);
        assert_ne!(
            original,
            base(
                &v1(b"v1", &other_chunks, bundled.clone()),
                Some(chunk.clone())
            )
        );

        // bundled_chunk_offsets.  Keep `chunk_index` where it is:
        // moving it would change which slot `SnapshotBase::new` zeroes,
        // and `chunk_fprints` would differ too, so the assertion would
        // pass without ever consulting the offsets.
        let mut other_bundled = bundled;
        other_bundled[0].chunk_offset = 9 * super::SNAPSHOT_GRANULARITY;
        assert_ne!(
            original,
            base(&v1(b"v1", &chunks, other_bundled), Some(chunk.clone()))
        );

        // base_chunk
        assert_ne!(original, base(&reference, Some(other_chunk)));
        assert_ne!(original, base(&reference, None));
    }

    /// `prepare_ready_buffer` hardlinks the staged manifest into
    /// "ready", which bumps its ctime without changing a byte.  If
    /// `equal_except_ctime` stopped tolerating that, the cache would
    /// simply never hit -- silently, and only in production.
    #[test]
    fn equal_except_ctime_tolerates_a_hardlink() {
        use crate::replication_buffer::FileIdentifier;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let path = dir.path().join("manifest");
        std::fs::write(&path, b"some manifest bytes").expect("must write");

        let before = FileIdentifier::from_metadata(&std::fs::metadata(&path).expect("must stat"));

        std::fs::hard_link(&path, dir.path().join("ready-manifest")).expect("must link");

        let after = FileIdentifier::from_metadata(&std::fs::metadata(&path).expect("must stat"));

        assert!(
            before.equal_except_ctime(&after),
            "a hardlink must not change the file's identity"
        );
        // Deliberately not asserting that ctime actually moved.  It
        // should, but on ext4 ctime comes from a coarse clock that
        // advances once per timer tick, while this sequence takes tens
        // of microseconds -- the assertion would fail on essentially
        // every run on the platform production uses.  What this test
        // owes is the tolerance direction; the discrimination direction
        // is pinned field by field in `replication_buffer`.

        // Length is part of the identity: appending to the file must
        // make it a different file as far as the check is concerned.
        std::fs::write(&path, b"some manifest bytes and more").expect("must write");
        let grown = FileIdentifier::from_metadata(&std::fs::metadata(&path).expect("must stat"));
        assert!(!before.equal_except_ctime(&grown));

        // And a different file of the same length is a different file,
        // whatever its timestamps happen to be.
        let sibling = dir.path().join("other");
        std::fs::write(&sibling, b"some manifest bytes").expect("must write");
        let sibling =
            FileIdentifier::from_metadata(&std::fs::metadata(&sibling).expect("must stat"));
        assert!(!before.equal_except_ctime(&sibling));
    }

    /// Drives a real `Tracker` through a snapshot and checks that the
    /// cache is populated, served, and invalidated.
    ///
    /// An earlier revision of this file asserted that nothing in
    /// `cargo test` could reach a `Tracker`, because one needs a file
    /// descriptor from the VFS.  That was wrong: `Tracker`'s fields are
    /// private to the `tracker` module, which this module is inside,
    /// and `fingerprint_sqlite_header` wants a hundred bytes rather
    /// than a real database.  Without this the entire cache lifecycle
    /// -- the stat, the comparison, the population, the invalidation --
    /// had no coverage at all.
    ///
    /// The second snapshot is a write transaction on purpose: that is
    /// the only path on which `judge_current_manifest` yields
    /// `Initial` and the cached base is actually handed to the diff,
    /// and it is also what reaches the `test_vfs` arm of
    /// `current_snapshot_base`, whose assertion needs both a cached
    /// value and one read back off disk.
    #[test]
    fn tracker_caches_the_manifest_it_published() {
        use std::collections::BTreeMap;
        use std::mem::ManuallyDrop;
        use std::os::unix::fs::FileExt;
        use std::sync::atomic::AtomicU64;

        // Past the base-chunk threshold (300 fingerprints today: the
        // constant is compared against the flattened word count), so
        // the published chunk list is xor-differenced.
        // Below it the encoded and decoded forms coincide and a cache
        // populated from the wrong one looks perfectly correct.
        const CHUNKS: u64 = 320;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let db_path = dir.path().join("db");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&db_path)
            .expect("must open db");
        // Sparse: the body reads back as zeros, which the loader knows
        // statically, so this stays fast.  Only the header has to be
        // real enough for `fingerprint_sqlite_header`.
        file.set_len(CHUNKS * super::SNAPSHOT_GRANULARITY)
            .expect("must size db");
        file.write_all_at(&[0x5au8; 256], 0)
            .expect("must write header");

        let buffer = ReplicationBuffer::new_for_test(dir.path().to_owned());
        let mut tracker = Tracker {
            file: ManuallyDrop::new(file),
            path: db_path,
            buffer,
            copier: crate::copier::Copier::get_global_copier(),
            replication_targets: empty_targets(),
            chunk_counter: AtomicU64::new(0),
            cached_uuid: None,
            dirty_chunks: BTreeMap::new(),
            backing_file_state: super::MutationState::Unknown,
            previous_version_id: Vec::new(),
            recent_base_chunk: None,
            staged_manifest_cache: None,
            cache_staged_manifest: true,
        };

        // Drive it the way the VFS does: a write marks the tracker
        // dirty and stamps a new version id, and `snapshot` resets that
        // state on the way out.  Going through `snapshot_file_contents`
        // directly instead leaves the version id empty, and then
        // `judge_current_manifest` can never reach `Initial`.
        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("first snapshot must succeed");

        assert!(
            tracker.staged_manifest_cache.is_some(),
            "a snapshot must populate the cache"
        );
        assert!(
            tracker.cached_base_if_current().is_some(),
            "the cache must be fresh right after we published it"
        );

        // What we cached has to be what a reader gets back.  Comparing
        // the two constructors against each other, as the other tests
        // do, cannot see a `stage_new_snapshot` that builds the entry
        // from the wrong value.
        {
            let cached = tracker
                .staged_manifest_cache
                .as_ref()
                .expect("cache must be populated");
            // Deterministic under the default features; under
            // `test_random_chunk_action` `decide` ignores the length
            // and picks at random, so the differenced case is a
            // coin flip there.  The equality below holds either way.
            #[cfg(not(feature = "test_random_chunk_action"))]
            assert!(
                cached.base.base_chunk.is_some(),
                "the database must be big enough to be differenced, \
                 or this test cannot see the bug it exists for"
            );
            assert_eq!(
                Some(&*cached.base),
                tracker.read_snapshot_base().as_deref(),
                "the cached value must equal what reading the manifest back gives"
            );
        }

        // A second write transaction: this is the only path on which
        // `judge_current_manifest` yields `Initial` and hands the
        // cached base to the diff.
        tracker.note_exclusive_lock();
        tracker
            .file
            .write_all_at(&[0xa5u8; 4096], 0)
            .expect("must rewrite header");
        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("second snapshot must succeed");

        assert!(
            tracker.cached_base_if_current().is_some(),
            "the cache must be repopulated by the snapshot that used it"
        );
        {
            let cached = tracker
                .staged_manifest_cache
                .as_ref()
                .expect("cache must be populated");
            assert_eq!(
                Some(&*cached.base),
                tracker.read_snapshot_base().as_deref(),
                "the cached value must still equal what a reader gets"
            );
        }

        // A second connection publishing must invalidate us: its
        // publish renames a fresh inode over the path.
        let mut second = test_tracker(&dir, 320, /*init=*/ false);
        second.note_exclusive_lock();
        second
            .file
            .write_all_at(&[0x11u8; 4096], 2 * super::SNAPSHOT_GRANULARITY)
            .expect("must write");
        second.flag_write(std::ptr::null(), 2 * super::SNAPSHOT_GRANULARITY, 4096);
        second.snapshot().expect("second connection must snapshot");

        assert!(
            tracker.cached_base_if_current().is_none(),
            "another connection's publish must invalidate the cache"
        );
        assert!(
            tracker.staged_manifest_cache.is_none(),
            "a stale entry must be cleared, not left pinning an inode"
        );
        assert!(
            second.cached_base_if_current().is_some(),
            "the connection that published last holds the fresh entry"
        );

        // And outright deletion -- the state `force_full_snapshot`
        // leaves behind -- must read as a miss too.  This is the
        // stat-ENOENT path, distinct from the replaced-inode path
        // above.
        let staged = second
            .buffer
            .staged_manifest_path(&second.path)
            .expect("must derive path");
        std::fs::remove_file(&staged).expect("must delete staged manifest");

        assert!(
            second.cached_base_if_current().is_none(),
            "a deleted staged manifest must invalidate the cache"
        );
        assert!(
            second.staged_manifest_cache.is_none(),
            "the entry must be cleared on deletion as well"
        );
    }

    /// The cache is opt-in: a tracker that did not opt in must never
    /// populate it -- so it holds no descriptor on the staged manifest
    /// -- and must still snapshot correctly, by reading the manifest
    /// back from the spool directory like the code before the cache
    /// existed.
    #[test]
    fn the_cache_is_opt_in() {
        use std::os::unix::fs::FileExt;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);
        tracker.cache_staged_manifest = false;

        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("first snapshot must succeed");

        assert!(
            tracker.staged_manifest_cache.is_none(),
            "a tracker that did not opt in must not populate the cache"
        );
        assert!(
            tracker.read_snapshot_base().is_some(),
            "the staged manifest itself must still be there to read back"
        );

        // A second write transaction works end to end, diffing
        // against the read-back manifest.
        tracker.note_exclusive_lock();
        tracker
            .file
            .write_all_at(&[0xa5u8; 4096], 0)
            .expect("must rewrite header");
        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("second snapshot must succeed");
        assert!(
            tracker.staged_manifest_cache.is_none(),
            "opting out must hold for every snapshot, not only the first"
        );
    }

    /// `configure_replication` must wire `Options::cache_staged_manifest`
    /// through to the flag `Tracker::new` reads, both directions --
    /// otherwise the cache is silently dead (or silently mandatory) no
    /// matter what the config says.  No other test constructs a
    /// `Tracker` through `Tracker::new`, so the global is asserted
    /// directly; nothing else in the suite writes it.
    #[test]
    fn the_config_flag_reaches_the_tracker() {
        assert!(
            !crate::tracker::staged_manifest_caching(),
            "the cache must be off by default"
        );

        crate::configure_replication(crate::Options {
            cache_staged_manifest: true,
            ..Default::default()
        })
        .expect("must configure");
        assert!(crate::tracker::staged_manifest_caching());

        crate::configure_replication(crate::Options::default()).expect("must configure");
        assert!(!crate::tracker::staged_manifest_caching());
    }

    /// `publish_manifest` must hand back the identity of the file it
    /// published, and that identity must still match a fresh stat of
    /// the staged manifest path.  A republish must not match.
    ///
    /// Note what this cannot check: reading the identity from the
    /// persisted descriptor rather than from the path only differs
    /// when someone replaces the path in the window between the two,
    /// and a single-threaded test has no way to land a write there.
    /// That property is argued, not tested.
    #[test]
    fn published_identity_tracks_the_staged_file() {
        use crate::replication_buffer::FileIdentifier;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let (buf, db_path) = fresh_buffer(&dir);

        let chunks = fake_fprints(4, 4);
        let flattened = flatten(&chunks);
        let manifest = Manifest {
            v1: Some(ManifestV1 {
                header_fprint: Some(Fingerprint::new(1, 2).into()),
                version_id: b"v1".to_vec(),
                contents_fprint: Some(fingerprint_v1_chunk_list(&flattened).into()),
                len: 0,
                ctime: 0,
                ctime_ns: 0,
                base_chunks_fprint: None,
                generated_by: crate::manifest_schema::generator_version_bytes(),
                chunks: flattened,
                bundled_chunks: Vec::new(),
            }),
        };

        let published = buf
            .publish_manifest(&db_path, &manifest)
            .expect("must publish");

        let path = buf
            .staged_manifest_path(&db_path)
            .expect("must derive path");
        let on_disk = FileIdentifier::from_metadata(
            &std::fs::metadata(&path).expect("staged manifest must exist"),
        );
        let published_id = published.id.as_ref().expect("must identify the file");
        assert!(published_id.equal_except_ctime(&on_disk));

        // The id and the descriptor must at least describe the same
        // file.  This does not distinguish reading the id from the
        // descriptor and reading it from the path -- see the note
        // above -- it only pins that the pair is self-consistent.
        assert_eq!(
            published_id,
            &FileIdentifier::new(&published.file).expect("must stat")
        );

        // Publishing again replaces the file with a *different inode*
        // -- that, plus holding the old one open, is the whole basis
        // for trusting `(dev, ino)`.
        let old_ino = inode_of(&path);
        let republished = buf
            .publish_manifest(&db_path, &manifest)
            .expect("must publish");
        let on_disk = FileIdentifier::from_metadata(
            &std::fs::metadata(&path).expect("staged manifest must exist"),
        );

        assert_ne!(
            old_ino,
            inode_of(&path),
            "a republish must land a new inode at the path"
        );
        assert!(!published_id.equal_except_ctime(&on_disk));
        assert!(republished
            .id
            .as_ref()
            .expect("must identify the file")
            .equal_except_ctime(&on_disk));
    }

    /// Builds a `Tracker` over a sparse db file of `chunks` 64KB
    /// chunks in `dir`, with a header real enough for
    /// `fingerprint_sqlite_header`.  `init` truncates and re-stamps
    /// the file; pass false to attach a second tracker to a database
    /// (and spool directory) that already exists.
    fn test_tracker(dir: &tempfile::TempDir, chunks: u64, init: bool) -> Tracker {
        use std::collections::BTreeMap;
        use std::mem::ManuallyDrop;
        use std::os::unix::fs::FileExt;
        use std::sync::atomic::AtomicU64;

        let db_path = dir.path().join("db");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(init)
            .open(&db_path)
            .expect("must open db");
        if init {
            file.set_len(chunks * super::SNAPSHOT_GRANULARITY)
                .expect("must size db");
            file.write_all_at(&[0x5au8; 256], 0)
                .expect("must write header");
        }

        Tracker {
            file: ManuallyDrop::new(file),
            path: db_path,
            buffer: ReplicationBuffer::new_for_test(dir.path().to_owned()),
            copier: crate::copier::Copier::get_global_copier(),
            replication_targets: empty_targets(),
            chunk_counter: AtomicU64::new(0),
            cached_uuid: None,
            dirty_chunks: BTreeMap::new(),
            backing_file_state: super::MutationState::Unknown,
            previous_version_id: Vec::new(),
            recent_base_chunk: None,
            staged_manifest_cache: None,
            cache_staged_manifest: true,
        }
    }

    /// `SnapshotBase` zeroes the slot of every chunk the
    /// *previous* manifest bundled, because a bundled chunk is not in
    /// the content-addressed store.  `stage_new_snapshot` repairs
    /// that by marking `current.bundled_chunk_offsets` dirty.
    ///
    /// That loop looks redundant next to the one right below it,
    /// which marks `self.bundled_chunks_offset_list()` dirty -- and
    /// is, as long as the staged manifest bundles exactly the set we
    /// want to bundle.  It is the state transition that matters: a
    /// manifest published by a build with a different
    /// `BUNDLED_CHUNK_OFFSETS` bundles a chunk this build does not,
    /// and nothing else will ever revisit that slot.  The published
    /// manifest then names `loader::zero_fingerprint()` for a chunk
    /// that is not zeroes, and the replica reads back a hole.
    ///
    /// Deleting the loop leaves the whole suite green, including
    /// `tracker_caches_the_manifest_it_published`, whose writes land
    /// on offset 0 -- the only offset this build bundles.
    // Not under `no_xattr`: with no version xattr the cold tracker's
    // `judge_current_manifest` forces `Desync`, so it never receives a
    // base and this test passes without exercising the loop it pins.
    #[cfg(not(feature = "no_xattr"))]
    #[test]
    fn a_bundled_chunk_the_new_build_does_not_bundle_is_still_re_snapshotted() {
        use std::os::unix::fs::FileExt;

        const BUNDLED_BY_THE_OLD_BUILD: u64 = 3;
        let offset = BUNDLED_BY_THE_OLD_BUILD * super::SNAPSHOT_GRANULARITY;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);

        // Chunk 3 must not be all zeroes, or its real fingerprint
        // would *be* the zero fingerprint and nothing could tell the
        // two apart.
        tracker
            .file
            .write_all_at(&[0x77u8; 4096], offset)
            .expect("must write");
        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.flag_write(std::ptr::null(), offset, 4096);
        tracker.snapshot().expect("first snapshot must succeed");

        // Republish what we just published, with chunk 3 bundled:
        // what a build whose `BUNDLED_CHUNK_OFFSETS` included this
        // offset would have left in "staging/meta".
        let (manifest, base_chunk) = tracker
            .read_current_manifest()
            .expect("must read")
            .expect("must exist");
        let v1 = manifest.v1.as_ref().expect("must have v1");
        let decoded = unflatten_chunk_fprints(&v1.chunks).expect("even number of words");
        assert_ne!(
            decoded[BUNDLED_BY_THE_OLD_BUILD as usize],
            crate::loader::zero_fingerprint()
        );

        let mut data = vec![0u8; super::SNAPSHOT_GRANULARITY as usize];
        tracker
            .file
            .read_exact_at(&mut data, offset)
            .expect("must read chunk");

        let mut bundled = v1.bundled_chunks.clone();
        bundled.push(BundledChunk {
            chunk_index: BUNDLED_BY_THE_OLD_BUILD,
            chunk_offset: offset,
            chunk_fprint: Some(decoded[BUNDLED_BY_THE_OLD_BUILD as usize].into()),
            chunk_data: data,
        });
        publish(
            &tracker.buffer,
            &tracker.path,
            &v1.version_id,
            &decoded,
            bundled,
            base_chunk,
        );

        // A second connection: no cache entry, so it reads that
        // manifest, and a write that touches neither bundled chunk.
        let mut cold = test_tracker(&dir, 320, /*init=*/ false);
        cold.note_exclusive_lock();
        cold.file
            .write_all_at(&[0x33u8; 4096], 5 * super::SNAPSHOT_GRANULARITY)
            .expect("must write");
        cold.flag_write(std::ptr::null(), 5 * super::SNAPSHOT_GRANULARITY, 4096);
        cold.snapshot().expect("second snapshot must succeed");

        let (manifest, _) = cold
            .read_current_manifest()
            .expect("must read")
            .expect("must exist");
        let v1 = manifest.v1.as_ref().expect("must have v1");
        let fprints = unflatten_chunk_fprints(&v1.chunks).expect("even number of words");
        assert_ne!(
            fprints[BUNDLED_BY_THE_OLD_BUILD as usize],
            crate::loader::zero_fingerprint(),
            "a chunk the previous manifest bundled must be re-snapshotted, \
             not left naming the placeholder `SnapshotBase` put in its slot"
        );
        assert_eq!(
            fprints[BUNDLED_BY_THE_OLD_BUILD as usize], decoded[BUNDLED_BY_THE_OLD_BUILD as usize],
            "and it must name the chunk's real contents"
        );
    }

    /// Pins the `test_vfs` self-check in `current_snapshot_base`:
    /// deleting that assertion, or making it compare a value with
    /// itself, must fail here.  The cache is a private field of a
    /// struct in this test's parent module, so producing the "wrong
    /// cached value" the assertion exists for is one assignment.
    #[cfg(feature = "test_vfs")]
    #[test]
    fn the_test_vfs_self_check_rejects_a_poisoned_cache() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);

        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("first snapshot must succeed");

        // The self-check is happy with the value we really published.
        assert!(tracker.current_snapshot_base().is_some());

        // Poison the cached base, leaving the staged manifest -- and
        // so the identity check -- untouched.
        let poisoned = SnapshotBase::from_decoded_manifest(
            &Manifest {
                v1: Some(ManifestV1 {
                    header_fprint: Some(Fingerprint::new(1, 2).into()),
                    version_id: b"not what we published".to_vec(),
                    contents_fprint: Some(Fingerprint::new(3, 4).into()),
                    len: 0,
                    ctime: 0,
                    ctime_ns: 0,
                    base_chunks_fprint: None,
                    generated_by: crate::manifest_schema::generator_version_bytes(),
                    chunks: flatten(&fake_fprints(99, 4)),
                    bundled_chunks: Vec::new(),
                }),
            },
            None,
        );
        tracker
            .staged_manifest_cache
            .as_mut()
            .expect("cache must be populated")
            .base = Arc::new(poisoned);

        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracker.current_snapshot_base()
        }));
        std::panic::set_hook(hook);

        assert!(
            result.is_err(),
            "the test_vfs self-check must reject a cached base that \
             disagrees with the staged manifest"
        );
    }

    /// `SnapshotBase::new`'s "no v1" arm:
    /// both of its constructors are only ever called with a real
    /// manifest elsewhere in the suite.
    #[test]
    fn a_manifest_without_v1_yields_an_empty_base() {
        let chunk = Chunk::arc_from_bytes(&[5u8; 16]);
        let chunks = fake_fprints(11, 4);

        let decoded =
            SnapshotBase::from_decoded_manifest(&Manifest { v1: None }, Some(chunk.clone()));
        assert_eq!(decoded.version_id, Vec::<u8>::new());
        assert_eq!(decoded.bundled_chunk_offsets, Vec::<u64>::new());
        assert_eq!(decoded.chunk_fprints, None);
        // The base chunk is the tracker's handle on a chunk it may
        // still need; it does not come from `v1` and must survive.
        assert_eq!(
            decoded.base_chunk.as_ref().map(|x| x.fprint()),
            Some(chunk.fprint())
        );

        // And a chunk list handed in by `from_published_manifest` must
        // be dropped rather than kept: without a `v1` there is no
        // `bundled_chunks` list to zero out against it, so the list
        // cannot be trusted.
        let published = SnapshotBase::from_published_manifest(
            &Manifest { v1: None },
            &chunks,
            Some(chunk.clone()),
        );
        assert_eq!(published.chunk_fprints, None);
        assert_eq!(published, decoded);
    }

    /// Returning the cached base is not the same as using it.  With
    /// the base, a snapshot re-reads only what changed; without it,
    /// `snapshot_chunks` starts from an empty list and re-stages every
    /// chunk that isn't statically known.  Count the staged chunks:
    /// dropping the base on the floor is otherwise invisible, because
    /// the published manifest comes out byte-identical either way.
    #[test]
    fn the_cached_base_spares_the_snapshot_from_restaging_everything() {
        use std::os::unix::fs::FileExt;

        const REAL_CHUNKS: u64 = 8;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);

        // Chunks of zeroes are well known to the loader and never
        // staged, so give the database some content to stage.
        for i in 0..REAL_CHUNKS {
            tracker
                .file
                .write_all_at(&[0x30u8 + i as u8; 4096], i * super::SNAPSHOT_GRANULARITY)
                .expect("must write");
            tracker.flag_write(std::ptr::null(), i * super::SNAPSHOT_GRANULARITY, 4096);
        }
        tracker.snapshot().expect("first snapshot must succeed");

        let chunk_dir = tracker.buffer.staged_chunk_directory();
        let staged_chunks = |dir: &std::path::Path| {
            std::fs::read_dir(dir)
                .map(|entries| entries.flatten().count())
                .unwrap_or(0)
        };
        let before = staged_chunks(&chunk_dir);
        // The premise the oracle rests on: the first snapshot published
        // a ready buffer, and `full_gc` kept at most the base chunk in
        // "staging/chunks".  (At most, not exactly: under
        // `test_random_chunk_action` the base chunk is a coin flip.)
        // Without this, `after - before` measures nothing meaningful.
        assert!(
            before <= 1,
            "full_gc must have emptied staging, got {}",
            before
        );

        // One chunk changes; the other seven must be taken from the
        // cached base rather than read and staged again.
        tracker.note_exclusive_lock();
        tracker
            .file
            .write_all_at(&[0xeeu8; 4096], 0)
            .expect("must write");
        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("second snapshot must succeed");

        let staged = staged_chunks(&chunk_dir).saturating_sub(before);
        // Chunk 0 is bundled (`BUNDLED_CHUNK_OFFSETS`), so a snapshot
        // that starts from the cached base stages nothing but the new
        // base chunk, while one that starts from an empty list stages
        // the other `REAL_CHUNKS - 1`.  `< REAL_CHUNKS` admits both:
        // it is one too loose, and leaves `base_fprints = None` green.
        //
        // The bound is 1, not 0: `reencode_flattened_chunks` may emit
        // a fresh base chunk, and under `test_random_chunk_action` it
        // decides at random, so 0 is flaky there.  At most one such
        // chunk exists per snapshot.
        assert!(
            staged <= 1,
            "expected the snapshot to stage only what changed, \
             got {} new chunks for a one-chunk write",
            staged
        );
    }

    /// The `test_vfs` arm of
    /// `current_snapshot_base` ends in `cached.or(fresh)`, and its
    /// comment says why: every `FEATURES` line in `t/test.sh` pulls in
    /// `test_vfs`, so returning `fresh` would mean the integration
    /// suite never exercises the production path at all.
    ///
    /// Nothing pinned the order.  `fresh.or(cached)` leaves the whole
    /// suite green, because the two are asserted equal whenever both
    /// exist, so no *value* can tell them apart -- only which `Arc`
    /// came back.  `the_cached_base_is_served_without_reading_the_manifest`
    /// cannot see it either: it destroys the manifest first, so
    /// `fresh` is `None` there and both orders return the cache.
    #[cfg(feature = "test_vfs")]
    #[test]
    fn the_test_vfs_arm_serves_the_cached_value_it_would_have_served() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);

        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("snapshot must succeed");

        // Both halves must exist, or this proves nothing: with
        // `fresh` empty every order returns the cache.
        assert!(
            tracker.read_snapshot_base().is_some(),
            "the staged manifest must still be readable"
        );
        let cached = tracker
            .staged_manifest_cache
            .as_ref()
            .expect("cache must be populated")
            .base
            .clone();

        let served = tracker
            .current_snapshot_base()
            .expect("a base must come back");
        assert!(
            Arc::ptr_eq(&served, &cached),
            "the test_vfs arm must hand back the cached value -- the one \
             production would use -- not the copy it just read back"
        );
    }

    /// A connection that opens a database somebody else
    /// already snapshotted has no cache entry, and must fall back to
    /// reading the staged manifest.  Nothing in the suite covered the
    /// miss path: the one `Tracker` test only ever asks after
    /// publishing.
    #[test]
    fn a_cold_tracker_still_reads_the_staged_manifest() {
        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);

        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("snapshot must succeed");
        let published = tracker.read_snapshot_base().expect("must read");

        let mut cold = test_tracker(&dir, 320, /*init=*/ false);
        assert!(cold.staged_manifest_cache.is_none());
        let base = cold
            .current_snapshot_base()
            .expect("a cold tracker must read the manifest somebody else published");
        assert_eq!(&*base, &*published);
    }

    /// The cache exists so a snapshot does not have to read
    /// and decode the manifest.  Corrupt the staged manifest's bytes
    /// in place -- same inode, same length, so the identity check
    /// still passes -- and the base must still come back, unchanged.
    ///
    /// Runs under `test_vfs` too: that arm reads as an oracle but
    /// returns the cached value, so clobbering the file still has to
    /// leave the base intact.
    #[test]
    fn the_cached_base_is_served_without_reading_the_manifest() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("must create temp dir");
        let mut tracker = test_tracker(&dir, 320, /*init=*/ true);

        tracker.flag_write(std::ptr::null(), 0, 4096);
        tracker.snapshot().expect("snapshot must succeed");
        let expected = tracker.read_snapshot_base().expect("must read");

        let staged = tracker
            .buffer
            .staged_manifest_path(&tracker.path)
            .expect("must derive path");
        let len = std::fs::metadata(&staged).expect("must stat").len() as usize;

        // Staged files are published 0444; a same-length overwrite
        // keeps (btime, len, dev, ino), so the identity check is
        // satisfied and only the contents are gone.
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o644))
            .expect("must chmod");
        std::fs::write(&staged, vec![0xffu8; len]).expect("must clobber");
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o444))
            .expect("must chmod back");

        assert!(
            tracker.read_snapshot_base().is_none(),
            "the test needs the on-disk manifest to be unreadable"
        );
        let base = tracker
            .current_snapshot_base()
            .expect("the cached base must be served without reading the file");
        assert_eq!(&*base, &*expected);
    }
}
