//! A Verneuil snapshot is constructed from a `Manifest` object,
//! by fetching all its constituent chunks.
use std::collections::BTreeMap;
use std::sync::Arc;

use kismet_cache::CacheBuilder;
use quinine::MonoArc;
use umash::Fingerprint;

use crate::chain_error;
use crate::fresh_error;
use crate::loader::Chunk;
use crate::loader::Loader;
use crate::manifest_schema::Manifest;
use crate::replication_target::ReplicationTarget;
use crate::result::Result;
use crate::tracker::SNAPSHOT_GRANULARITY;

/// When loading partially, always load at least this many chunks at
/// the beginning of the file.
///
/// With our 64 KB chunk size, 16 chunks means ~1 MB of data (and
/// sqlite pages compress well, often to 20% of the original size).
/// It's also in the same order of magnitude as the size of the loader
/// thread pool, so we don't waste too much time waiting to execute.
#[cfg(not(feature = "test_vfs"))]
const PARTIAL_LOAD_MIN_CHUNK_COUNT: usize = 16;

// Exercise demand loading more heavily in tests.
#[cfg(feature = "test_vfs")]
const PARTIAL_LOAD_MIN_CHUNK_COUNT: usize = 2;

#[derive(Clone, Debug)]
struct LazyChunk {
    fprint: Fingerprint,
    bytes: MonoArc<Chunk>,
}

/// How do we want to populate a snapshot's data.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotLoadingPolicy {
    // Let the implementation decide.
    #[default]
    Default,
    // Always fully load everything when creating the `Snapshot`.
    Eager,
    // Let the implementation load some things on demand, while
    // loading a chunk count in `[min, max]` (defaults to 0 and
    // an internal default never less than `min`).
    //
    // We always load the first and last chunk.
    Partial {
        min: Option<u64>,
        max: Option<u64>,
    },
}

pub struct Snapshot {
    len: u64,

    // The chunks map is a sorted map from the exclusive end of the
    // byte range covered by each chunk to the chunk of data.
    //
    // For example, `4 => chunk` with a chunk of 2 byte means that the
    // chunk represents bytes `[2, 4)` of the snapshotted file.
    chunks: BTreeMap<u64, LazyChunk>,

    // The chunk loader, if any.  This is `None` when we do not expect
    // to load chunks lazily.
    loader: Option<Box<Loader>>,

    // Keep a reference to the base chunk for the Manifest used to
    // create this `Snapshot`, if any: keeping it alive means we'll
    // get it from cache when we refresh the snapshot and find a
    // Manifest with the same base chunk.
    //
    // This field is only used to keep the `Chunk` alive.
    _base_chunk: Option<Arc<Chunk>>,
}

/// A SnapshotReader implements `std::io::Read` for a parent `Snapshot`.
pub struct SnapshotReader<'parent> {
    // The current slice is either None, or non-empty.
    current_chunk: Option<&'parent [u8]>,
    // Byte offset where we want to stop reading (exclusive).
    end_byte_offset: u64,
    // Number of bytes left in this reader.
    left_to_read: u64,
    chunks: std::collections::btree_map::Range<'parent, u64, LazyChunk>,
}

impl LazyChunk {
    fn from_chunk(chunk: Arc<Chunk>) -> Self {
        Self {
            fprint: chunk.fprint(),
            bytes: chunk.into(),
        }
    }

    /// Ensures we have a value in `bytes`, or errors out.
    ///
    /// Once this function returns successfully, `payload()` will
    /// never panic.
    fn ensure_loaded(&self, loader: Option<&Loader>) -> Result<()> {
        if self.bytes.is_some() {
            return Ok(());
        }

        // If we get here and `loader` is `None`, that's a programmer
        // error.
        let loader =
            loader.expect("loader should always be available when loading chunks on demand");
        let fprint = self.fprint;
        match loader
            .fetch_chunk(self.fprint)
            .map_err(|e| chain_error!(e, "failed to fetch chunk data on demand", ?fprint))?
        {
            // We assume lazily loaded chunks are all exactly
            // `SNAPSHOT_GRANULARITY` long when constructing the snapshot.
            Some(chunk) if chunk.payload().len() == SNAPSHOT_GRANULARITY as usize => {
                // If `store` fails, that's because the `MonoArc`
                // already holds a value; either way, our job's done.
                let _ = self.bytes.store(chunk);
                Ok(())
            }
            Some(chunk) => Err(
                fresh_error!("invalid chunk size found during on-demand loading",
                             fprint=?self.fprint,
                             actual=chunk.payload().len(),
                             expected=SNAPSHOT_GRANULARITY),
            ),
            None => {
                Err(fresh_error!("chunk not found during on-demand loading", fprint=?self.fprint))
            }
        }
    }

    fn payload(&self) -> &[u8] {
        self.bytes
            .as_ref()
            .expect("`ensure_loaded()` should be called before `payload()`")
            .payload()
    }
}

impl From<Arc<Chunk>> for LazyChunk {
    fn from(chunk: Arc<Chunk>) -> LazyChunk {
        LazyChunk::from_chunk(chunk)
    }
}

fn decide_fprints_to_fetch(
    policy: SnapshotLoadingPolicy,
    fprints: &[Fingerprint],
) -> Vec<Fingerprint> {
    let mut ret = Vec::new();

    if fprints.is_empty() {
        return ret;
    }

    match policy {
        // Map `Default` to `Eager` for now, in the interest of the
        // impact of lazy loading.
        SnapshotLoadingPolicy::Default | SnapshotLoadingPolicy::Eager => {
            // If we have to load everything, do so.
            ret.extend_from_slice(fprints);
        }
        SnapshotLoadingPolicy::Partial { min, max } => {
            // We know there's at least one chunk, and we always want
            // the first one (sqlite always reads it).
            let min = min.unwrap_or(0).clamp(1, fprints.len() as u64) as usize;
            let max = max
                .unwrap_or(PARTIAL_LOAD_MIN_CHUNK_COUNT as u64)
                .clamp(min as u64, usize::MAX as u64) as usize;

            // Aim to load ~sqrt(# chunks).
            let prefix_size = ((fprints.len() as f64).sqrt().ceil() as usize).clamp(min, max);

            ret.extend_from_slice(&fprints[0..prefix_size.clamp(0, fprints.len())]);

            // We always want the last chunk: it (and no other chunk)
            // could be shorter than a full SNAPSHOT_GRANULARITY, and
            // we assume lazily loaded chunks are exactly that size.
            //
            // However, we only want to add it when it's missing: the
            // caller assumes only the last fprint is the final one.
            if ret.len() < fprints.len() {
                ret.extend(fprints.last());
            }
        }
    }

    ret
}

lazy_static::lazy_static! {
    static ref DEFAULT_SNAPSHOT_LOADING_POLICY: std::sync::RwLock<SnapshotLoadingPolicy> =
        Default::default();
}

/// Sets the global default snapshot loading policy.
pub(crate) fn set_default_snapshot_loading_policy(policy: SnapshotLoadingPolicy) {
    *DEFAULT_SNAPSHOT_LOADING_POLICY.write().unwrap() = policy;
}

impl Snapshot {
    /// Constructs a `Snapshot` for `manifest` by fetching chunks
    /// from the global default replication targets.
    pub fn new_with_default_targets(
        load_policy: SnapshotLoadingPolicy,
        manifest: &Manifest,
        base_chunk: Option<Arc<Chunk>>,
    ) -> Result<Snapshot> {
        let targets = crate::replication_target::get_default_replication_targets();

        Snapshot::new(
            load_policy,
            CacheBuilder::new(),
            &targets.replication_targets,
            manifest,
            base_chunk,
        )
    }

    /// Constructs a `Snapshot` for `manifest` by fetching chunks
    /// from `local_caches` directories and from `remote_sources`.
    pub fn new(
        mut load_policy: SnapshotLoadingPolicy,
        cache_builder: CacheBuilder,
        remote_sources: &[ReplicationTarget],
        manifest: &Manifest,
        base_chunk: Option<Arc<Chunk>>,
    ) -> Result<Snapshot> {
        if load_policy == SnapshotLoadingPolicy::Default {
            load_policy = *DEFAULT_SNAPSHOT_LOADING_POLICY.read().unwrap();
        }

        // The load policy shouldn't affect correctness.  Apply an
        // arbitrary one in tests.
        #[cfg(feature = "test_vfs")]
        let load_policy = {
            use rand::Rng;

            let _ = load_policy;
            match rand::thread_rng().gen_range(0..3usize) {
                0 => SnapshotLoadingPolicy::Default,
                1 => SnapshotLoadingPolicy::Eager,
                _ => SnapshotLoadingPolicy::Partial {
                    min: None,
                    max: None,
                },
            }
        };

        let fprints = crate::manifest_schema::extract_manifest_chunks(manifest)?;
        let mut loader = Loader::new(cache_builder, remote_sources)?;

        // Go through all `BundledChunk`s in the manifest, and register
        // them in `loader`.
        if let Some(v1) = manifest.v1.as_ref() {
            v1.bundled_chunks
                .iter()
                .filter_map(|bundled| {
                    let fprint: umash::Fingerprint = bundled.chunk_fprint.as_ref()?.into();

                    Chunk::arc_from_cache(fprint)
                        .or_else(|| Some(Chunk::arc_from_bytes(&bundled.chunk_data)))
                })
                .for_each(|chunk| loader.adjoin_known_chunk(chunk));
        }

        let to_fetch = decide_fprints_to_fetch(load_policy, &fprints);
        let fetched = loader.fetch_all_chunks(&to_fetch)?;

        // Check that we fetched all the chunks we were looking for.
        // We assume the last value in `to_fetch` is always the last
        // chunk in the manifest in test-only assertions.
        if let Some((last, prefix)) = to_fetch.split_last() {
            let find_chunk = |fprint| {
                fetched
                    .get(fprint)
                    .cloned()
                    .ok_or_else(|| fresh_error!("unable to find chunk data", ?fprint, ?loader))
            };

            for fprint in prefix {
                let _chunk = find_chunk(fprint)?;
                // All but the last chunk must be exactly snapshot-sized.
                #[cfg(feature = "test_vfs")]
                assert_eq!(_chunk.payload().len(), SNAPSHOT_GRANULARITY as usize);
            }

            let _chunk = find_chunk(last)?;
            // The last chunk may be short.
            #[cfg(feature = "test_vfs")]
            assert!(_chunk.payload().len() <= SNAPSHOT_GRANULARITY as usize);
        }

        let mut len: u64 = 0;
        let mut chunks = BTreeMap::new();
        let mut any_missing_chunk = false;

        for fprint in fprints {
            let chunk_or = fetched.get(&fprint).cloned();
            let payload_len = chunk_or
                .as_deref()
                .map(|chunk| chunk.payload().len() as u64)
                .unwrap_or(SNAPSHOT_GRANULARITY);

            if chunk_or.is_none() {
                any_missing_chunk = true;
            }

            // If we definitely have an empty chunk, we can ignore it.
            if payload_len == 0 {
                continue;
            }

            len = len.checked_add(payload_len).ok_or_else(|| {
                fresh_error!("total snapshot length overflowed", len, payload_len)
            })?;

            chunks.insert(
                len,
                LazyChunk {
                    fprint,
                    bytes: chunk_or.into(),
                },
            );
        }

        let expected_len = crate::manifest_schema::extract_manifest_len(manifest)?;
        if len != expected_len {
            return Err(fresh_error!(
                "reconstructed file does not match source file length",
                len,
                expected_len,
                ?manifest
            ));
        }

        Ok(Snapshot {
            len,
            chunks,
            // If we found any chunk that hasn't been loaded yet, save
            // the loader for any on-demand loading.
            loader: if any_missing_chunk {
                Some(Box::new(loader))
            } else {
                None
            },
            _base_chunk: base_chunk,
        })
    }

    /// Returns whether the snapshot represents a zero-byte file.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the total size of the file in bytes.
    #[inline(always)]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns a stateful wrapper for `Snapshot` that implements
    /// `std::io::Read` for the snapshot's contents at bytes
    /// `[initial_offset, min(initial_offset + len, self.len()))`.
    #[inline]
    pub fn as_read(&self, initial_offset: u64, len: u64) -> Result<SnapshotReader> {
        // Fast path empty ranges.
        if initial_offset >= self.len() || len == 0 {
            return Ok(SnapshotReader {
                current_chunk: None,
                end_byte_offset: 0,
                left_to_read: 0,
                chunks: self.chunks.range(0..0),
            });
        }

        let end_byte_offset = initial_offset.saturating_add(len).clamp(0, self.len());

        // Iterate over all chunks that end after `initial_offset`,
        // and make sure any that overlaps with
        // `[initial_offset, end_byte_offset)` is loaded.
        let loader = self.loader.as_ref().map(AsRef::as_ref);
        for (end, lazy_chunk) in self.chunks.range((initial_offset + 1)..=u64::MAX) {
            lazy_chunk.ensure_loaded(loader)?;
            if *end >= end_byte_offset {
                break;
            }
        }

        // Find the first chunk that ends after `initial_offset`.
        let mut chunks = self.chunks.range((initial_offset + 1)..=u64::MAX);
        let current_chunk = chunks.next().and_then(|(end, x)| {
            let bytes = x.payload();
            let count = bytes.len() as u64;
            // Figure out how many bytes we can to skip between `begin`
            // and `initial_offset`.
            let begin = end.checked_sub(count)?;
            let skip = initial_offset.checked_sub(begin)?;
            let chop_front = bytes.split_at(skip.clamp(0, count) as usize).1;

            // And now drop any slop in the tail.
            let chop_tail = SnapshotReader::drop_trailing_bytes(chop_front, *end, end_byte_offset);

            // The remainder shouldn't be empty, because
            // `end_byte_offset > initial_offset`.
            debug_assert!(!chop_tail.is_empty());
            Some(chop_tail)
        });

        // `current_chunk` shouldn't be `None` because the iterator
        // shouldn't be empty: `initial_offset + 1 <= self.len()`, the
        // max key in `self.chunks`.
        debug_assert!(current_chunk.is_some());

        Ok(SnapshotReader {
            current_chunk,
            end_byte_offset,
            left_to_read: end_byte_offset - initial_offset,
            chunks,
        })
    }
}

impl SnapshotReader<'_> {
    /// Let `data` be a chunk of snapshot bytes that ends at
    /// `data_end` (exclusive); returns the prefix of `data` without
    /// trailing bytes that go past `end_byte_offset`.
    fn drop_trailing_bytes(data: &[u8], data_end: u64, end_byte_offset: u64) -> &[u8] {
        if data_end <= end_byte_offset {
            return data;
        }

        let to_drop = data_end - end_byte_offset;
        data.split_at((data.len() as u64).saturating_sub(to_drop) as usize)
            .0
    }
}

impl<'a> std::io::Read for SnapshotReader<'a> {
    fn read(&mut self, mut dst: &mut [u8]) -> std::io::Result<usize> {
        if dst.len() as u64 > self.left_to_read {
            dst = &mut dst[0..self.left_to_read as usize];
        }

        if dst.is_empty() {
            return Ok(0);
        }

        if self.current_chunk.is_none() {
            self.current_chunk = self.chunks.next().and_then(|(end, chunk)| {
                let bytes = SnapshotReader::drop_trailing_bytes(
                    chunk.payload(),
                    *end,
                    self.end_byte_offset,
                );
                if bytes.is_empty() {
                    None
                } else {
                    Some(bytes)
                }
            });
        }

        match self.current_chunk.as_mut() {
            Some(current) => {
                let ret = (*current).read(dst)?;

                assert!(ret as u64 <= self.left_to_read);
                self.left_to_read -= ret as u64;
                if (*current).is_empty() {
                    self.current_chunk = None;
                }

                Ok(ret)
            }
            None => Ok(0),
        }
    }
}

/// Creates a new chunk from `bytes`.  We don't need ownership, but
/// more rigid type propagation makes test code easier to write.
#[cfg(test)]
fn create_chunk(bytes: Vec<u8>) -> Arc<Chunk> {
    Arc::new(Chunk::new_from_bytes(&bytes))
}

/// Exercise all the ways to get an empty reader.
#[test]
fn test_empty_reader_boundaries() {
    use std::io::Read;

    let first = create_chunk((0..4).collect());
    let second = create_chunk((4..10).collect());
    let snapshot = Snapshot {
        len: 10u64,
        chunks: [(4u64, first.into()), (10u64, second.into())]
            .iter()
            .cloned()
            .collect(),
        loader: None,
        _base_chunk: None,
    };

    let mut dst = Vec::new();
    // Zero-length reads.
    assert_eq!(
        snapshot
            .as_read(0, 0)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(10, 0)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(11, 0)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );

    // Past-the-end reads
    assert_eq!(
        snapshot
            .as_read(10, 1)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(11, 1)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );

    // Overflowing end offsets, starting past the end of the snapshot's data.
    assert_eq!(
        snapshot
            .as_read(10, u64::MAX)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(u64::MAX, 1)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(u64::MAX, u64::MAX)
            .expect("as_read should not fail")
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
}

/// Create a snapshot that consists of a single long (1MB) chunk.
/// Make sure that we read it correctly, even when the copy loop makes
/// multiple calls for the same chunk.
#[test]
fn test_reader_simple() {
    let mut base = Vec::new();

    for i in 0..1_000_000 {
        base.push(((i * 2) % 256) as u8)
    }

    let chunk = create_chunk(base.clone());
    let snapshot = Snapshot {
        len: chunk.payload().len() as u64,
        chunks: [(chunk.payload().len() as u64, chunk.into())]
            .iter()
            .cloned()
            .collect(),
        loader: None,
        _base_chunk: None,
    };

    let mut read = snapshot
        .as_read(0, u64::MAX)
        .expect("as_read should not fail");
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        1_000_000
    );
    assert_eq!(base.len(), dst.len());
    assert_eq!(base, dst);
}

/// Create a snapshot that consists of one populated chunk surrounded
/// by two unpopulated ones.  Make sure that we read it correctly when
/// we stop *exactly* at the populated chunk.
#[test]
fn test_reader_one_chunk() {
    let mut base = Vec::new();

    for i in 0..1000 {
        base.push(((i * 2) % 256) as u8)
    }

    let chunk = create_chunk(base.clone());
    let snapshot = Snapshot {
        len: 2500,
        chunks: [
            (
                500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
            (1500, chunk.into()),
            (
                2500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
        ]
        .iter()
        .cloned()
        .collect(),
        loader: None,
        _base_chunk: None,
    };

    // Read from the middle of the populated chunk
    let mut read = snapshot.as_read(700, 800).expect("as_read should not fail");
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        800
    );
    assert_eq!(&dst, &base[200..]);
}

/// Create a snapshot that consists of one populated chunk surrounded
/// by two unpopulated ones.  Make sure that we read it correctly when
/// we stop just before then end of the populated chunk.
#[test]
fn test_reader_one_chunk_short() {
    let mut base = Vec::new();

    for i in 0..1000 {
        base.push(((i * 2) % 256) as u8)
    }

    let chunk = create_chunk(base.clone());
    let snapshot = Snapshot {
        len: 2500,
        chunks: [
            (
                500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
            (1500, chunk.into()),
            (
                2500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
        ]
        .iter()
        .cloned()
        .collect(),
        loader: None,
        _base_chunk: None,
    };

    let mut read = snapshot.as_read(501, 998).expect("as_read should not fail");
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        998
    );
    assert_eq!(&dst, &base[1..999]);
}

/// Create a snapshot that consists of one populated chunk surrounded
/// by two unpopulated ones.  Make sure that we read it correctly when
/// we start exactly at the populated chunk.
#[test]
fn test_reader_one_chunk_short_from_start() {
    let mut base = Vec::new();

    for i in 0..1000 {
        base.push(((i * 2) % 256) as u8)
    }

    let chunk = create_chunk(base.clone());
    let snapshot = Snapshot {
        len: 2500,
        chunks: [
            (
                500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
            (1500, chunk.into()),
            (
                2500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
        ]
        .iter()
        .cloned()
        .collect(),
        loader: None,
        _base_chunk: None,
    };

    let mut read = snapshot.as_read(500, 999).expect("as_read should not fail");
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        999
    );
    assert_eq!(&dst, &base[0..999]);
}

/// Create a snapshot that consists of one populated chunk surrounded
/// by two unpopulated ones.  Make sure that we read it correctly when
/// we read exactly the populated chunk.
#[test]
fn test_reader_one_chunk_exact() {
    let mut base = Vec::new();

    for i in 0..1000 {
        base.push(((i * 2) % 256) as u8)
    }

    let chunk = create_chunk(base.clone());
    let snapshot = Snapshot {
        len: 2500,
        chunks: [
            (
                500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
            (1500, chunk.into()),
            (
                2500,
                LazyChunk {
                    fprint: Fingerprint { hash: [0, 0] },
                    bytes: MonoArc::empty(),
                },
            ),
        ]
        .iter()
        .cloned()
        .collect(),
        loader: None,
        _base_chunk: None,
    };

    let mut read = snapshot
        .as_read(500, 1000)
        .expect("as_read should not fail");
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        1000
    );
    assert_eq!(&dst, &base);
}

/// Create a snapshot that merely consists of two short chunks.
/// Make sure that we correctly switch from one chunk to the next.
#[test]
fn test_reader_short_sequence() {
    let expected: Vec<u8> = (0..10).collect();

    let first = create_chunk((0..4).collect());
    let second = create_chunk((4..10).collect());

    let snapshot = Snapshot {
        len: 10u64,
        chunks: [(4u64, first.into()), (10u64, second.into())]
            .iter()
            .cloned()
            .collect(),
        loader: None,
        _base_chunk: None,
    };

    let mut read = snapshot
        .as_read(0, u64::MAX)
        .expect("as_read should not fail");
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        10
    );
    assert_eq!(expected.len(), dst.len());
    assert_eq!(expected, dst);
}

/// Create a snapshot of 6x6 bytes (6 because that lets us exercise
/// boundary handling, as well as fully internal ranges), and read
/// from it at different offsets and lengths; make sure we get the
/// correct bytes.
#[test]
fn test_reader_subseq() {
    const FRAGMENT_COUNT: usize = 6;
    const FRAGMENT_SIZE: usize = 6;
    let flattened: Vec<u8> = (0..(FRAGMENT_COUNT * FRAGMENT_SIZE) as u8).collect();

    let snapshot = Snapshot {
        len: (FRAGMENT_COUNT * FRAGMENT_SIZE) as u64,
        chunks: (0..FRAGMENT_COUNT)
            .map(|i| {
                let begin = (i * FRAGMENT_SIZE) as u64;
                let end = begin + FRAGMENT_SIZE as u64;
                (
                    end,
                    create_chunk(((begin as u8)..(end as u8)).collect()).into(),
                )
            })
            .collect(),
        loader: None,
        _base_chunk: None,
    };

    for begin in 0..=flattened.len() {
        for end in begin..=(flattened.len() + 1) {
            use std::io::Read;

            let mut dst = Vec::new();

            assert_eq!(
                snapshot
                    .as_read(begin as u64, (end - begin) as u64)
                    .expect("as_read should not fail")
                    .read_to_end(&mut dst)
                    .expect("read never fails"),
                flattened.len().min(end) - begin,
            );
            assert_eq!(&dst, &(&flattened)[begin..end.clamp(0, flattened.len())]);
        }
    }
}

#[test]
fn test_snapshot_loading_policy_json_default() {
    let json = r#""default""#;

    assert_eq!(
        serde_json::from_str::<SnapshotLoadingPolicy>(json).unwrap(),
        SnapshotLoadingPolicy::Default
    );
}

#[test]
fn test_snapshot_loading_policy_json_eager() {
    let json = r#"{"eager": null}"#;

    assert_eq!(
        serde_json::from_str::<SnapshotLoadingPolicy>(json).unwrap(),
        SnapshotLoadingPolicy::Eager
    );
}

#[test]
fn test_snapshot_loading_policy_json_partial_default() {
    let json = r#"{"partial": {}}"#;

    assert_eq!(
        serde_json::from_str::<SnapshotLoadingPolicy>(json).unwrap(),
        SnapshotLoadingPolicy::Partial {
            min: None,
            max: None
        }
    );
}

#[test]
fn test_snapshot_loading_policy_json_partial() {
    let json = r#"{"partial": { "max": 10}}"#;

    assert_eq!(
        serde_json::from_str::<SnapshotLoadingPolicy>(json).unwrap(),
        SnapshotLoadingPolicy::Partial {
            min: None,
            max: Some(10)
        }
    );
}

#[test]
fn test_snapshot_loading_policy_json_partial_full() {
    let json = r#"{"partial": { "min": 1, "max": 10}}"#;

    assert_eq!(
        serde_json::from_str::<SnapshotLoadingPolicy>(json).unwrap(),
        SnapshotLoadingPolicy::Partial {
            min: Some(1),
            max: Some(10)
        }
    );
}
