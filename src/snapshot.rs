//! A Verneuil snapshot is constructed from a `Manifest` object,
//! by fetching all its constituent chunks.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::fresh_error;
use crate::loader::Chunk;
use crate::loader::Loader;
use crate::manifest_schema::Manifest;
use crate::replication_target::ReplicationTarget;
use crate::result::Result;

pub struct Snapshot {
    len: u64,
    // The chunks map is a sorted map from the exclusive end of the
    // byte range covered by each chunk to the chunk of data.
    //
    // For example, `4 => chunk` with a chunk of 2 byte means that the
    // chunk represents bytes `[2, 4)` of the snapshotted file.
    chunks: BTreeMap<u64, Arc<Chunk>>,
}

/// A SnapshotReader implements `std::io::Read` for a parent `Snapshot`.
pub struct SnapshotReader<'parent> {
    // The current slice is either None, or non-empty.
    current_chunk: Option<&'parent [u8]>,
    // Byte offset where we want to stop reading (exclusive)
    end_byte_offset: u64,
    // Set to true once we have read up to `last_byte`.
    exhausted: bool,
    chunks: std::collections::btree_map::Range<'parent, u64, Arc<Chunk>>,
}

impl Snapshot {
    /// Constructs a `Snapshot` for `manifest` by fetching chunks
    /// from the global default replication targets.
    pub fn new_with_default_targets(manifest: &Manifest) -> Result<Snapshot> {
        let targets = crate::replication_target::get_default_replication_targets();

        Snapshot::new(Vec::new(), &targets.replication_targets, manifest)
    }

    /// Constructs a `Snapshot` for `manifest` by fetching chunks
    /// from `local_caches` directories and from `remote_sources`.
    pub fn new(
        local_caches: Vec<PathBuf>,
        remote_sources: &[ReplicationTarget],
        manifest: &Manifest,
    ) -> Result<Snapshot> {
        let fprints = crate::manifest_schema::extract_manifest_chunks(manifest)?;

        let loader = Loader::new(local_caches, remote_sources)?;
        let fetched = loader.fetch_all_chunks(&fprints)?;

        let mut len: u64 = 0;
        let mut chunks = BTreeMap::new();

        let find_chunk = |fprint| {
            fetched
                .get(fprint)
                .cloned()
                .ok_or_else(|| fresh_error!("unable to find chunk data", ?fprint, ?loader))
        };

        let mut insert_chunk = |chunk: Arc<Chunk>| {
            if !chunk.payload.is_empty() {
                len = len.checked_add(chunk.payload.len() as u64).ok_or_else(|| {
                    fresh_error!(
                        "total snapshot length overflowed",
                        len,
                        payload_len = chunk.payload.len()
                    )
                })?;
                chunks.insert(len, chunk);
            }

            Ok(())
        };

        if let Some((last, prefix)) = (&fprints).split_last() {
            for fprint in prefix {
                let chunk = find_chunk(fprint)?;

                // All but the last chunk must be exactly snapshot-sized.
                #[cfg(feature = "test_vfs")]
                assert_eq!(
                    chunk.payload.len(),
                    crate::tracker::SNAPSHOT_GRANULARITY as usize
                );

                insert_chunk(chunk)?;
            }

            let chunk = find_chunk(last)?;

            // The last chunk may be short.
            #[cfg(feature = "test_vfs")]
            assert!(chunk.payload.len() <= crate::tracker::SNAPSHOT_GRANULARITY as usize);

            insert_chunk(chunk)?;
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

        Ok(Snapshot { len, chunks })
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
    pub fn as_read(&self, initial_offset: u64, len: u64) -> SnapshotReader {
        // Fast path empty ranges.
        if initial_offset >= self.len() || len == 0 {
            return SnapshotReader {
                current_chunk: None,
                end_byte_offset: 0,
                exhausted: true,
                chunks: self.chunks.range(0..0),
            };
        }

        let end_byte_offset = initial_offset.saturating_add(len).clamp(0, self.len());

        // Find the first chunk that ends after `initial_offset`.
        let mut chunks = self.chunks.range((initial_offset + 1)..=u64::MAX);
        let current_chunk = chunks.next().and_then(|(end, x)| {
            let bytes = &*x.payload;
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

        SnapshotReader {
            current_chunk,
            end_byte_offset,
            exhausted: false,
            chunks,
        }
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
    fn read(&mut self, dst: &mut [u8]) -> std::io::Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }

        if self.current_chunk.is_none() {
            if self.exhausted {
                return Ok(0);
            }

            self.current_chunk = self.chunks.next().and_then(|(end, chunk)| {
                let bytes = SnapshotReader::drop_trailing_bytes(
                    &*chunk.payload,
                    *end,
                    self.end_byte_offset,
                );
                if bytes.is_empty() {
                    None
                } else {
                    Some(bytes)
                }
            });
            self.exhausted = self.current_chunk.is_none()
        }

        match self.current_chunk.as_mut() {
            Some(current) => {
                let ret = (*current).read(dst)?;

                if (*current).is_empty() {
                    self.current_chunk = None;
                }

                Ok(ret)
            }
            None => Ok(0),
        }
    }
}

#[cfg(test)]
fn create_chunk(bytes: Vec<u8>) -> Arc<Chunk> {
    use crate::manifest_schema::fingerprint_file_chunk;

    Arc::new(Chunk::new(fingerprint_file_chunk(&bytes), bytes).expect("must build: fprint matches"))
}

/// Exercise all the ways to get an empty reader.
#[test]
fn test_empty_reader_boundaries() {
    use std::io::Read;

    let first = create_chunk((0..4).collect());
    let second = create_chunk((4..10).collect());
    let snapshot = Snapshot {
        len: 10u64,
        chunks: [(4u64, first), (10u64, second)].iter().cloned().collect(),
    };

    let mut dst = Vec::new();
    // Zero-length reads.
    assert_eq!(
        snapshot
            .as_read(0, 0)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(10, 0)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(11, 0)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );

    // Past-the-end reads
    assert_eq!(
        snapshot
            .as_read(10, 1)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(11, 1)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );

    // Overflowing end offsets, starting past the end of the snapshot's data.
    assert_eq!(
        snapshot
            .as_read(10, u64::MAX)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(u64::MAX, 1)
            .read_to_end(&mut dst)
            .expect("read should never fail"),
        0
    );
    assert_eq!(
        snapshot
            .as_read(u64::MAX, u64::MAX)
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
        len: chunk.payload.len() as u64,
        chunks: [(chunk.payload.len() as u64, chunk)]
            .iter()
            .cloned()
            .collect(),
    };

    let mut read = snapshot.as_read(0, u64::MAX);
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        1_000_000
    );
    assert_eq!(base.len(), dst.len());
    assert_eq!(base, dst);
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
        chunks: [(4u64, first), (10u64, second)].iter().cloned().collect(),
    };

    let mut read = snapshot.as_read(0, u64::MAX);
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
                (end, create_chunk(((begin as u8)..(end as u8)).collect()))
            })
            .collect(),
    };

    for begin in 0..=flattened.len() {
        for end in begin..=(flattened.len() + 1) {
            use std::io::Read;

            let mut dst = Vec::new();

            assert_eq!(
                snapshot
                    .as_read(begin as u64, (end - begin) as u64)
                    .read_to_end(&mut dst)
                    .expect("read never fails"),
                flattened.len().min(end) - begin,
            );
            assert_eq!(&dst, &(&flattened)[begin..end.clamp(0, flattened.len())]);
        }
    }
}
