//! A Verneuil snapshot is constructed from a `Directory` object,
//! by fetching all its constituent chunks.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::directory_schema::Directory;
use crate::fresh_error;
use crate::loader::Chunk;
use crate::loader::Loader;
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

/// A SnapshotReader implements `std::io::Read` for `Snapshot`.
pub struct SnapshotReader<'a> {
    current_chunk: Option<&'a [u8]>,
    chunks: std::collections::btree_map::Iter<'a, u64, Arc<Chunk>>,
}

impl Snapshot {
    /// Constructs a `Snapshot` for `directory` by fetching chunks
    /// from the global default replication targets.
    pub fn new_with_default_targets(directory: &Directory) -> Result<Snapshot> {
        let targets = crate::replication_target::get_default_replication_targets();

        Snapshot::new(Vec::new(), &targets.replication_targets, directory)
    }

    /// Constructs a `Snapshot` for `directory` by fetching chunks
    /// from `local_caches` directory and from `remote_sources`.
    pub fn new(
        local_caches: Vec<PathBuf>,
        remote_sources: &[ReplicationTarget],
        directory: &Directory,
    ) -> Result<Snapshot> {
        let fprints = crate::directory_schema::extract_directory_chunks(directory)?;

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
                #[cfg(feature = "verneuil_test_vfs")]
                assert_eq!(
                    chunk.payload.len(),
                    crate::tracker::SNAPSHOT_GRANULARITY as usize
                );

                insert_chunk(chunk)?;
            }

            let chunk = find_chunk(last)?;

            // The last chunk may be short.
            #[cfg(feature = "verneuil_test_vfs")]
            assert!(chunk.payload.len() <= crate::tracker::SNAPSHOT_GRANULARITY as usize);

            insert_chunk(chunk)?;
        }

        let expected_len = crate::directory_schema::extract_directory_len(directory)?;
        if len != expected_len {
            return Err(fresh_error!(
                "reconstructed file does not match source file length",
                len,
                expected_len,
                ?directory
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
    /// `std::io::Read` for the snapshot's contents.
    #[inline]
    pub fn as_read(&self) -> SnapshotReader {
        SnapshotReader {
            current_chunk: None,
            chunks: self.chunks.iter(),
        }
    }
}

impl<'a> std::io::Read for SnapshotReader<'a> {
    fn read(&mut self, dst: &mut [u8]) -> std::io::Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }

        if self.current_chunk.is_none() {
            self.current_chunk = self.chunks.next().map(|(_, chunk)| &*chunk.payload);
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
    use crate::directory_schema::fingerprint_file_chunk;

    Arc::new(Chunk::new(fingerprint_file_chunk(&bytes), bytes).expect("must build: fprint matches"))
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

    let mut read = snapshot.as_read();
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

    let mut read = snapshot.as_read();
    let mut dst = Vec::new();
    assert_eq!(
        std::io::copy(&mut read, &mut dst).expect("copy should succeed"),
        10
    );
    assert_eq!(expected.len(), dst.len());
    assert_eq!(expected, dst);
}
