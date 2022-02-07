//! This module implements the test-only invariant checks for the
//! `Tracker`'s snapshot generation logic.
use std::sync::Arc;

use crate::chain_error;
use crate::loader::Chunk;
use crate::manifest_schema::fingerprint_sqlite_header;
use crate::manifest_schema::Manifest;
use crate::result::Result;

use super::Tracker;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ChunkSource {
    Consuming = 0,
    Ready = 1,
    Staged = 2,
}

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
            None,
        )
        .expect("failed to instantiate snapshot")
    }

    /// If the snapshot manifest exists, confirms that we can get
    /// every chunk in that snapshot.
    fn validate_snapshot(
        &self,
        manifest_or: crate::result::Result<Option<(Manifest, Option<Arc<Chunk>>)>>,
        source: ChunkSource,
    ) -> Result<()> {
        let manifest = match manifest_or {
            // If the manifest file can't be found, assume it was
            // replicated correctly, and checked earlier.
            Ok(None) => return Ok(()),
            Ok(Some((manifest, _))) => manifest,
            Err(err) => return Err(err),
        };

        self.fetch_snapshot_or_die(&manifest, source);
        Ok(())
    }

    /// Assert that the contents of ready and staged snapshots make
    /// sense (if they exist).
    #[cfg(feature = "test_validate_reads")]
    pub(super) fn validate_all_snapshots(&self) {
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
    pub(super) fn compare_snapshot(&self) -> Result<()> {
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
        let (manifest, _) = self
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
