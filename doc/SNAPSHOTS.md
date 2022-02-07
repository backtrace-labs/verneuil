The snapshot format
===================

Relevant code: `src/manifest_schema.rs`, `src/tracker.rs`, and
`src/snapshot.rs`.

The Verneuil VFS maintains a byte-level snapshot of the source
database file because doing so simplifies correctness checks during
testing: we can literally compare (a cryptographic hash of) the
contents of a snapshot and of the sqlite file it's supposed to
match. Conceptually, recreating a snapshot is a stateless affair, so
we don't have to worry about complex ordering bugs that might be
hidden by the way our test code reconstructs snapshots after each
write.

Of course, it wouldn't be practical to copy and upload a
multi-gigabyte database after each transaction commit. The snapshot
format must allow for incremental updates.

That's why Verneuil snapshots have a two-level structure: a "manifest"
blob (of protobuf bytes) describes some of the source file's metadata
(e.g., its ctime), and refers to "chunk" blocks for the file's
contents. The manifest (see `src/manifest_schema.rs`) describes the
file's contents with an array of 128-bit
[UMASH](https://github.com/backtrace-labs/umash) fingerprints, one for
each 64 KB chunk in the file (chunks are always aligned to 64KB, and
the last one may be short). Manifests are usually uploaded to a
versioned bucket, with a retention rule that deletes superseded
versions after a few days.

The chunk blob that corresponds to each fingerprint can be found in a
content-addressed "chunks" bucket: we deterministically derive a blob
name from the fingerprint, and expect to find the corresponding bytes
in that blob.

Given this two-level structure, we only have to update and upload
chunks that surround mutations to the database file, and read replicas
will naturally be able to share cache storage for any chunk they have
in common. When updating a valid replication state, change tracking
will thus take time proportional to the number of bytes updated by the
write transaction plus the time to write out the updated manifest,
which is linear in the total size of the sqlite db, with an enviable
constant factor (16 bytes in the manifest file for each 64KB chunk in
the db file). The same scaling applies to copying the resulting
snapshot to remote blob stores.

Content addressing also gets us storage deduplication for free. That's
helpful because it means we can always restart replication from an
empty initial state, and only pay for that with buffer space and API
costs, but no long-term impact on the storage footprint. This gives us
a simple recovery mechanism whenever something might have gone wrong
with the replication logic (e.g., a process crashed while
reconstructing the snapshot): just ignore the old replication state.

Working with snapshots rather than change logs also makes it easy to
squash replication data.

The Verneuil VFS maintains a snapshot of each source database file by
storing chunks whenever a new dirty page is committed, and renaming a
new manifest file over the old one. From time to time, the VFS also
scans for chunks that don't appear in the current manifest file, and
delete them. The snapshot thus never grows more than approximately
twice as large as the source database file, regardless of progress (or
lack of) made by the copier worker threads.

Compressible snapshots
----------------------

Our representation of a snapshot's contents as a list of fingerprints
(128 bits for each 64 KB chunk) in manifest blobs works well for
database files up to a few megabytes.  Things aren't as nice for
larger databases.  For example, for a 1 GB database, each manifest
must include 256 KB of incompressible fingerprints.  Not disastrous,
but also not great.

We expect most manifests to change slowly over time: fully overwriting
a 1 GB database probably doesn't happen too often.  That's why we
attempt to make manifests compressible by `xor`-ing the list of
fingerprints with an optional "base chunk:" in the worst case, we
haven't made things any less compressible, but, if the base chunk is
similar to our manifest's list of fingerprints, the `xor` will create
compressible runs of zero bytes.  When such a base chunk is present,
we can undo the `xor` by applying it again.  If it's absent, there's
nothing to do (we `xor` with an infinite stream of zeros).

This `xor`-ing is simple, robust, and efficient.  There's only one
operation (`xor` in a base chunk) that acts as its self-inverse, we
can ignore any extra data in the base chunk, and conceptually pad the
base chunk withs 0s if necessary, and the `xor` is trivially
vectorisable.

Once a database file grows large enough, the Verneuil change tracker
will regularly generate a new base chunk, and try to reuse it while it
seems to roughly match the current list of fingerprints.

This approach scales fine for databases up to a few GBs, but probably
wouldn't be great at 10+ GB, at which point the flat list of
fingerprints becomes a liability.  However, sqlite itself probably
isn't a great fit at that point either.
