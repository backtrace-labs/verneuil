Incremental change tracking
===========================

Relevant code: `src/tracker.rs`.

Verneuil replication is based on snapshots, so it's always correct to
snapshot a database file's state from scratch.  However, that can be
slow, so the change tracking logic attempts to detect when it's safe
to update a pre-existing snapshot incrementally.

When the Verneuil VFS intercepts the beginning of a write transaction
commit (when sqlite acquires the write lock), it saves a summary of
the database's state.  Minimally, this state includes 
[sqlite's 32-bit change counter](https://www.sqlite.org/fileformat.html#file_change_counter);
when the filesystem supports extended attributes, we mix in a
Verneuil-specific version UUID; that UUID is replaced with a fresh
value before the first physical write in each transaction.

Once the transaction is fully committed, the change tracker then
compares that summary with that saved in the most recent snapshot.
Only if they match can that snapshot be updated incrementally.  The
VFS knows what file offsets it just updated in the previous write
transaction, and updates the snapshot's entries for the corresponding
chunks.  Everything else can stay as is.

This approach is safe against crashes, even after transaction commit,
because the updated snapshot is published to disk atomically.  Until
the snapshot is fully updated, a crash in the change tracking logic
will result in a mismatch between the source database file and the
most recent snapshot.  The next write transaction will detect the
mismatch and reconstruct a snapshot from scratch.

At first sight, the change counter should suffice; it is definitely
necessary in order to detect writes by other VFSes than Verneuil
(e.g., ad hoc updates with the sqlite shell).  However, sqlite may
write to the database file without updating the change counter.  For
example, when a large transaction is flushed to disk and aborted, the
change counter is left untouched, but pages in the freelist aren't
rolled back.  In theory, the contents of these pages is irrelevant, so
it doesn't matter that they're not replicated.  In practice, this makes
byte-level comparisons fail during testing, so we opportunistically
mix in a UUID.

Incremental replication is only invoked conservatively: never after a
reboot, nor immediately after [rolling back a hot journal](https://www.sqlite.org/atomiccommit.html#_rollback).
In the first case, Verneuil will fail to find any replication state,
since the replication buffer is tagged with a boot id.  In the second
case, the sqlite change counter will match, but the UUID xattr will
not.  In both cases, Verneuil will thus disregard any preexisting
snapshot and build a fresh one.
