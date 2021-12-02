High-level design of Verneuil
=============================

The implementation of Verneuil follows from three design decisions
that leave very little freedom.

1. We rely on [sqlite's locking protocol](https://www.sqlite.org/lockingv3.html)
   to determine when the database file is in a consistent state.  WAL
   databases really consist of *two* files (the database file and the
   WAL); that would be more complex to replicate, so Verneuil only
   targets databases that use the default rollback journaling mode.
   Whenever sqlite downgrades a write lock, the database is in a
   consistent state, and thus ready to replicate.  That's why Verneuil
   intercepts write unlocks to stage a new snapshot for replication
   while holding a read lock.

2. We only work with (deduplicated) snapshots, rather than a diff
   stream, which also needs periodic snapshots.  The maximum page size
   for sqlite databases is 64KB (which is still small for a blob), so
   represent snapshots as small manifests that each refer to a list of
   64 KB chunks (except the last chunk, which may be shorter).
   Storing the chunks as content-addressed blobs gets us storage
   deduplication for free, without any coordination.  We only need
   additional logic to determine when we can update a snapshot's
   manifest incrementally (i.e., when it matches the state of the
   source sqlite db before the first write), and to then track the set
   of dirty chunks.  When in doubt, it's always safe to construct a
   snapshot from scratch: there will be a temporary cost for API
   calls, but content addressing guarantees that any redundant chunk
   upload will not increase our storage footprint.  See `SNAPSHOTS.md`
   for details.

3. We do not want remote systems (e.g., the target blob store for our
   replication data) to affect the code that runs sqlite transactions:
   programs should not suffer from new failure modes when they enable
   Verneuil, compared to only using the local sqlite database.
   Programs should also not have to give up the isolation benefits of
   multiprocessing just for read replication.  Thus, when Verneuil
   intercepts sqlite write unlocks, it only writes updated replication
   data to a wait-free (in terms of POSIX file operations)
   multiprocess/crash-safe buffer directory, and relies on dedicated
   worker threads to actually send that data to the blob store.  The
   buffer serves as a data diode to block feedback loops from workers
   back to the Verneuil VFS, and must thus avoid unbounded growth when
   workers can't make progress (e.g., the target bucket for
   replication data is misconfigured).  See `REPLICATION_BUFFER.md`
   for details.
