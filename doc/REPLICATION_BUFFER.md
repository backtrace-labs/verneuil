Wait-free replication buffer
============================

Relevant code: `src/replication_buffer.rs`, `src/copier.rs`,
`src/tracker.rs`.

Verneuil associates a directory with each source database file, and
uses that directory as a buffer for replication data. For example, the
VFS's snapshot is stored in the "staging" subdirectory of the source
file's replication buffer. There can be a lot of files (e.g., each
chunk blob is stored as an independent file) in that buffer directory,
and we don't want to incur even more fsync per commit than
unreplicated sqlite. Thankfully, restarting replication from an empty
state is always a reasonable response to potential issues, so Verneuil
simply tags each replication buffer with a [boot id](https://www.kernel.org/doc/html/latest/admin-guide/sysctl/kernel.html#random):
the boot id is reset to a fresh unique value after each reboot
(graceful or otherwise), so Verneuil will automatically fail to find
any replication state after a reboot and start from scratch.

We can also avoid all explicit user-level locking in the VFS's change
tracking code because the VFS always updates the snapshot in its
"staging" subdirectory while holding a read lock on the source sqlite
file. The read lock lets other transactions make progress (until they
try to commit their writes), and guarantees that any other process
that's also updating the snapshot sees the same bytes in the sqlite
file. The change-tracking code only has to update the snapshot in a
manner that's safe with respect to application crashes: files are
always fully populated and made read-only before publishing them with
rename(2) or link(2).

This suffices to ensure synchronous change tracking (the replication
buffer's write side) can't get stuck behind a misbehaving process, or
at least not more so than sqlite already would. Worker threads that
copy files from disk to the remote blob store can also attempt to read
from the "staging" snapshot, and detect concurrent changes by
comparing the manifest file before and after uploading all chunks: the
change-tracking logic garbage collects unused chunks after publishing
new chunks and updating the manifest file.

Unfortunately, while reading from the "staging" snapshot is an
effective way to try and grab the most recent snapshot, it can lead to
livelock for copiers: there's no guarantee that a copier worker thread
can read and upload all the chunks a manifest file refers to before
the manifest is replaced with a new version.

The VFS's change tracking code thus also maintains a more stable
"ready" snapshot, which it only refreshes after it has been consumed
by a copier worker. Whenever the VFS notices that the "ready"
subdirectory is empty or absent, it copies the current snapshot (i.e.,
the current manifest file and all relevant chunks) to a temporary
directory, and atomically publishes that directory as the new "ready"
snapshot. Thus, even under constant write activity, copier worker
threads can make progress by consuming the "ready" snapshot and
uploading its contents to the blob store. These workers consume
"ready" snapshots by renaming the directory to "consuming" before
actually uploading the contents: triple buffering helps smooth out
replication hiccups when uploads are slow.

Copier workers don't need any explicit user-level synchronisation for
correctness either: renaming directories is atomic (and hardware
crashes are already handled with the bootid tag), and we tag "ready"
snapshots (and thus "consuming" snapshot) with a uuid sub-subdirectory
to make it easy to upload all chunks before uploading the
corresponding manifest. This uuid tag also makes it easy to guarantee
we always unlink a "consuming" subdirectory's manifest file after we
have uploaded that file to the blob store, and not a later
incarnation: we never repeat paths inside "ready" or "consuming"
subdirectories.

Of course, we would prefer to avoid redundant work, especially for
manifest blobs, where an ill-timed concurrent update could cause time
travel back to an older snapshot. That's why copiers try to avoid
concurrent uploads with advisory file locks, but will just keep going
if the acquisition times out.

Thanks to the triviality of squashing physical snapshots, we can rely
on a wait-free (in terms of atomic POSIX file operations) buffer for
replication to decouple the change tracking logic, which must run
synchronously with sqlite's transaction commit protocol, and the
copier threads that interact asynchronously with a remote blob store,
with few opportunities for feedback loops. Copiers will also reliably
observe fresh snapshots when writes are quiescent, by reading the
versioned "staging" snapshot, and make progress under constant write
load, by consuming the "ready" snapshot.
