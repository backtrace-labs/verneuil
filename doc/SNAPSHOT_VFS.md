The `verneuil_snapshot` VFS
===========================

The read-only snapshot (read replica) VFS is loaded and configured
exactly like the replicating Verneuil VFS (see `VFS.md`).  The
snapshot VFS can read data replicated by the Verneuil VFS by
configuring it with the same `replication_targets` list.  There is no
need to worry about accidentally overwriting data from a verneuil
snapshot connection: the sqlite connection is marked as read-only,
and the snapshot code only fetches data from blob stores, and never
interacts with the copier workers that upload data.

The snapshot VFS only needs the `replication_target` array in the
configuration JSON.  If there are multiple S3 targets, only the
first one is used.

```
{
  "replication_targets": [
    {
      "s3": {
        "region": "us-east-1",
        "chunk_bucket": "verneuil_chunks",
        "manifest_bucket": "verneuil_manifests",
        "domain_addressing": true  // or false for the legacy bucket-as-path interface
      }
    },
    {
      "local": {  // cache up to ~20_000 chunk files (64 KB each) in 100 shard
                  // subdirectories under `/tmp/verneuil-cache`.
        "directory": "/tmp/verneuil-cache",
        "num_shards": 100,
        "capacity": 20000
      }
    }
  ]
}
```

A local cache is not mandatory, but tends to dramatically reduce the
cost and latency of snapshot reconstruction.

The replication VFS can also probe multiple read-only caches before
fetching data from the blob store.  Such caches are useful when they
are populated by processes with a different permission set than the
readers.  When a local Verneuil replicating VFS is configured with
a `local` read-write cache in the `replication_targets` list, e.g.,

```
  "replication_targets": [
    ...
    {
      "local": {
        "directory": ...,
        "num_shards": ...,
        "capacity": ...
      }
    }
    ...
  ]
```

snapshot readers can access the cache in read-only mode by duplicating
the `directory` and `num_shard` keys in a `read_only` replication target:

```
  "replication_targets": [
    ...
    {
      "read_only": {
        "directory": ...,
        "num_shards": ...
      }
    }
    ...
  ]
```

A snapshot read may list multiple such `read_only` caches if it pulls
snapshots from multiple replicating programs with different
configurations.

Opening a snapshot
------------------

When Verneuil is loaded and configured, either as a runtime loadable
extension for sqlite, or as a statically linked library, the
`verneuil_snapshot` VFS is never made the default.  It must be
explicitly configured as the VFS when opening a database.  This can be
done by passing `verneuil_snapshot` as
[the `zVfs` argument to `sqlite3_open_v2`](https://www.sqlite.org/c3ref/open.html),
or with the `?vfs=verneuil_snapshot` querystring in a URI-style
connection string.

By default, the `verneuil_snapshot` VFS expects to open a manifest
file that describes the snapshot's contents.  This makes it possible
to consistently recreate the same snapshot.  The `verneuilctl
manifest` utility can fetch such a manifest file from the blob store.

It's usually more convenient to let the `verneuil_snapshot` VFS handle
fetching and updating the manifest.  We can do that by "opening" a
file path that starts with `verneuil://` string.  A path of the form
`verneuil://some.host.name/path/to/sqlite.db` will attempt to fetch a
recent manifest for the replicated sqlite db at `/path/to/sqlite.db`
on the machine `some.host.name`, and reconstruct the corresponding
snapshot.  When the hostname component is missing (i.e., the path
looks like `verneuil:///path/to/sqlite.db`), it defaults to the current
machine's hostname.

This syntax can be combined with sqlite's URI prefix, e.g.,
`file:verneuil://some.host.name/path/to/sqlite.db?vfs=verneuil_snapshot`.

The open call will block the current thread on network I/O when a
process opens a new snapshot, unless there is already a connection
opened for the same path.  If that's the case, the open call will
instead use the most recent snapshot available, without blocking.

Data freshness pragmas
----------------------

The `verneuil_snapshot` VFS can report on data freshness and update
its snapshot dynamically, while connections are open, via pragmas.

Executing `pragma verneuil_snapshot_ctime;` in a connection opened
with `verneuil_snapshot` returns the ctime (on the replicating
machine) of the source database file when the current snapshot was
taken, with nanosecond precision.  The `verneuil_snapshot_updated`
pragma instead returns the local unix timestamp at which the
current snapshot was fetched from remote storage.

Updating a snapshot can take several seconds for larger databases in
the hundreds of megabytes.  Connections can update their snapshot
without blocking, with the `verneuil_snapshot_async_reload` pragma:
executing this pragma will update the current snapshot if a fresher
one is available, and schedule a background snapshot update (unless
such an update is already in flight).  This pragma returns 1 if the
snapshot was updated, and 0 otherwise.

Applications may prefer to update snapshot more explicitly, perhaps in
their own worker threads.  The `verneuil_snapshot_refresh` pragma can
be more finely tuned for such use cases.  Invoking `pragma
verneuil_snapshot_refresh(0)` updates to the most recent available
snapshot, without blocking.  Changing the argument to
`verneuil_snapshot_refresh(1)` first attempts to synchronously fetch a
more recent snapshot (unless an update is already in flight), and
updates the connection to the most recent snapshot.  Finally,
`pragma verneuil_snapshot_refresh(2)` always fetches a new snapshot
nd updates the connection to the result.  Returns the local unix
timestamp at which the updated snapshot was fetched on success,
and an error string on failure.

By default, a connection uses the same snapshot until it is explicitly
updated with a pragma.  This can be changed with `pragma
verneuil_snapshot_auto_refresh(1);`, in which case the connection will
always switch to the most recent snapshot available when sqlite opens
a new transaction.

How are snapshot contents stored?
---------------------------------

An application may have multiple connections to similar snapshots for
the same database, taken at slightly different times.  The
`verneuil_snapshot` naturally deduplicates chunks, in order to avoid
downloading or storing redundant data.  The chunks are also always
stored in file-backed mappings, so the operating system is free to
flush them to disk when under memory pressure.  Multiple processes
sharing the same `local` cache replication target will also be able
to share the same file mappings, and thus not only save on API
calls, but also improve memory efficiency.

At a high level, the data for the snapshot VFS has the same footprint
as a temporary file.
