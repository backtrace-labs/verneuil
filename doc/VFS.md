The Verneuil (replicating) VFS
==============================

The `verneuil` VFS opens local sqlite database files like the sqlite's
default `unix` VFS, and replicates them to a remote blob store when so
configured.  It is otherwise (nearly) identical[^no-dirsync] to the
unix VFS.

[^no-dirsync]: The Verneuil VFS never opens parent directories to fsync them.  It is thus safer to use it with the journaling mode set to `TRUNCATE` or `PERSIST`.

The Verneuil VFS can be loaded via the `libverneuil_vfs` runtime
extension for sqlite.  The extension obeys `RUST_LOG` conventions, and
sends tracing output to stderr (set `RUST_LOG=off` to silence the VFS).

When loaded as a runtime extension, the Verneuil VFS can only be
configured via the `VERNEUIL_CONFIG` environment variable.  The
variable's value must be set before loading the `libverneuil_vfs`
sqlite extension (ideally, before the program is even started).  If it
is empty or missing, Verneuil acts like a modernised version of the
default unix VFS, without any replication logic.

In order to enable replication, a JSON object must be stored
in `VERNEUIL_CONFIG`.  The object may also be stored in a file,
in which case `VERNEUIL_CONFIG=@/path/to/verneuil/config.json`
will read the JSON object at that path.

The JSON schema is "documented" by the `verneuil::Option` struct in
`lib.rs`.  Minimally, in order to enable replication, we must specify
where the replication data must be buffered, and where to send chunk
(content-addressed data) and manifest (snapshot metadata) blobs.

The chunk bucket is typically configured with versioning disabled and
a TTL of a few days or more (Verneuil attempts to "touch" live chunk
blobs once a day), while the manifest bucket should have versioning
enabled, and apply a TTL on old versions.

```
{
  "make_default": true,  // or false by default
  "replication_spooling_dir": "/tmp/verneuil",
  "replication_targets": [
    {
      "s3": {
        "region": "us-east-1",
        "chunk_bucket": "verneuil_chunks",
        "manifest_bucket": "verneuil_manifests",
        "domain_addressing": true  // or false for the legacy bucket-as-path interface
      }
    }
  ]
}
```

See the [aws-region](https://docs.rs/aws-region/latest/awsregion/enum.Region.html) crate for the list of supported region names.
Verneuil can also connect to custom S3-compatible servers.  For
example, we test with a local minio container with the following
replication target.

```
  "replication_targets": [
    {
      "s3": {
        "region": "minio",
        "endpoint": "http://127.0.0.1:7777",
        "chunk_bucket": "chunks",
        "manifest_bucket": "manifests",
        "domain_addressing": false,
        "create_buckets_on_demand": true
      }
    }
  ]
```

There may be multiple S3 replication targets in the list.  In that
case, Verneuil will attempt to send data to each pair of buckets in
the list, and only tret the operation as successful if all replication
targets succeed.

However, if there is no S3 replication target, replication always
fails (i.e., replication data remains in the buffer directory, within
size bounds).

Buffer flushing pragmas
-----------------------

By default, the Verneuil VFS synchronously pushes replication data to
a spooling directory, but does not wait for that data to make it to
remote storage.

Executing `pragma verneuil_flush_replication_data` with no argument,
or with "now", "force", or `2` will synchronously send any spooled
replication data for the connection's current (main) database.  The
pragma returns 1 on success and 0 on failure.

Verneuil can also flush when the connection is closed (disabled by
default).  Executing `pragma verneuil_flush_replication_data` with a
boolean argument (e.g., 1 for true, 0 for false) will set the flag
that determines whether to flush on close.  A true value will execute
a synchronous flush before closing the connection, and a false value
(default) will disable that behaviour.  The pragma returns 1 if the
behaviour was previously enabled, and 0 otherwise.

Deeper integration, with static linking
---------------------------------------

When an application links directly against the `verneuil` crate,
either as a Rust program or via the C interface, Verneuil is
configured programmatically, without going through the
`VERNEUIL_CONFIG` environment variable.

Rust programs should invoke `verneuil::configure`, and C programs
`verneuil_configure` (declared in `include/verneuil.h`).

When verneuil is built as a crate, it logs information via the
[tracing](https://docs.rs/tracing/latest/tracing/) crate, but does not
create any subscriber.  The embedding program must handle that part.

Local writable cache
--------------------

When a program reads from Verneuil replicas with the
`verneuil_snapshot` VFS (see `SNAPSHOT_VFS.md`) on the same machine
that generates the replication data (e.g., to execute long-running
read-only queries without blocking writes), it may make sense to
publish chunks to a local cache as they're uploaded.  Pointing the
`verneuil_snapshot` VFS to that cache (e.g., by using the same config
JSON for the replicating and the snapshot VFSes) will reduce the
number of calls to the remote blob store.

We can do that by adding a local writable cache to the list of
replication targets:

```
  "replication_targets": [
    {
      "local": {  // cache up to ~20_000 chunk files (64 KB each) in 100 shard
                  // subdirectories under `/tmp/verneuil-cache`.
        "directory": "/tmp/verneuil-cache",
        "num_shards": 100,
        "capacity": 20000
      }
    }
  ]
```

The cache is a directory tree, rooted in `directory`.  `num_shards`
specifies the number of cache subdirectories, and `capacity` a limit
for the total number of 64KB chunk files in the cache.  Ideally, each
shard is responsible for at most one thousand files, i.e., `capacity /
num_shards <= 1000`.  The actual number of files in the cache may
temporarily exceed the capacity, but never by more than 100%.
