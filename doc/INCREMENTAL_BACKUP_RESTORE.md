Incremental backup/restore with verneuilctl
===========================================

Verneuil is primarily used as a pair of loadable VFSes (one for the writer
and another for read replicas), but it also comes with `verneuilctl`, a
command-line utility that wraps the combination of sqlite and the VFSes.

The `verneuilctl` utility was always suitable for one-off backups from
scratch, and the VFS works for live replication.  With
https://github.com/backtrace-labs/verneuil/pull/30, `verneuilctl` is
now reasonable for incremental backups.

Building
--------

The `verneuilctl` utility is bundled as an example that depends on the
`vendor_sqlite` feature; build the utility with:

```
$ cargo build --release --examples --features vendor_sqlite
```

Setup
-----

Verneuil backs up and restores sqlite databases to S3 (or any
compatible service).  On an EC2 machine, Verneuil implicitly picks up
credentials from the instance's metadata. Otherwise, you can setup the
usual `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment
variables:

```
$ export AWS_ACCESS_KEY_ID=...
$ export AWS_SECRET_ACCESS_KEY=...
```

The credentials must give us access (read for restore, read and write
for backup) to *two* S3 buckets in the same region: one bucket will be
used for content-addressed "chunks", and the other for "manifests" with
names generated deterministically based on the source machine's hostname
and the database's local path.

At scale, the content-addressed "chunks" bucket usually has an
expiration policy; long-lived Verneuil worker threads attempt to touch
every chunk at least once a week, so expiring after a month or two of
inactivity is safe.

The "manifests" bucket should have versioning enabled, and the bucket
is usually configured to delete old versions (e.g., more than 1000
versions behind) automatically.

Assume the buckets are pre-created and accessible to the acount with
the access key.  We need a configuration JSON file to tell Verneuil
where to find the chunks and manifests:

```
$ cat /tmp/verneuil.json
{
  "make_default": true,
  "replication_spooling_dir": "/tmp/verneuil/",
  "replication_targets": [
    {
      "s3": {
        "region": "SOMETHING LIKE us-east-2 SET IT TO THE CORRECT VALUE",
        "chunk_bucket": "A BUCKET NAME LIKE my-verneuil-chunks",
        "manifest_bucket": "A BUCKET NAME LIKE my-verneuil-manifests",
        "domain_addressing": true
      }
    }
  ]
}
```

The configuration file also specifies the `replication_spooling_dir`; that's
a local storage directory where replication data and metadata will be stored.
This information doesn't have to survive machine crashes or reboots. In fact,
it's assumed that its contents are invalid after a reboot.

Prepare the data
----------------

We're finally ready to back up and restore SQLite databases.

For our demo, let's fill a database file with random data (https://antonz.org/random-table/):

```
 sqlite3 test.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> create table random_data as
with recursive tmp(x) as (
    select random()
    union all
    select random() from tmp
    limit 1000000
)
select * from tmp;
sqlite> ^D

```

Initial backup
--------------

We'll use the `verneuilctl sync` subcommand (run `verneuilctl --help` and `verneuilctl sync --help` for details).

```
$ time target/release/examples/verneuilctl -c @/tmp/verneuil.json sync test.db --optimize
5deb-verneuil%3Apk-m2-mba%3A8d12%2F%2Fhome%2Fpkhuong%2Fopen-sauce%2Fverneuil%2Ftest.db

real    0m13.981s
user    0m1.810s
sys     0m0.412s
```

The test data base spans 15.4 MB (245 pages at 64 KB each, plus the
header page).  The `sync` command wrote 490 blobs to the chunks
bucket, double what we expect: that's because the sync always starts
by backing up the current state of the database *before* running the
optimization command, `"PRAGMA page_size = 65536; VACUUM;"`.

It's possible to avoid that overhead by just running the command above
manually before the initial `sync`.  That leaves us with exactly the
number of chunks we expect (one per 64 KB page in the DB file, except
for the header page).  Alternatively, use the regular 4KB page size,
the only impact is that we might end up writing more chunks to S3.

Restore
-------

In order to restore a database, we must find a manifest, download the
corresponding chunks, and assemble them.  The `verneuilctl restore`
subcommand can reconstruct the manifest's blob name for a given
hostname and source path, and fetch the most recent version from S3:

```
$ time target/release/examples/verneuilctl -c @/tmp/verneuil.json restore --hostname pk-m2-mba --source-path /home/pkhuong/open-sauce/verneuil/test.db --out restore.db

real    0m4.693s
user    0m0.621s
sys     0m0.244s
$ sha256sum test.db restore.db
a895465a62e1afcdc95703b739c23699d8e9a56b7ee2d2b0e51dfa938b5e64e8  test.db
a895465a62e1afcdc95703b739c23699d8e9a56b7ee2d2b0e51dfa938b5e64e8  restore.db
```

Subsequent backups
------------------

Immediately running a sync for the same database no-ops quickly:

```
$ time target/release/examples/verneuilctl -c @/tmp/verneuil.json sync test.db
5deb-verneuil%3Apk-m2-mba%3A8d12%2F%2Fhome%2Fpkhuong%2Fopen-sauce%2Fverneuil%2Ftest.db

real    0m2.847s
user    0m0.008s
sys     0m0.015s
```

That's because verneuil compares the database with the metadata in the
spooling directory to avoid useless work.  If the spooling directory
is missing or was populated before the most recent boot, we'll always
sync from scratch.  However, the chunks bucket is content-addressed,
so this only wastes API calls and bandwidth, never persistent storage.

Let's perform a small update to the `test.db` file and sync it again.

```
$ sqlite3 test.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> .schema
CREATE TABLE random_data(x);
sqlite> select * from random_data limit 1;
-511094929343328393
sqlite> update random_data set x = 0 where x = -511094929343328393;
sqlite> ^D

```

```
$ time target/release/examples/verneuilctl -c @/tmp/verneuil.json sync test.db
5deb-verneuil%3Apk-m2-mba%3A8d12%2F%2Fhome%2Fpkhuong%2Fopen-sauce%2Fverneuil%2Ftest.db

real    0m1.995s
user    0m0.019s
sys     0m0.015s
```

This is a lot faster because the `sync` process still scans the whole
database file when the database has changed, but only uploads chunks
that have actually changed the manifest on disk.

Incremental restore
-------------------

The `verneuilctl` utility always writes out the whole database when
restoring.  We can however use a local cache directory to save
redundant fetches from S3.

We simply add this block to the `replication_targets` array in verneuil.json:

```
    {
      "local": {
        "directory": "/tmp/verneuil-cache/",
        "num_shards": 128,
        "capacity": 10000
      }
    }
```

This lets the *read-side* logic know to also look for chunks there
before hitting the S3 bucket, and to populate the local cache on
misses.  For now, writes only consider S3 replication targets.

```
$ cat /tmp/verneuil.json
{
  "make_default": true,
  "replication_spooling_dir": "/tmp/verneuil/",
  "replication_targets": [
    {
      "s3": {
        "region": "us-east-2",
        "chunk_bucket": "pkhuong-verneuil-chunks",
        "manifest_bucket": "pkhuong-verneuil-manifests",
        "domain_addressing": true
      }
    },
    {
      "local": {
        "directory": "/tmp/verneuil-cache/",
        "num_shards": 128,
        "capacity": 10000
      }
    }
  ]
}
```

The first time we restore with the cache enabled, the runtime is about
the same: the cache is empty.  The second time however, is much faster
because the chunks are cached locally.

```
$ time target/release/examples/verneuilctl -c @/tmp/verneuil.json restore --hostname pk-m2-mba --source-path /home/pkhuong/open-sauce/verneuil/test.db --out restore2.db

real    0m5.007s
user    0m0.646s
sys     0m0.297s
$ time target/release/examples/verneuilctl -c @/tmp/verneuil.json restore --hostname pk-m2-mba --source-path /home/pkhuong/open-sauce/verneuil/test.db --out restore3.db

real    0m0.179s
user    0m0.008s
sys     0m0.025s
$ sha256sum restore2.db restore3.db
5c40f7ea75ea6c02e0f3f1f6965e293a14966b6d42123fd4a8adfbb4a0c2f72a  restore2.db
5c40f7ea75ea6c02e0f3f1f6965e293a14966b6d42123fd4a8adfbb4a0c2f72a  restore3.db
```

Using the Verneuil (write) VFS
------------------------------

We can perform similar update through the Verneuil VFS, which will
both update the local file and replicate the changes on the fly.

But first, we have to build the vfs

```
$ cargo build --examples --release --features dynamic_vfs
```

```
$ VERNEUIL_CONFIG=@/tmp/verneuil.json sqlite3
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> .load target/release/examples/libverneuil_vfs.so
sqlite> .open test.db
sqlite> update random_data set x = 1 where rowid == 1;
sqlite> ^D

$ target/release/examples/verneuilctl flush /tmp/verneuil  # just in case we exited too quickly
$ target/release/examples/verneuilctl -c @/tmp/verneuil.json restore --hostname pk-m2-mba --source-path /home/pkhuong/open-sauce/verneuil/test.db --out restore2.db
$ sqlite3 restore2.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> select * from random_data limit 1;
2
sqlite> ^D

```

Using the Verneuil replica VFS
------------------------------

Once the loadable VFS is built, we can also use it for read replicas.
The `verneuilctl shell` subcommand simply runs the sqlite3 shell with
two pre-defined commands to load the VFS and open a remote read replica.

```
$ target/release/examples/verneuilctl -c @/tmp/verneuil.json shell --hostname pk-m2-mba --source-path /home/pkhuong/open-sauce/verneuil/test.db
sqlite> select * from random_data limit 2;
2
7377110096418126384
sqlite> update random_data set x = 2 where rowid == 1;
Runtime error: attempt to write a readonly database (8)
```

However, if another process (on the source machine!) opens the same
source data base with the write VFS, we'll be able to update the
replica in-place and observe the new writes.

```
 VERNEUIL_CONFIG=@/tmp/verneuil.json sqlite3
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> .load target/release/examples/libverneuil_vfs.so
sqlite> .open test.db
sqlite> update random_data set x = -2 where rowid == 2;
sqlite>

```

Back in the `verneuilctl shell`
```
sqlite> pragma verneuil_snapshot_refresh = 2;
1728153173.150809900
sqlite> select * from random_data limit 2;
2
-2
sqlite> 
```
