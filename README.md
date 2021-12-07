Verneuil: streaming replication for sqlite
==========================================

Verneuil[^verneuil-process] [[vɛʁnœj]](https://en.wikipedia.org/wiki/Auguste_Victor_Louis_Verneuil)
is a [VFS (OS abstraction layer)](https://www.sqlite.org/vfs.html) for
[sqlite](https://www.sqlite.org/index.html) that accesses local
database files like the default unix VFS while asynchronously
replicating snapshots to [S3](https://aws.amazon.com/s3/)-compatible
blob stores.  We wrote it to improve the scalability and availability
of pre-existing services for which sqlite is a good fit, at least for
single-node deployments.

[^verneuil-process]:  The [Verneuil process](https://en.wikipedia.org/wiki/Verneuil_method) was the first commercial method of manufacturing synthetic gemstones... and DRH insists on pronouncing sqlite like a mineral, surely a precious one (:

The primary design goal of Verneuil is to add asynchronous read
replication to working single-node systems without introducing new
catastrophic failure modes.  Avoiding new failure modes takes
precedence over all other considerations, including replication lag:
there is no attempt to bound or minimise the staleness of read
replicas.  Verneuil read replicas should only be used when stale
data is acceptable.

In keeping with this conservative approach to replication, the local
database file on disk remains the source of truth, and the VFS is
fully compatible with sqlite's default `unix` VFS, even for concurrent
(with file locking) accesses.  Verneuil stores all state that must
persist across sqlite transactions on disk, so multiple processes can
still access and replicate the same database with Verneuil.

Verneuil also paces all API calls (with a [currently hardcoded] limit
of 30 call/second/process) to avoid "surprising" cloud bills, and
decouples the sqlite VFS from the replication worker threads that
upload data to a remote blob store with a crash-safe buffer directory
that bounds its worst-case disk footprint to roughly four times the
size of the source database file.  It's thus always safe to disable
access to the blob store: buffered replication data may grow over
time, but always within bounds.

Replacing the default unix VFS with Verneuil impacts local sqlite
operations, of course: writes must be slower, in order to queue
updates for replication.  However, this slowdown is usually
proportional to the time it took to perform the write itself, and
often dominated by the *two* `fsync`s incurred by sqlite transaction
commits in rollback mode.  In addition, the additional replication
logic runs with the write lock downgraded to a read lock, so
subsequent transactions only block on the new replication step once
they're ready to commit.

This effort is incomparable with [litestream](https://github.com/benbjohnson/litestream/issues/8):
Verneuil is meant for asynchronous read replication, with streaming
backups as a nice side effect.  The
[replication approach](https://docs.google.com/document/d/173cfdvnVB_68No9vqmgKHc_SSd0Cy3-YmPaXZsVvXIg)
is thus completely different.  In particular, while litestream only
works with sqlite databases in WAL mode, Verneuil only supports
rollback journaling.  See `doc/DESIGN.md` for details.

What's in this repo
-------------------

1. A "Linux" VFS (`c/linuxvfs.c`) that implements everything that
   sqlite needs for a non-WAL DB, without all the backward
   compatibility cruft in sqlite's Unix VFS.  The new VFS's behaviour
   is fully compatible with upstream's Unix VFS!  It's a simpler
   starting point for new (Linux-only) sqlite VFSes.

2. A Rust crate with a C interface (see `include/verneuil.h`) to
   configure and register:

   - The `verneuil` VFS, which hooks into the Linux VFS to track changes,
     generate snapshots in spooling directories, and asynchronously
     upload spooled data to a remote blob store like S3.  This VFS is
     only compatible with sqlite's rollback journal mode.  It can be
     called directly as a Rust program, or via its C interface.

   - The `verneuil_snapshot` VFS that lets sqlite access snapshots stored
     in S3-compatible blob stores.

3. A runtime-loadable sqlite extension, `libverneuil_vfs`, that lets
   sqlite open databases with the `verneuil` VFS (to replicate the
   database to remote storage), or with the `verneuil_snapshot` VFS
   (to access a replicated snapshot).

4. The `verneuilctl` command-line tool to restore snapshots, forcibly
   upload spooled data, synchronise a database file to remote
   storage, and perform other ad hoc administrative tasks.

Quick start
-----------

There is more detailed setup information, including how to directly
link against the verneuil crate instead of loading it as a sqlite
extension, in `doc/VFS.md` and `doc/SNAPSHOT_VFS.md`.  The
`rusqlite_integration` example shows how that works for a Rust crate.

For quick hacks and test drives, the easiest way to use Verneuil is to
build it as a runtime loadable extension for sqlite
(`libverneuil_vfs`).

`cargo build --release --examples --features='dynamic_vfs'`

The `verneuilctl` tool will also be useful.

`cargo build --release --examples --features='vendor_sqlite'`

Verneuil needs additional configuration to know where to spool
replication data, and where to upload or fetch data from remote
storage.  That configuration data must be encoded in JSON, and will be
deserialised into a `verneuil::Options` struct (in `src/lib.rs`).

A minimal configuration string looks as follows.  See `doc/VFS.md` and
`doc/SNAPSHOT_VFS.md` for more details.

```
{
  // "make_default": true, to use the replicating VFS by default
  // "tempdir": "/my/tmp/", to override the location of temporary files
  "replication_spooling_dir": "/tmp/verneuil/",
  "replication_targets": [
    {
      "s3": {
        "region": "us-east-1",
        // "endpoint": "http://127.0.0.1:9000", //for non-standard regions
        "chunk_bucket": "verneuil_chunks",
        "manifest_bucket": "verneuil_manifests",
        "domain_addressing": true  // or false for the legacy bucket-as-path interface
        // "create_buckets_on_demand": true // to create private buckets as needed
      }
    }
  ]
}
```

That's a mouthful to pass as query string parameters to
`sqlite3_open_v2`, so Verneuil currently looks for that configuration
string in the `VERNEUIL_CONFIG` environment variable.  If that
variable's value starts with an at sign, like "@/path/to/config.json",
Verneuil looks for the configuration JSON in that file.

The configuration file does not include any credential: Verneuil
gets those from the environment, either by hitting the local EC2
credentials daemon, or by reading the `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` environment variables.

Now that the environment is set up, we can load the extension in
sqlite, and start replicating our writes to
[S3](https://aws.amazon.com/s3/), or any other compatible blob server
(we use [minio](https://min.io/) for testing).

```
$ RUST_LOG=warn VERNEUIL_CONFIG=@verneuil.json sqlite3
SQLite version 3.22.0 2018-01-22 18:45:57
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> .load ./libverneuil_vfs  -- Load the Verneuil VFS extension.
sqlite> .open file:source.db?vfs=verneuil
-- The contents of source.db will now be spooled for replication before
-- letting each transaction close.
sqlite> .open file:verneuil://source.host.name/path/to/replicated.db?vfs=verneuil_snapshot
-- opens a read replica for the most current snapshot replicated to s3 by `source.host.name`
-- for the database at `/path/to/replicated.db`.
```

Outside the sqlite shell, [extensions loading must be enabled](https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigenableloadextension)
in order to allow access to the [`load_extension` SQL function](https://www.sqlite.org/lang_corefunc.html#load_extension).

[URI filenames](https://www.sqlite.org/uri.html) must also be enabled
in order to specify the VFS in the connection string; it's also possible
to [pass a VFS argument to `sqlite3_open_v2`](https://www.sqlite.org/c3ref/open.html).

Replication data is buffered to the `replication_spooling_dir`
synchronously, before the end of each sqlite transaction.  Actually
uploading the data to remote storage happens asynchronously: we
wouldn't want to block transaction commit on network calls.

After exiting the shell or closing an application, we can make sure
that all spooled data is flushed to remote storage with `verneuilctl
flush $REPLICATION_SPOOLING_DIR`: this command will attempt to
synchronously upload all pending spooled data in the spooling
directory, and log noisily / error out on failure.

Find documentation for other `verneuilctl` subcommands with `verneuilctl help`:

```
$ ./verneuilctl --help
verneuilctl 0.1.0
utilities to interact with Verneuil snapshots

USAGE:
    verneuilctl [OPTIONS] <SUBCOMMAND>

FLAGS:
    -h, --help
            Prints help information

    -V, --version
            Prints version information


OPTIONS:
    -c, --config <config>
            The Verneuil JSON configuration used when originally copying the database to remote storage.

            A value of the form "@/path/to/json.file" refers to the contents of that file; otherwise, the argument
            itself is the configuration string.

            This parameter is optional, and defaults to the value of the `VERNEUIL_CONFIG` environment variable.
    -l, --log <log>
            Log level, in the same format as `RUST_LOG`.  Defaults to only logging errors to stderr; `--log=info`
            increases the verbosity to also log info and warning to stderr.

            To fully disable logging, pass `--log=off`.

SUBCOMMANDS:
    flush            The verneuilctl flush utility accepts the path to a spooling directory, (i.e., a value for
                     `verneuil::Options::replication_spooling_dir`), and attempts to upload all the files pending
                     replication in that directory
    help             Prints this message or the help of the given subcommand(s)
    manifest         The verneuilctl manifest utility accepts the path to a source replicated file and an optional
                     hostname, and outputs the contents of the corresponding manifest file to `--out`, or stdout by
                     default
    manifest-name    The verneuilctl manifest-name utility accepts the path to a source replicated file and an
                     optional hostname, and prints the name of the corresponding manifest file to stdout
    restore          The verneuilctl restore utility accepts the path to a verneuil manifest file, and reconstructs
                     its contents to the `--out` argument (or stdout by default)
    sync             The verneuilctl sync utility accepts the path to a sqlite db, and uploads a fresh snapshot to
                     the configured replication targets
$ ./verneuilctl restore --help
verneuilctl-restore 0.1.0
The verneuilctl restore utility accepts the path to a verneuil manifest file, and reconstructs its contents to the
`--out` argument (or stdout by default)

USAGE:
    verneuilctl restore [OPTIONS]

FLAGS:
        --help
            Prints help information

    -V, --version
            Prints version information


OPTIONS:
    -h, --hostname <hostname>
            The hostname of the machine that generated the snapshot.

            Defaults to the current machine's hostname.
    -m, --manifest <manifest>
            The manifest file that describes the snapshot to restore.

            These are typically stored as objects in versioned buckets; it is up to the invoker to fish out the relevant
            version.

            If missing, verneuilctl restore will attempt to download it from remote storage, based on `--hostname` and
            `--source_path`.

            As special cases, an `http://` or `https://` prefix will be downloaded over HTTP(S), an
            `s3://bucket.region[.endpoint]/path/to/blob` URI will be loaded via HTTPS domain-addressed S3,
            `verneuil://machine-host-name/path/to/sqlite.db` will be loaded based on that hostname (or the current
            machine's hostname if empty) and source path, and a `file://` prefix will always be read as a local path.
    -o, --out <out>
            The path to the reconstructed output file.

            Defaults to stdout.
    -s, --source-path <source-path>
            The path to the source file that was replicated by Verneuil, when it ran on `--hostname`

```

But why?
--------

Backtraces shards most of its backend metadata in a few thousand small
(1-2 MB) to medium size (up to 1-2 GB) sqlite databases, with an
average aggregate write rate of a few dozen write transactions per
second (with a few hot databases and many cold ones).  Before
Verneuil, this approach offered adequate performance and availability.
However, things could be better, and that's why we wrote Verneuil: to
distribute logic that can work with slightly stale read replicas and to
simplify our disaster recovery playbooks, without introducing new
failure modes in single-node code that already works well enough.

In fact, making sure replicas are up to date is explicitly not a goal.
Nevertheless, we find that once our backend reaches its steady state,
less than 0.1% of write transactions take more than 5 seconds to
replicate, and detect a replication lag of more than one minute for
more rarely than once every million write.  Of course, this all
depends on the write load and the number of replicated databases on a
machine or process.  For example, we experience temporary spikes in
replication lag whenever a service restarts and writes to a few
hundred databases in rapid succession.

Data freshness is not a goal because Verneuil prioritises disaster
avoidance over everything else.  That's why we interpose a wait-free
crash-safe replication buffer (implemented as files on disk) between
the snapshot update logic, which must run synchronously with sqlite
transaction commits, and the copier worker threads that upload
snapshot data to remote blob stores.  We trust this buffer to act as a
"data diode" that architecturally shuts off feedback loops from the
copier workers back to the sqlite VFS (i.e., back to the application).
Crucially, the amount of buffered data for a given sqlite data base is
bounded to a multiple of that database file's size, even when copiers
are completely stuck.  Even when the blob store is inaccessible or a
target bucket misconfigured, local operations will not be interrupted
by an ever-growing replication queue.  The buffer is also updated
without `fsync` calls that could easily impact the whole storage
subsystem; Verneuil instead achieves crash safety by discarding all
replication state after a reboot.

All too often, distributed solutions for scalability and availability
end up introducing new catastrophic failure modes, and the result is a
system that might offer resilience to rare (once a year or less)
events like hardware failure or power loss, but does so by increasing
complexity to a level such that unforeseen interactions between
correctly functioning pieces of code regularly cause protracted
customer impacting issues.  Verneuil's conservative approach gives us
some confidence that we can use it to improve the scalability and
availability of our preexisting centralised systems without worsening
the reliablity of everything that already works well enough.

Disaster avoidance includes bounding cloud costs.  Verneuil can
guarantee cost effectiveness for a wide range of update rate because
it's always able to throttle the API calls that update data: the
replication buffer will simply squash snapshots and always bound the
replication data's footprint to four times the size of the source
database file.

Regardless of the update pattern (frequency and number of databases),
we can count on Verneuil to remain within our budget for replication:
it will never average more than 30[^size-limit] API calls/replication
target/second/process.  Each call uploads either a chunk (64 KB for
incompressible data, less if zstd is useful), or a manifest (16 bytes
per 64 KB chunk in the source database file, so 512 KB for a 2 GB
file).

[^size-limit]: This hardcoded limit, coupled with the patrol logic that "touches" every extant chunk once a day, limits the total size of replicated databases for a single process: the replication logic may break down around 20-30 GB, but local operations should not be affected, except for the bounded growth in buffered replication data.  That's not an issue for us because we only store metadata in sqlite, metadata that tends to be orders of magnitude smaller than the data.

Chunks can be reaped by a retention rule that deletes them after a
week of inactivity (Verneuil attempts to "touch" useful chunks once a
day), so, even when there's a lot of churn, a
[chunk upload to a standard bucket in US East](https://aws.amazon.com/s3/pricing/)
costs at most $5e-6 + 64 K/1 GB * $0.023 / 4 (weeks per month) < $6e-6.

Manifests for multi-GB databases can be much larger, but manifest
updates are throttled to less than one per second per database, and
manifest blobs can be deleted more aggressively (e.g., as soon as a
version becomes stale).  With a 24h retention rule, uploading the
manifest for a 2 GB database adds up to less than $6e-6 for the API
call and churned storage.

We could also take into account storage costs for the permanent
footprint of the replicated databases ($0.023/GB/month for standard
buckets in US East) to this upper bound, but that's usually dominated
by API costs.

At an average rate of 30 upload/replication target/second/process, the
cost of churned data thus adds up to less than $15.55/replication
target/day/process.  There is usually only one replication target and
one replicating process per machine, so this translates into a
*maximum* of $15.55/day/machine (comparable to a c5.4xlarge).  In
practice, the average daily cost for Backtrace's backend fleet
(millions of writes a day scattered across a few thousand databases)
is on the order of $40/day.

How is it tested?
-----------------

In addition to simply running this in production to confirm that
regular single-node operations still work and that the code correctly
paces its API calls, we use sqlite's open source regression test
suite, after replacing the default Unix VFS with Verneuil.
Unfortunately, some tests assume WAL DBs are supported, so we have to
disable them; some others inject failures to exercise sqlite's failure
handling logic, those too must be disabled.  The resulting test suite
lives at https://github.com/pkhuong/sqlite/tree/pkhuong/no-wal-baseline

Configure a sqlite `build` directory from the mutilated test suite,
then run `verneuil/t/test.sh` to build test executables that load
the Verneuil VFS and make it the new default.  The test script also
spins up a local minio container for the Verneuil VFS.

In test mode, the VFS executes internal consistency checking code, and
panics whenever it notices a spooling or replication failure.

The logic for read replicas can't piggyback on top of the sqlite test
suite as easily. It is instead subjected to classic unit testing and
manual smoke testing.

What's missing for 1.0
----------------------

- Configurability: most of the plumbing is there to configure
  individual sqlite connections, but the current implementation is
  geared towards a program linking directly against libverneuil and
  configuring it with C calls.  We can already load Verneuil in
  sqlite by configuring it with an environment variable (which matches
  the current global configuration structure), but we should add
  support for reading configuration data from the connection string
  (sqlite query string parameters).

Things we should do after 1.0
-----------------------------

1. We currently always create the journal file in `0644`.  Umask applies,
   but it would make sense to implement the same logic as sqlite's Unix
   VFS and inherit the main db file's permissions.

2. The first page in a sqlite DB almost always changes after a write
   transaction.  Should we bundle it with the directory proto?

3. Many filesystems now support copy-on-write; we should think about
   using that for the commit step, instead of a journal file!

4. The S3 client library is really naive.  We should reuse HTTPS
   connections.

5. Consider some way to get chunks without indirecting through S3.
   Could gossip promising chunks ahead of time, or simply serve them
   on demand over request-response like HTTP.
