use std::path::PathBuf;

use clap::Parser;
use verneuil::chain_error;
use verneuil::fresh_error;
use verneuil::Options;
use verneuil::Result;

#[derive(Debug, Parser)]
#[clap(
    name = "verneuilctl",
    about = "utilities to interact with Verneuil snapshots"
)]
/// In order to interact with Verneuil snapshots, verneuilctl must
/// know where writers are configured to upload their replication
/// data.  By default, verneuilctl looks for a configuration
/// string (either "@/path/to/file.json" or actual JSON) in the
/// `VERNEUIL_CONFIG` environment variable; this can be overridden
/// with the `--config` flag.
struct Opt {
    /// The Verneuil JSON configuration used when originally copying
    /// the database to remote storage.
    ///
    /// A value of the form "@/path/to/json.file" refers to the
    /// contents of that file; otherwise, the argument itself is the
    /// configuration string.
    ///
    /// This parameter is optional, and defaults to the value of the
    /// `VERNEUIL_CONFIG` environment variable.
    #[clap(short, long)]
    config: Option<String>,

    /// Log level, in the same format as `RUST_LOG`.  Defaults to
    /// only logging errors to stderr; `--log=info` increases the
    /// verbosity to also log info and warning to stderr.
    ///
    /// To fully disable logging, pass `--log=off`.
    #[clap(short, long)]
    log: Option<String>,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Restore(Restore),
    ManifestName(ManifestName),
    Manifest(Manifest),
    Flush(Flush),
    Sync(Sync),
    Shell(Shell),
    CheckCredentials(CheckCredentials),
}

/// Repeatedly run the S3 default-credential-chain pre-flight
/// (`verneuil::verify_credentials`) — the exact check the copier/loader do
/// before replicating — timing each attempt.  The pre-flight caches:
/// a resolved credential is served until near expiry, and a failure is
/// served for its backoff window, so expect one real resolution up
/// front and near-instant probes after it (cached failures are
/// labelled as such).  That is the behaviour the copier experiences;
/// this tool measures it, it does not bypass it.  Needs no `--config`.
/// Pass `--log aws_config=debug` to see the SDK's credential detail.
#[derive(Debug, Parser)]
struct CheckCredentials {
    /// How many times to probe the credential chain.
    #[clap(short, long, default_value_t = 20)]
    iterations: u32,

    /// Milliseconds to sleep between probes.
    #[clap(long, default_value_t = 200)]
    interval_ms: u64,
}

fn check_credentials(cmd: CheckCredentials) -> Result<()> {
    use std::time::{Duration, Instant};

    let (mut ok, mut fail) = (0u32, 0u32);
    for i in 0..cmd.iterations {
        let t0 = Instant::now();
        match verneuil::verify_credentials() {
            Ok(()) => {
                ok += 1;
                eprintln!("[{i:3}] OK   {:>8.1?}", t0.elapsed());
            }
            Err(e) => {
                fail += 1;
                eprintln!("[{i:3}] FAIL {:>8.1?}  {}", t0.elapsed(), e);
            }
        }
        if i + 1 < cmd.iterations {
            std::thread::sleep(Duration::from_millis(cmd.interval_ms));
        }
    }
    eprintln!(
        "check-credentials: {ok} ok / {fail} failed of {}",
        cmd.iterations
    );
    if fail > 0 {
        Err(fresh_error!("credential probe had failures"))
    } else {
        Ok(())
    }
}

// Writes the contents of `reader` to `out`, or stdout if `None`.
fn output_reader(mut reader: impl std::io::Read, out: &Option<PathBuf>) -> Result<()> {
    if let Some(dst) = out {
        let out_file = dst
            .file_name()
            .ok_or_else(|| fresh_error!("no file name in output path", ?dst))?;
        let out_dir = dst
            .parent()
            .ok_or_else(|| fresh_error!("output path has no file name", ?dst))?;
        let mut temp = tempfile::Builder::new()
            .prefix(out_file)
            .suffix(&format!(".{}.verneuilctl-tmp", std::process::id()))
            .tempfile_in(out_dir)
            .map_err(|e| chain_error!(e, "failed to create temporary file", ?dst))?;

        std::io::copy(&mut reader, temp.as_file_mut()).map_err(|e| {
            chain_error!(
                e,
                "failed to write snapshot contents to temporary file",
                ?dst,
                ?temp
            )
        })?;

        temp.persist(dst).map_err(|e| {
            chain_error!(
                e,
                "failed to persist temporary snapshot to out destination",
                ?dst
            )
        })?;
    } else {
        std::io::copy(&mut reader, &mut std::io::stdout())
            .map_err(|e| chain_error!(e, "failed to write snapshot contents to stdout"))?;
    }

    Ok(())
}

#[derive(Debug, Parser)]
/// The verneuilctl restore utility accepts the paths of one or more
/// verneuil manifest files (and/or replicated source files), and
/// reconstructs their contents.
///
/// A single database is written to `--out` (or stdout by default).
/// Several databases require `--out-dir` or `--in-place`, so that
/// each one has its own destination; they are then restored
/// independently -- a failure on one is reported on stderr and does
/// not stop the others, and the exit status is non-zero if any
/// failed.  Each successfully restored file's path is printed to
/// stdout, one per line, in input order (manifests first, then
/// source paths).
struct Restore {
    /// A manifest file that describes a snapshot to restore.  May be
    /// repeated to restore several snapshots.
    ///
    /// These are typically stored as objects in versioned buckets;
    /// it is up to the invoker to fish out the relevant version.
    ///
    /// If missing, verneuilctl restore will attempt to download it
    /// from remote storage, based on `--hostname` and `--source_path`.
    ///
    /// As special cases, an `http://` or `https://` prefix will be
    /// downloaded over HTTP(S), an
    /// `s3://bucket.region[.endpoint]/path/to/blob` URI will be
    /// loaded via HTTPS domain-addressed S3,
    /// `verneuil://machine-host-name/path/to/sqlite.db` will be
    /// loaded based on that hostname (or the current machine's
    /// hostname if empty) and source path, and a `file://` prefix
    /// will always be read as a local path.
    #[clap(short, long)]
    manifest: Vec<String>,

    /// The hostname of the machine that generated the snapshots.
    ///
    /// Defaults to the current machine's hostname.  Applies to every
    /// `--source_path`; to mix hostnames in one invocation, pass
    /// `verneuil://hostname/path` manifest URIs instead.
    #[clap(long)]
    hostname: Option<String>,

    /// The path to a source file that was replicated by Verneuil,
    /// when it ran on `--hostname`.  May be repeated to restore
    /// several databases.
    #[clap(short, long, parse(from_os_str))]
    source_path: Vec<PathBuf>,

    /// The path to the reconstructed output file.
    ///
    /// Defaults to stdout.  Only valid when restoring a single
    /// database; see `--out-dir` and `--in-place` for several.
    #[clap(short, long, parse(from_os_str))]
    out: Option<PathBuf>,

    /// Root directory under which each restored database is written
    /// to its own source path: with `--out-dir /tmp/stage`, a
    /// database replicated from /var/lib/app/db.sqlite is restored
    /// to /tmp/stage/var/lib/app/db.sqlite.  Parent directories are
    /// created as needed.
    ///
    /// Every input must carry a source path: `--source_path` inputs
    /// do, as do `verneuil://` manifest URIs; other `--manifest`
    /// forms do not, and are rejected up front.
    #[clap(long, parse(from_os_str))]
    out_dir: Option<PathBuf>,

    /// Restore each database over its own source path, i.e.
    /// `--out-dir /`.
    ///
    /// Each file is written to a temporary sibling and atomically
    /// renamed into place -- but that only protects processes that
    /// open the database *afterwards*.  Restoring over a database
    /// that a live process already has open yanks the data out from
    /// under it; only use this when nothing on this machine has the
    /// databases open.
    #[clap(long)]
    in_place: bool,

    /// How many databases to restore concurrently.
    ///
    /// Defaults to 1.  Chunk downloads share one small process-global
    /// thread pool no matter what this is set to, so higher values
    /// mostly overlap one database's decompression and output writing
    /// with another's downloads.  Note that every in-flight restore
    /// also stages its full database in the local chunk cache, so the
    /// transient disk footprint is roughly twice the combined size of
    /// the databases in flight.
    #[clap(short, long, default_value = "1")]
    jobs: usize,
}

fn restore(cmd: Restore, config: Options) -> Result<()> {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    let inputs: Vec<RestoreInput> = cmd
        .manifest
        .iter()
        .cloned()
        .map(RestoreInput::Manifest)
        .chain(
            cmd.source_path
                .iter()
                .cloned()
                .map(RestoreInput::SourcePath),
        )
        .collect();

    if inputs.is_empty() {
        return Err(fresh_error!(
            "One of `--manifest` or `--source_path` must be provided to `verneuilctl restore`",
            ?cmd
        ));
    }

    if cmd.in_place && cmd.out_dir.is_some() {
        return Err(fresh_error!(
            "`--in-place` and `--out-dir` are mutually exclusive",
            ?cmd
        ));
    }

    if cmd.out.is_some() && (cmd.in_place || cmd.out_dir.is_some()) {
        return Err(fresh_error!(
            "`--out` conflicts with `--out-dir` and `--in-place`",
            ?cmd
        ));
    }

    if inputs.len() > 1 && cmd.out_dir.is_none() && !cmd.in_place {
        return Err(fresh_error!(
            "restoring more than one database requires `--out-dir` or `--in-place` \
             (`--out` and stdout can only carry one)",
            ?cmd
        ));
    }

    // Resolve every destination up front: an input whose output path
    // cannot be derived is a usage error, and should reject the batch
    // before any network work, not after half of it restored.
    //
    // `mirrored` doubles as "print the destination on success": the
    // single-database `--out`/stdout modes stay silent, as they
    // always were.
    let mirrored = cmd.in_place || cmd.out_dir.is_some();
    let mut work: Vec<(&RestoreInput, Option<PathBuf>)> = Vec::with_capacity(inputs.len());
    {
        let mut seen = std::collections::HashSet::new();
        for input in &inputs {
            let out = if mirrored {
                let source = input.source_path()?;
                let out = match &cmd.out_dir {
                    // `--in-place`: the database's own path.
                    None => source,
                    Some(dir) => {
                        let relative = source.strip_prefix("/").unwrap_or(&source).to_owned();
                        dir.join(relative)
                    }
                };
                if !seen.insert(out.clone()) {
                    tracing::warn!(?input, ?out, "skipping duplicate database");
                    continue;
                }
                Some(out)
            } else {
                cmd.out.clone()
            };
            work.push((input, out));
        }
    }

    // `--jobs` workers pull databases off a shared cursor; each
    // result lands in its database's own slot, so stdout below stays
    // in input order whatever the completion order was.
    let results: Mutex<Vec<Option<Result<()>>>> =
        Mutex::new((0..work.len()).map(|_| None).collect());
    let next = AtomicUsize::new(0);
    let workers = cmd.jobs.max(1).min(work.len());
    let hostname = cmd.hostname.as_deref();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                match work.get(i) {
                    Some((input, out)) => {
                        let result = restore_one(input, out, hostname, &config);
                        results.lock().unwrap()[i] = Some(result);
                    }
                    None => break,
                }
            });
        }
    });

    // Restored paths on stdout, in input order.  `scope` joined every
    // worker, so a slot left empty means its worker died; count it
    // with the failures rather than pretending it restored.
    let mut failures: Vec<&RestoreInput> = Vec::new();
    for ((input, out), result) in work.iter().zip(results.into_inner().unwrap()) {
        match result {
            Some(Ok(())) => {
                if mirrored {
                    if let Some(out) = out {
                        println!("{}", out.display());
                    }
                }
            }
            _ => failures.push(input),
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        let num_failed = failures.len();
        let num_inputs = inputs.len();
        Err(fresh_error!(
            "failed to restore some databases",
            ?failures,
            num_failed,
            num_inputs
        ))
    }
}

/// One database to restore: a manifest path/URI, or a replicated
/// source file's path (resolved against `--hostname`).
#[derive(Debug)]
enum RestoreInput {
    Manifest(String),
    SourcePath(PathBuf),
}

impl RestoreInput {
    /// Returns the replicated file's path, when the input names one:
    /// `--out-dir` and `--in-place` need it for the destination.
    ///
    /// The `verneuil://` parse mirrors `manifest_bytes_for_path`:
    /// everything up to the first '/' is the hostname, the rest --
    /// with the leading '/' restored -- is the source path.
    fn source_path(&self) -> Result<PathBuf> {
        match self {
            RestoreInput::SourcePath(path) => Ok(path.clone()),
            RestoreInput::Manifest(uri) => {
                let suffix = uri.strip_prefix("verneuil://").ok_or_else(|| {
                    fresh_error!(
                        "cannot derive an output path for this manifest: only `verneuil://` \
                         manifest URIs carry a source path.  Use `--source_path`, or restore \
                         it by itself with `--out`.",
                        %uri
                    )
                })?;
                match suffix.split_once('/') {
                    Some((_machine, path)) => Ok(PathBuf::from(format!("/{}", path))),
                    None => Err(fresh_error!(
                        "failed to parse verneuil URI; should be verneuil://machine-hostname/path/to/sqlite.db",
                        %uri
                    )),
                }
            }
        }
    }
}

/// Restores one database to `out` (`None` means stdout).
fn restore_one(
    input: &RestoreInput,
    out: &Option<PathBuf>,
    hostname: Option<&str>,
    config: &Options,
) -> Result<()> {
    let (manifest_contents, manifest_or) = match input {
        RestoreInput::Manifest(path) => match verneuil::manifest_bytes_for_path(None, path)? {
            Some(bytes) => (bytes, Some(path.clone())),
            None => return Err(fresh_error!("manifest not found", ?path)),
        },
        RestoreInput::SourcePath(path) => {
            match verneuil::manifest_bytes_for_hostname_path(Some(config), hostname, path)? {
                Some(bytes) => (bytes, None),
                None => return Err(fresh_error!("unable to fetch manifest", ?path, ?config)),
            }
        }
    };

    // Use the global default target lists for the manifest and when
    // fetching its chunks.
    let (manifest, base) = verneuil::Manifest::decode_and_validate(
        &*manifest_contents,
        Default::default(),
        None,
        &manifest_or,
    )?;
    let snapshot = verneuil::Snapshot::new_with_default_targets(
        verneuil::SnapshotLoadingPolicy::Eager,
        &manifest,
        base,
    )?;
    let reader = snapshot.as_read(0, u64::MAX)?; // Read the whole thing.

    if let Some(parent) = out.as_ref().and_then(|dst| dst.parent()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| chain_error!(e, "failed to create output directory", ?parent))?;
    }

    output_reader(reader, out)
}

#[derive(Debug, Parser)]
/// The verneuilctl manifest-name utility accepts the path to a source
/// replicated file and an optional hostname, and prints the name of
/// the corresponding manifest file to stdout.
struct ManifestName {
    /// The path to the source file that was replicated by Verneuil.
    #[clap(parse(from_os_str))]
    source: PathBuf,

    /// The hostname (/etc/hostname) of the machine that replicated
    /// that source file.  Defaults to the current hostname.
    #[clap(long)]
    hostname: Option<String>,
}

fn manifest_name(cmd: ManifestName) -> Result<()> {
    println!(
        "{}",
        verneuil::manifest_name_for_hostname_path(cmd.hostname.as_deref(), &cmd.source)
            .map_err(|e| chain_error!(e, "failed to construct manifest name", ?cmd))?
    );
    Ok(())
}

#[derive(Debug, Parser)]
/// The verneuilctl manifest utility accepts the path to a source
/// replicated file and an optional hostname, and outputs the contents
/// of the corresponding manifest file to `--out`, or stdout by default.
struct Manifest {
    /// The path to the source file that was replicated by Verneuil.
    #[clap(parse(from_os_str))]
    source: PathBuf,

    /// The hostname (/etc/hostname) of the machine that replicated
    /// that source file.  Defaults to the current hostname.
    #[clap(long)]
    hostname: Option<String>,

    /// The path to the output manifest file.
    ///
    /// Defaults to stdout.
    #[clap(short, long, parse(from_os_str))]
    out: Option<PathBuf>,
}

fn manifest(cmd: Manifest, config: Options) -> Result<()> {
    let bytes = match verneuil::manifest_bytes_for_hostname_path(
        Some(&config),
        cmd.hostname.as_deref(),
        &cmd.source,
    )? {
        Some(bytes) => bytes,
        None => return Err(fresh_error!("unable to fetch manifest", ?cmd, ?config)),
    };

    output_reader(&*bytes, &cmd.out)
}

#[derive(Debug, Parser)]
/// The verneuilctl flush utility accepts the path to a spooling directory,
/// (i.e., a value for `verneuil::Options::replication_spooling_dir`), and
/// attempts to upload all the files pending replication in that directory.
struct Flush {
    /// The replication spooling directory prefix.
    #[clap(parse(from_os_str))]
    spooling: PathBuf,
}

fn flush(cmd: Flush) -> Result<()> {
    verneuil::copy_all_spool_paths(cmd.spooling, /*best_effort*/ false)
}

#[derive(Debug, Parser)]
/// The verneuilctl sync utility accepts the paths of one or more
/// sqlite dbs, and uploads a fresh snapshot of each to the configured
/// replication targets.
///
/// Only the listed databases' spooled replication data is touched:
/// syncing a few dbs out of a large spool does not walk, upload, or
/// contend with the others.  (The spool subtree is keyed on the
/// *canonical* path, so data spooled under a different spelling of the
/// same file -- a since-retargeted symlink, say -- is not this
/// invocation's to flush; `flush` drains everything.)  To flush every
/// database's pending data, use `flush`.
///
/// The databases are synced independently: a failure on one is
/// reported on stderr and does not stop the others, and the exit
/// status is non-zero if any failed.  On success, prints each
/// database's manifest name to stdout, one per line, in argument
/// order -- skipping failures and duplicate spellings of the same
/// file.
struct Sync {
    /// The source sqlite database files.
    #[clap(parse(from_os_str), required = true)]
    sources: Vec<PathBuf>,

    /// Whether to optimize the databases before uploading them.
    ///
    /// Databases are currently optimized by fixing the sqlite page
    /// size to 64 KB (ideal for Verneuil), and vacuuming the
    /// database.  Vacuuming makes the updated page size actually take
    /// effect, and garbage collects the database's contents.
    #[clap(short, long)]
    optimize: bool,

    /// How many databases to sync concurrently.
    ///
    /// Defaults to 1: one at a time spreads the upload load out while
    /// still paying the per-process setup -- credential resolution,
    /// client construction, DNS -- only once for the whole batch.
    /// Higher values trade burstier uploads and burstier source I/O
    /// for wall-clock time.
    #[clap(short, long, default_value = "1")]
    jobs: usize,
}

/// Syncs one database: drains its spool, forces a fresh snapshot,
/// and drains that too.  Returns the database's manifest name.
///
/// `path` is `source` canonicalized; the spool subtree is keyed on
/// the canonical path (that is what the tracker mangles into the
/// directory name), so the flushes and the manifest name use `path`,
/// while sqlite opens the file by the spelling it was given.
fn sync_one(
    dir: &std::path::Path,
    source: &std::path::Path,
    path: &std::path::Path,
    optimize: bool,
) -> Result<String> {
    extern "C" {
        fn verneuil__cycle_db(path: *const std::os::raw::c_char, vacuum: bool) -> i32;
    }

    let cstr = std::ffi::CString::new(
        source
            .to_str()
            .ok_or_else(|| fresh_error!("source is not a valid utf-8 string", ?source))?,
    )
    .map_err(|e| chain_error!(e, "source could not be converted to a C string", ?source))?;

    // Drain this db's spool first so the fresh snapshot below can claim
    // the single-occupancy "ready" slot: a tracker only publishes a new
    // ready buffer once the previous one has been fully uploaded.
    // Best-effort because a db that has never replicated has no spool
    // directory yet.
    tracing::info!(?dir, ?path, "flushing spooled replication data for db");
    verneuil::copy_db_spool_path(dir.to_owned(), path, /*best_effort*/ true)?;

    tracing::info!(?source, %optimize, "cycling a transaction on source db");
    let code = unsafe { verneuil__cycle_db(cstr.as_ptr(), optimize) };
    if code != 0 {
        return Err(fresh_error!(
            "Failed to force a transaction on database",
            ?source,
            code
        ));
    }

    tracing::info!(?dir, ?path, "flushing new snapshot for db");
    verneuil::copy_db_spool_path(dir.to_owned(), path, /*best_effort*/ false)?;

    verneuil::manifest_name_for_hostname_path(None, path)
        .map_err(|e| chain_error!(e, "failed to construct manifest name", ?source))
}

fn sync(cmd: Sync, config: Options) -> Result<()> {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    let dir: PathBuf = match &config.replication_spooling_dir {
        Some(dir) => dir.into(),
        None => {
            return Err(fresh_error!(
                "Replication must be enabled (replication_spooling_dir must be set).",
                ?config
            ))
        }
    };

    // Resolve every source to its canonical path up front, and keep
    // the first spelling of each file: syncing a db twice would only
    // cycle and upload it twice for nothing.  A path that cannot be
    // canonicalized counts as a failed sync, not a fatal error --
    // one bad entry in a batch must not strand the rest.
    let mut work: Vec<(&PathBuf, PathBuf)> = Vec::with_capacity(cmd.sources.len());
    let mut failures: Vec<&PathBuf> = Vec::new();
    {
        let mut seen = std::collections::HashSet::new();
        for source in &cmd.sources {
            match std::fs::canonicalize(source)
                .map_err(|e| chain_error!(e, "failed to canonicalize database path", ?source))
            {
                Ok(path) => {
                    if seen.insert(path.clone()) {
                        work.push((source, path));
                    } else {
                        tracing::warn!(?source, ?path, "skipping duplicate database");
                    }
                }
                Err(_) => failures.push(source),
            }
        }
    }

    // `--jobs` workers pull databases off a shared cursor; each
    // result lands in its database's own slot, so stdout below stays
    // in argument order whatever the completion order was.
    let results: Mutex<Vec<Option<Result<String>>>> =
        Mutex::new((0..work.len()).map(|_| None).collect());
    let next = AtomicUsize::new(0);
    let workers = cmd.jobs.max(1).min(work.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                match work.get(i) {
                    Some((source, path)) => {
                        let result = sync_one(&dir, source, path, cmd.optimize);
                        results.lock().unwrap()[i] = Some(result);
                    }
                    None => break,
                }
            });
        }
    });

    // Successes on stdout, in argument order.  `scope` joined every
    // worker, so a slot left empty means its worker died; count it
    // with the failures rather than pretending it synced.
    for ((source, _), result) in work.iter().zip(results.into_inner().unwrap()) {
        match result {
            Some(Ok(manifest_name)) => println!("{}", manifest_name),
            _ => failures.push(source),
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        let num_failed = failures.len();
        let num_sources = cmd.sources.len();
        Err(fresh_error!(
            "failed to sync some databases",
            ?failures,
            num_failed,
            num_sources
        ))
    }
}

#[derive(Debug, Parser)]
#[clap(setting = clap::AppSettings::TrailingVarArg)]
/// The verneuilctl shell utility accepts the path to a verneuil
/// manifest file, and opens the snapshot it describes in the sqlite
/// shell.
struct Shell {
    /// The manifest file that describes the snapshot to restore.
    ///
    /// These are typically stored as objects in versioned buckets;
    /// it is up to the invoker to fish out the relevant version.
    ///
    /// If missing, verneuilctl restore will attempt to download it
    /// from remote storage, based on `--hostname` and `--source_path`.
    ///
    /// As special cases, an `http://` or `https://` prefix will be
    /// downloaded over HTTP(S), an
    /// `s3://bucket.region[.endpoint]/path/to/blob` URI will be
    /// loaded via HTTPS domain-addressed S3,
    /// `verneuil://machine-host-name/path/to/sqlite.db` will be
    /// loaded based on that hostname (or the current machine's
    /// hostname if empty) and source path, and a `file://` prefix
    /// will always be read as a local path.
    #[clap(short, long)]
    manifest: Option<String>,

    /// The hostname of the machine that generated the snapshot.
    ///
    /// Defaults to the current machine's hostname.
    #[clap(long)]
    hostname: Option<String>,

    /// The path to the source file that was replicated by Verneuil,
    /// when it ran on `--hostname`.
    #[clap(short, long, parse(from_os_str))]
    source_path: Option<PathBuf>,

    /// The path to the sqlite3 shell executable; defaults to
    /// searching PATH for `sqlite3`.
    #[clap(short, long)]
    executable: Option<std::ffi::OsString>,

    /// The path to the libverneuil_vfs shared object; defaults to
    /// searching the `lib` directory in the executable's parent
    /// directory, then the executable's parent directory, and finally
    /// sqlite3's default search logic.
    #[clap(short, long)]
    vfs: Option<String>,

    /// Additional arguments for sqlite3.
    sqlite3_arguments: Vec<String>,
}

fn shell(cmd: Shell, config: Options) -> Result<()> {
    use std::os::unix::process::CommandExt;

    const VFS_NAME: &str = "libverneuil_vfs";

    // We have to URL encode the path we pass to sqlite3.
    const ESCAPED: percent_encoding::AsciiSet =
        percent_encoding::CONTROLS.add(b'%').add(b'?').add(b'&');

    // Looks for `libverneuil_vfs.so` in a sibling `lib` directory
    // (e.g., if we're at `.../bin/verneuilctl` and the vfs is at
    // `.../lib/libverneuil_vfs.so`), then in the same directory
    // as this executable.
    //
    // If that fails, let sqlite perform its own lookup.
    fn find_verneuil_vfs_path() -> Result<String> {
        let self_path = std::env::current_exe()
            .map_err(|e| chain_error!(e, "failed to get the path to verneuilctl"))?;
        let self_path = std::fs::canonicalize(&self_path)
            .map_err(|e| chain_error!(e, "failed to canonicalise self path", ?self_path))?;

        if let Some(parent) = self_path.parent().map(std::path::Path::parent).flatten() {
            let probe = parent.join("lib").join(format!("{}.so", VFS_NAME));

            if probe.exists() {
                return probe
                    .into_os_string()
                    .into_string()
                    .map_err(|e| chain_error!(e, "failed to stringify path"));
            }
        }

        if let Some(parent) = self_path.parent() {
            let probe = parent.join(format!("{}.so", VFS_NAME));

            if probe.exists() {
                return probe
                    .into_os_string()
                    .into_string()
                    .map_err(|e| chain_error!(e, "failed to stringify path"));
            }
        }

        Ok(VFS_NAME.to_string())
    }

    let source = if let Some(source) = cmd.manifest {
        source
    } else if let Some(path) = cmd.source_path {
        let canonical;

        format!(
            "verneuil://{}{}",
            cmd.hostname.as_deref().unwrap_or(""),
            if path.is_absolute() {
                path.to_string_lossy()
            } else {
                canonical = std::fs::canonicalize(&path)
                    .map_err(|e| chain_error!(e, "failed to canonicalize relative path", ?path))?;

                canonical.to_string_lossy()
            }
        )
    } else {
        return Err(fresh_error!(
            "One of `--manifest` or `--source_path` must be provided to `verneuilctl shell`",
            ?cmd
        ));
    };

    let vfs_path = if let Some(vfs) = cmd.vfs {
        vfs
    } else {
        find_verneuil_vfs_path()?
    };

    let target = cmd
        .executable
        .as_deref()
        .unwrap_or_else(|| std::ffi::OsStr::new("sqlite3"));

    let mut exec = std::process::Command::new(target);
    exec.env(
        verneuil::VERNEUIL_CONFIG_ENV_VAR,
        serde_json::to_string(&config).map_err(|e| chain_error!(e, "failed to unparse config"))?,
    )
    .arg("-cmd")
    .arg(format!(".load {}", vfs_path))
    .arg("-cmd")
    .arg(format!(
        ".open file:{}?vfs=verneuil_snapshot",
        percent_encoding::utf8_percent_encode(&source, &ESCAPED)
    ))
    .args(cmd.sqlite3_arguments);

    Err(exec.exec()).map_err(|e| chain_error!(e, "failed to spawn sqlite3 shell", ?target))
}

pub fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let opts = Opt::parse();

    // Send tracing calls to stderr, and convert any log! call to
    // traces.
    let filter = if let Some(log_level) = &opts.log {
        EnvFilter::try_new(log_level)
    } else {
        Ok(EnvFilter::from_default_env())
    }
    .expect("failed to parse --log level.");

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .compact()
        .init();

    let config_or = &opts.config;
    enum ApplyConfig {
        No,
        Replication,
        All,
    }

    let replication_config = |apply: ApplyConfig| {
        let config = if let Some(config) = config_or {
            verneuil::parse_configuration_string(config)
                .ok_or_else(|| fresh_error!("failed to parse --config"))?
        } else {
            let value = std::env::var(verneuil::VERNEUIL_CONFIG_ENV_VAR)
                .map_err(|e| chain_error!(e, "failed to fetch the value of VERNEUIL_CONFIG"))?;
            verneuil::parse_configuration_string(&value)
                .ok_or_else(|| fresh_error!("failed to parse VERNEUIL_CONFIG", %value))?
        };

        tracing::info!(?config, "parsed replication config");
        match apply {
            ApplyConfig::No => {}
            ApplyConfig::Replication => {
                verneuil::configure_replication(config.clone()).map_err(|e| {
                    chain_error!(e, "failed to configure verneuil replication", ?config)
                })?
            }
            ApplyConfig::All => verneuil::configure(config.clone())
                .map_err(|e| chain_error!(e, "failed to configure verneuil", ?config))?,
        }

        Ok(config)
    };

    match opts.cmd {
        Command::Restore(cmd) => restore(cmd, replication_config(ApplyConfig::Replication)?),
        Command::ManifestName(cmd) => manifest_name(cmd),
        Command::Manifest(cmd) => manifest(cmd, replication_config(ApplyConfig::No)?),
        Command::Flush(cmd) => flush(cmd),
        Command::Sync(cmd) => sync(cmd, replication_config(ApplyConfig::All)?),
        Command::Shell(cmd) => shell(cmd, replication_config(ApplyConfig::No)?),
        Command::CheckCredentials(cmd) => check_credentials(cmd),
    }
}
