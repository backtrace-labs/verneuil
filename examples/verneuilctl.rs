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
/// The verneuilctl restore utility accepts the path to a verneuil
/// manifest file, and reconstructs its contents to the `--out`
/// argument (or stdout by default).
struct Restore {
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

    /// The path to the reconstructed output file.
    ///
    /// Defaults to stdout.
    #[clap(short, long, parse(from_os_str))]
    out: Option<PathBuf>,
}

fn restore(cmd: Restore, config: Options) -> Result<()> {
    let read_manifest = || {
        if let Some(path) = &cmd.manifest {
            match verneuil::manifest_bytes_for_path(None, path)? {
                Some(bytes) => Ok(bytes),
                None => Err(fresh_error!("manifest not found", ?path)),
            }
        } else if let Some(path) = &cmd.source_path {
            match verneuil::manifest_bytes_for_hostname_path(
                Some(&config),
                cmd.hostname.as_deref(),
                path,
            )? {
                Some(bytes) => Ok(bytes),
                None => Err(fresh_error!("unable to fetch manifest", ?cmd, ?config)),
            }
        } else {
            Err(fresh_error!(
                "One of `--manifest` or `--source_path` must be provided to `verneuilctl restore`",
                ?cmd
            ))
        }
    };

    let manifest_contents = read_manifest()?;
    // Use the global default target lists for the manifest and when
    // fetching its chunks.
    let (manifest, base) = verneuil::Manifest::decode_and_validate(
        &*manifest_contents,
        Default::default(),
        None,
        &cmd.manifest,
    )?;
    let snapshot = verneuil::Snapshot::new_with_default_targets(&manifest, base)?;
    let reader = snapshot.as_read(0, u64::MAX); // Read the whole thing.
    output_reader(reader, &cmd.out)
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
/// The verneuilctl sync utility accepts the path to a sqlite db, and
/// uploads a fresh snapshot to the configured replication targets.
///
/// On success, prints the manifest name to stdout.
struct Sync {
    /// The source sqlite database file.
    #[clap(parse(from_os_str))]
    source: PathBuf,

    /// Whether to optimize the database before uploading it.
    ///
    /// Databases are currently optimized by fixing the sqlite page
    /// size to 64 KB (ideal for Verneuil), and vacuuming the
    /// database.  Vacuuming makes the updated page size actually take
    /// effect, and garbage collects the database's contents.
    #[clap(short, long)]
    optimize: bool,
}

fn sync(cmd: Sync, config: Options) -> Result<()> {
    extern "C" {
        fn verneuil__cycle_db(path: *const std::os::raw::c_char, vacuum: bool) -> i32;
    }

    let dir: PathBuf = match &config.replication_spooling_dir {
        Some(dir) => dir.into(),
        None => {
            return Err(fresh_error!(
                "Replication must be enabled (replication_spooling_dir must be set).",
                ?config
            ))
        }
    };

    let cstr = std::ffi::CString::new(
        cmd.source
            .to_str()
            .ok_or_else(|| fresh_error!("--source is not a valid utf-8 string", ?cmd))?,
    )
    .map_err(|e| chain_error!(e, "--source could not be converted to a C string", ?cmd))?;

    tracing::info!(?dir, "flushing all spooled replication data");
    verneuil::copy_all_spool_paths(dir.clone(), /*best_effort*/ true)?;

    tracing::info!(?cmd.source, %cmd.optimize, "cycling a transaction on source db");
    let code = unsafe { verneuil__cycle_db(cstr.as_ptr(), cmd.optimize) };
    if code != 0 {
        return Err(fresh_error!("Failed to force a transaction on database", ?cmd.source, code));
    }

    tracing::info!(
        ?dir,
        "flushing all spooled replication data, with new snapshot"
    );
    verneuil::copy_all_spool_paths(dir, /*best_effort*/ false)?;

    let path = std::fs::canonicalize(&cmd.source)
        .map_err(|e| chain_error!(e, "failed to canonicalize database path", ?cmd))?;
    let manifest_name = verneuil::manifest_name_for_hostname_path(None, &path)
        .map_err(|e| chain_error!(e, "failed to construct manifest name", ?cmd))?;

    println!("{}", manifest_name);
    Ok(())
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
    }
}
