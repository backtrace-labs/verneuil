use prost::Message;
use std::path::PathBuf;
use structopt::StructOpt;
use verneuil::chain_error;
use verneuil::fresh_error;
use verneuil::Result;

#[derive(Debug, StructOpt)]
#[structopt(
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
    #[structopt(short, long)]
    config: Option<String>,

    /// Log level, in the same format as `RUST_LOG`.  Defaults to
    /// only logging errors to stderr; `--log=info` increases the
    /// verbosity to also log info and warning to stderr.
    ///
    /// To fully disable logging, pass `--log=off`.
    #[structopt(short, long)]
    log: Option<String>,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Restore(Restore),
    ManifestName(ManifestName),
}

#[derive(Debug, StructOpt)]
/// The verneuilctl restore utility accepts the path to a verneuil
/// manifest file, and reconstructs its contents to the `--out`
/// argument (or stdout by default).
struct Restore {
    /// The manifest file that describes the snapshot to restore.
    ///
    /// These are typically stored as objects in versioned buckets;
    /// it is up to the invoker to fish out the relevant version.
    #[structopt(parse(from_os_str))]
    manifest: PathBuf,

    /// The path to the reconstructed output file.
    ///
    /// Defaults to stdout.
    #[structopt(short, long, parse(from_os_str))]
    out: Option<PathBuf>,
}

fn restore(cmd: Restore) -> Result<()> {
    let manifest_contents = std::fs::read(&cmd.manifest)
        .map_err(|e| chain_error!(e, "failed to read manifest file", path=?cmd.manifest))?;
    let manifest = verneuil::Manifest::decode(&*manifest_contents)
        .map_err(|e| chain_error!(e, "failed to parse manifest file", path=?cmd.manifest))?;
    let snapshot = verneuil::Snapshot::new_with_default_targets(&manifest)?;
    let mut reader = snapshot.as_read(0, u64::MAX); // Read the whole thing.

    if let Some(dst) = &cmd.out {
        let out_file = dst
            .file_name()
            .ok_or_else(|| fresh_error!("no file name in output path", ?dst))?;
        let out_dir = dst
            .parent()
            .ok_or_else(|| fresh_error!("output path has no file name", ?dst))?;
        let mut temp = tempfile::Builder::new()
            .prefix(out_file)
            .suffix(&format!(".{}.verneuilctl-restore-tmp", std::process::id()))
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

#[derive(Debug, StructOpt)]
/// The verneuilctl manifest-name utility accepts the path to a source
/// replicated file and an optional hostname, and prints the name of
/// the corresponding manifest file to stdout.
struct ManifestName {
    /// The path to the source file that was replicated by Verneuil.
    #[structopt(parse(from_os_str))]
    source: PathBuf,

    /// The hostname (/etc/hostname) of the machine that replicated
    /// that source file.  Defaults to the current hostname.
    #[structopt(short, long)]
    hostname: Option<String>,
}

fn manifest_name(cmd: ManifestName) -> Result<()> {
    println!(
        "{}",
        verneuil::manifest_name_for_hostname_path(
            cmd.hostname.as_ref().map(String::as_str),
            &cmd.source
        )
        .map_err(|e| chain_error!(e, "failed to construct manifest name", ?cmd))?
    );
    Ok(())
}

pub fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let opts = Opt::from_args();

    // Send tracing calls to stdout, and converts any log! call to
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
    let configure_replication = || {
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
        verneuil::configure_replication(config.clone())
            .map_err(|e| chain_error!(e, "failed to configure verneuil", ?config))?;
        Ok(())
    };

    match opts.cmd {
        Command::Restore(cmd) => {
            configure_replication()?;
            restore(cmd)
        }
        Command::ManifestName(cmd) => manifest_name(cmd),
    }
}
