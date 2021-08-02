use std::path::PathBuf;
use structopt::StructOpt;

use verneuil::chain_error;
use verneuil::fresh_error;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "verneuil-restore",
    about = "Rebuilds a db snapshot from a verneuil directory file."
)]
/// The verneuil-restore utility accepts the path to a verneuil
/// directory file, and reconstructs its contents to the `--out`
/// argument (or stdout by default).
///
/// In order to recover the database's contents, verneuil-restore must
/// know where the writer was configured to upload its replication
/// data.  By default, verneuil-restore looks for a configuration
/// string (either "@/path/to/file.json" or actual JSON) in the
/// `VERNEUIL_CONFIG` environment variable; this can be overridden
/// with the `--config` flag.
struct Opt {
    /// The directory file that describes the snapshot to restore.
    ///
    /// These are typically stored as objects in versioned buckets;
    /// it is up to the invoker to fish out the relevant version.
    #[structopt(parse(from_os_str))]
    directory: PathBuf,

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

    /// The path to the reconstructed output file.
    ///
    /// Defaults to stdout.
    #[structopt(short, long, parse(from_os_str))]
    out: Option<PathBuf>,
}

pub fn main() -> verneuil::Result<()> {
    use prost::Message;
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

    let config = if let Some(config) = &opts.config {
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

    let directory_contents = std::fs::read(&opts.directory)
        .map_err(|e| chain_error!(e, "failed to read directory file", path=?opts.directory))?;
    let directory = verneuil::Directory::decode(&*directory_contents)
        .map_err(|e| chain_error!(e, "failed to parse directory file", path=?opts.directory))?;
    let snapshot = verneuil::Snapshot::new_with_default_targets(&directory)?;
    let mut reader = snapshot.as_read(0, u64::MAX); // Read the whole thing.

    if let Some(dst) = &opts.out {
        let out_file = dst
            .file_name()
            .ok_or_else(|| fresh_error!("no file name in output path", ?dst))?;
        let out_dir = dst
            .parent()
            .ok_or_else(|| fresh_error!("output path has no file name", ?dst))?;
        let mut temp = tempfile::Builder::new()
            .prefix(out_file)
            .suffix(&format!(".{}.verneuil-restore-tmp", std::process::id()))
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
