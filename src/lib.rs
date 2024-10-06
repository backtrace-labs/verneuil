mod atomic_kv32;
mod copier;
mod executor;
mod instance_id;
mod lazy_buckets; // For loader
mod loader;
mod manifest_schema;
mod ofd_lock;
mod process_id;
mod racy_time;
mod recent_work_set;
mod replication_buffer;
pub mod replication_target;
pub mod result; // Must be exposed for the helper functions
mod snapshot;
mod snapshot_vfs_ops;
mod sqlite_code;
mod sqlite_lock_level;
mod tracker;
mod unzstd;
mod vfs_ops;

use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;
use std::path::Path;

pub use copier::copy_spool_path;
pub use instance_id::hostname;
pub use manifest_schema::Manifest;
pub use replication_buffer::manifest_name_for_hostname_path;
pub use result::Result;
pub use snapshot::Snapshot;
pub use snapshot::SnapshotLoadingPolicy;

/// Read the verneuil configuration from this variable by default.
pub const VERNEUIL_CONFIG_ENV_VAR: &str = "VERNEUIL_CONFIG";

/// Initialization options for the Verneuil VFS.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct Options {
    /// If true, the Verneuil VFS overrides the default sqlite VFS.
    pub make_default: bool,

    /// All temporary file will live in this directory, or a default
    /// value if `None`.
    pub tempdir: Option<String>,

    /// If provided, temporary replication data will live in
    /// subdirectories of that spooling directory.
    pub replication_spooling_dir: Option<String>,

    /// Unix permissions for the spooling dir if we must create it.
    ///
    /// ORed with 0o700.
    pub replication_spooling_dir_permissions: Option<u32>,

    /// List of default replication targets.
    pub replication_targets: Vec<replication_target::ReplicationTarget>,

    /// Global policy when loading snapshot data.
    pub snapshot_loading_policy: SnapshotLoadingPolicy,
}

#[repr(C)]
pub struct ForeignOptions {
    pub make_default: bool,
    pub tempdir: *const c_char,
    pub replication_spooling_dir: *const c_char,
    pub replication_spooling_dir_permissions: u32,
    pub json_options: *const c_char,
}

// See `c/vfs.h`.
extern "C" {
    fn verneuil_configure_impl(options: *const ForeignOptions) -> i32;
    #[cfg(feature = "test_vfs")]
    fn verneuil_test_only_register() -> i32;
}

/// Attempts to parse the Verneuil config in `config`.  The string's
/// contents must be a config JSON for an `Options` struct, or a
/// "@/path/to/config_file.json".
#[tracing::instrument]
pub fn parse_configuration_string(config: &str) -> Option<Options> {
    if let Some(path) = config.strip_prefix('@') {
        let contents = match std::fs::read(path) {
            Ok(contents) => contents,
            Err(e) => {
                tracing::warn!(?e, %path, "failed to read verneuil configuration file");
                return None;
            }
        };

        match serde_json::from_slice(&contents) {
            Ok(parsed) => {
                tracing::info!(?parsed, %path, "found verneuil configuration");
                Some(parsed)
            }
            Err(e) => {
                tracing::warn!(?e, %path, "failed to parse verneuil configuration file");
                None
            }
        }
    } else {
        match serde_json::from_str(config) {
            Ok(parsed) => {
                tracing::info!(?parsed, "found verneuil configuration");
                Some(parsed)
            }
            Err(e) => {
                tracing::warn!(?e, %config, "failed to parse verneuil configuration string");
                None
            }
        }
    }
}

/// Attempts to load a default Verneuil configuration from the
/// `var_name_or` environment variable, or `VERNEUIL_CONFIG_ENV_VAR`
/// if `None`.  The variable's value should be a config JSON for
/// an `Options` struct, or "@/path/to/config_file.json".
#[tracing::instrument]
pub fn load_configuration_from_env(var_name_or: Option<&str>) -> Option<Options> {
    let var_name = var_name_or.unwrap_or(VERNEUIL_CONFIG_ENV_VAR);

    let os_value = std::env::var_os(var_name)?;
    let value = if let Some(value) = os_value.to_str() {
        value
    } else {
        tracing::warn!(?os_value, %var_name, "invalid value for verneuil configuration string");
        return None;
    };

    // With environment variables, it's sometimes easier to set an
    // empty value than to unset the variable.
    if value.is_empty() {
        return None;
    }

    parse_configuration_string(value)
}

/// Configures the replication subsystem, but not the sqlite VFS itself.
pub fn configure_replication(options: Options) -> Result<()> {
    if let Some(spooling_dir) = options.replication_spooling_dir {
        let mode = options.replication_spooling_dir_permissions.unwrap_or(0) | 0o700;

        replication_buffer::set_default_spooling_directory(
            Path::new(&spooling_dir),
            std::os::unix::fs::PermissionsExt::from_mode(mode),
        )?;
    }

    replication_target::set_default_replication_targets(options.replication_targets);
    snapshot::set_default_snapshot_loading_policy(options.snapshot_loading_policy);

    Ok(())
}

/// Configures the Verneuil VFS.
pub fn configure(options: Options) -> std::result::Result<(), i32> {
    let c_path;
    let mut foreign_options = ForeignOptions {
        make_default: options.make_default,
        tempdir: std::ptr::null(),
        replication_spooling_dir: std::ptr::null(),
        replication_spooling_dir_permissions: 0,
        json_options: std::ptr::null(),
    };

    if let Some(path) = &options.tempdir {
        c_path = CString::new(path.clone()).map_err(|_| -1)?;
        foreign_options.tempdir = c_path.as_ptr();
    }

    // The C VFS only cares about `make_default` and `tempdir`.
    let ret = unsafe { verneuil_configure_impl(&foreign_options) };
    if ret != 0 {
        return Err(ret);
    }

    configure_replication(options).map_err(|_| -1)?;
    Ok(())
}

/// This is the C-visible configuration function.
///
/// When `options_ptr` is NULL, attempts to load a configuration from the
/// `VERNEUIL_CONFIG` environment variable.
///
/// # Safety
///
/// Assumes the `options_ptr` is NULL or valid.
#[no_mangle]
pub unsafe extern "C" fn verneuil_configure(options_ptr: *const ForeignOptions) -> i32 {
    let mut options: Options;

    fn cstr_to_string(ptr: *const c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }

        let cstr = unsafe { CStr::from_ptr(ptr) }
            .to_str()
            .expect("string must be valid")
            .to_owned();
        Some(cstr)
    }

    if options_ptr.is_null() {
        options = load_configuration_from_env(None).unwrap_or_default();
    } else {
        let foreign_options = &*options_ptr;

        if let Some(json) = cstr_to_string(foreign_options.json_options) {
            match parse_configuration_string(&json) {
                Some(parsed) => options = parsed,
                None => return -1,
            }
        } else {
            options = Default::default();
        }

        options.make_default = foreign_options.make_default;
        if let Some(dir) = cstr_to_string(foreign_options.tempdir) {
            options.tempdir = Some(dir);
        }

        if let Some(dir) = cstr_to_string(foreign_options.replication_spooling_dir) {
            options.replication_spooling_dir = Some(dir);
        }

        if foreign_options.replication_spooling_dir_permissions != 0 {
            options.replication_spooling_dir_permissions =
                Some(foreign_options.replication_spooling_dir_permissions);
        }
    }

    match configure(options) {
        Ok(()) => 0,
        Err(code) => code,
    }
}

/// This test-only registration callback is invoked by the sqlite test
/// suite, and overrides the default and the Unix VFSes with the
/// Verneuil VFS for testing.  We define this wrapper in Rust because
/// cargo hides C definitions in cdylib builds.
///
/// # Safety
///
/// This function must only be called in `-DSQLITE_CORE` builds, and is
/// only expected to be invoked by sqlite's test harness.
#[no_mangle]
#[cfg(feature = "test_vfs")]
pub unsafe extern "C" fn sqlite3_verneuil_test_only_register(_: *const c_char) -> i32 {
    // Send tracing calls to stdout, and converts any log! call to
    // traces.
    let _ = tracing_subscriber::fmt::try_init();
    tracing::info!("tracing initialized");

    if let Err(code) =
        configure(load_configuration_from_env(None).expect("VERNEUIL_CONFIG must be populated"))
    {
        return code;
    }

    verneuil_test_only_register()
}

pub struct ReplicationProtoData {
    // Fingerprint of the sqlite db's header.
    pub header_fprint: umash::Fingerprint,
    // Fingerprint of the db's full contents.
    pub contents_fprint: umash::Fingerprint,
    // ctime of the snapshotted db file.
    pub ctime: std::time::SystemTime,

    // Manifest proto bytes.
    pub bytes: Vec<u8>,
}

impl ReplicationProtoData {
    /// Attempts to parse the replication proto in `bytes`, using
    /// the globally configured list of replication targets if it
    /// needs access to a base chunk.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the bytes could not be parsed.  Returns
    /// `Ok(None)` if the bytes encode protobuf data, but fail
    /// application-level validation.
    fn new(bytes: Vec<u8>) -> Result<Option<ReplicationProtoData>> {
        let v1 = match manifest_schema::Manifest::decode_and_validate(
            &bytes,
            Default::default(),
            None,
            "bytes",
        )?
        .0
        .v1
        {
            Some(v1) => v1,
            None => return Ok(None),
        };

        // The branches below are checked by `decode_and_validate`,
        // but it doesn't hurt to be safe.
        let header_fprint = if let Some(fprint) = v1.header_fprint {
            fprint.into()
        } else {
            return Ok(None);
        };
        let contents_fprint = if let Some(fprint) = v1.contents_fprint {
            fprint.into()
        } else {
            return Ok(None);
        };
        let ctime = if v1.ctime > 0 {
            std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::new(v1.ctime as u64, v1.ctime_ns as u32)
        } else {
            return Ok(None);
        };

        Ok(Some(ReplicationProtoData {
            header_fprint,
            contents_fprint,
            ctime,
            bytes,
        }))
    }
}

/// Attempts to return the name for the manifest proto blob associated
/// with `source_db`, and our local snapshot of that blob if available.
///
/// The contents of the file are looked up in a subdirectory of
/// `spool_prefix`, or in the default prefix if None.
pub fn current_replication_proto_for_db(
    source_db: &std::path::Path,
    spool_prefix: Option<std::path::PathBuf>,
) -> Result<(String, Option<ReplicationProtoData>)> {
    let manifest_path =
        replication_buffer::tapped_manifest_path_in_spool_prefix(spool_prefix, source_db)?;

    let blob_name = manifest_path
        .file_name()
        .expect("manifest_path must have file name")
        .to_str()
        .expect("url-encoded blob name must be valid utf-8")
        .to_string();

    let proto_data = (|| {
        let bytes = match std::fs::read(&manifest_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(chain_error!(
                    e,
                    "failed to read tapped manifest file",
                    ?manifest_path
                ))
            }
            Ok(bytes) => bytes,
        };

        ReplicationProtoData::new(bytes)
    })()?;

    Ok((blob_name, proto_data))
}

/// Attempts to fetch the latest manifest for the sqlite database at
/// `path` on `hostname_or` (on the current machine if None).
///
/// Fetches data from the replication targets in `config`, or from the
/// global list of of targets if None.
pub fn manifest_bytes_for_hostname_path(
    config: Option<&Options>,
    hostname_or: Option<&str>,
    path: &Path,
) -> Result<Option<Vec<u8>>> {
    use replication_buffer::tap_path_in_spool_prefix;

    let manifest_name = manifest_name_for_hostname_path(hostname_or, path)?;

    let default_targets;
    let (targets, tap_path) = match config {
        Some(options) => (
            &options.replication_targets,
            tap_path_in_spool_prefix(options.replication_spooling_dir.as_ref().map(|x| x.into())),
        ),
        None => {
            default_targets = replication_target::get_default_replication_targets();
            (
                &default_targets.replication_targets,
                tap_path_in_spool_prefix(None),
            )
        }
    };

    loader::fetch_manifest(
        &manifest_name,
        &tap_path
            .iter()
            .map(std::path::PathBuf::as_path)
            .collect::<Vec<&Path>>(),
        targets,
    )
}

fn retry_loop<T, E>(body: impl Fn() -> std::result::Result<T, E>) -> std::result::Result<T, E> {
    use rand::Rng;
    use std::time::Duration;

    let mut rng = rand::thread_rng();
    for i in 0..=3 {
        match body() {
            Ok(ret) => return Ok(ret),
            ret if i == 3 => return ret,
            _ => {
                let sleep = Duration::from_millis(100).mul_f64(10f64.powi(i));
                std::thread::sleep(sleep.mul_f64(rng.gen_range(1.0..2.0)));
            }
        }
    }

    unreachable!();
}

/// Attempts to fetch the manifest from `path`.
///
/// If `path` starts with `http://` or `https://`, the bytes are
/// downloaded over http(s) at that URL.
///
/// If `path` starts with `s3://` (followed by
/// `bucket-name.region[.endpoint]/path/to/blob`) the bytes are
/// downloaded over the S3 protocol and the default credentials.
///
/// If `path` starts with `verneuil://` (followed by
/// `source-machine-hostname/path/to/sqlite.db`), the bytes are
/// dowloaded for that manifest path in the configuration's
/// manifest buckets.  An empty hostname resolve to the current
/// machine.
///
/// Otherwise, or if the `path` starts with `file://`, returns the
/// contents of the local file.
pub fn manifest_bytes_for_path(config: Option<&Options>, path: &str) -> Result<Option<Vec<u8>>> {
    use std::io::ErrorKind;
    use std::time::Duration;
    const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30);

    if path.starts_with("http://") || path.starts_with("https://") {
        let response = retry_loop(|| attohttpc::get(path).timeout(DOWNLOAD_TIMEOUT).send())
            .map_err(|e| chain_warn!(e, "failed to download HTTP(S) manifest", %path))?;
        if response.status() == attohttpc::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Ok(Some(response.bytes().map_err(
                |e| chain_warn!(e, "HTTP request for manifest failed", %path),
            )?))
        }
    } else if let Some(path) = path.strip_prefix("s3://") {
        use s3::bucket::Bucket;
        use s3::creds::Credentials;

        let (bucket_region, blob) = match path.split_once('/') {
            Some(pair) => pair,
            None => {
                return Err(fresh_error!(
                    "failed to parse S3 URI; should be s3://bucket-name.region[.endpoint]/path-to-blob",
                    %path))
            }
        };

        let (bucket, region) = match bucket_region.split_once('.') {
            Some(pair) => pair,
            None => {
                return Err(fresh_error!(
                    "failed to parse S3 URI; should be s3://bucket-name.region[.endpoint]/path-to-blob",
                    %path))
            }
        };

        let creds =
            Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;

        let region = replication_target::parse_s3_region_specification(region, None);
        let mut bucket = Bucket::new(bucket, region, creds)
            .map_err(|e| chain_error!(e, "failed to create S3 bucket", path))?;
        bucket.set_subdomain_style();
        bucket.set_request_timeout(Some(DOWNLOAD_TIMEOUT));

        loader::load_from_source(&bucket, blob)
    } else if let Some(suffix) = path.strip_prefix("verneuil://") {
        let (machine, path) = match suffix.split_once('/') {
            Some((machine, path)) => (machine, format!("/{}", path)),
            None => {
                return Err(fresh_error!(
                "failed to parse verneuil URI; should be verneuil://machine-hostname/path/to/sqlite.db",
                %path))
            }
        };

        manifest_bytes_for_hostname_path(
            config,
            if machine.is_empty() {
                None
            } else {
                Some(machine)
            },
            Path::new(&path),
        )
    } else {
        let path = path.strip_prefix("file://").unwrap_or(path);

        match std::fs::read(path) {
            Ok(ret) => Ok(Some(ret)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(chain_error!(e, "failed to read manifest file", %path)),
        }
    }
}

/// Synchronously uploads all the data contained in the spooling
/// directory prefix.
///
/// If `best_effort`, succeed instead of erroring out when the
/// spooling directory does not exist.
pub fn copy_all_spool_paths(
    replication_spooling_dir: std::path::PathBuf,
    best_effort: bool,
) -> Result<()> {
    use rand::prelude::SliceRandom;
    use rayon::prelude::*;

    let mut to_copy = Vec::new();

    let current = replication_buffer::current_spooling_dir(replication_spooling_dir);
    if best_effort && !current.exists() {
        return Ok(());
    }

    for subdir in std::fs::read_dir(&current)
        .map_err(|e| chain_error!(e, "failed to list current spooling prefix", ?current))?
        .flatten()
    {
        if subdir.file_name().to_string_lossy().starts_with('#') {
            let mut spools: Vec<_> = std::fs::read_dir(subdir.path())
                .map_err(|e| chain_error!(e, "failed to list spooling directory", ?subdir))?
                .flatten()
                .map(|entry| Ok((entry.metadata()?.modified()?, entry.path())))
                .collect::<std::io::Result<_>>()
                .map_err(|e| chain_error!(e, "failed to stat subdirectory", parent=?subdir))?;

            // If there are multiple spool directories (for different
            // inodes), use the one that was modified most recently.
            spools.sort_unstable();

            if spools.len() > 1 {
                tracing::warn!(?spools, path=?subdir.path(), "found multiple inodes for the same source db");
            }

            if let Some(spool) = spools.pop() {
                to_copy.push(spool.1);
            }
        }
    }

    // We make sure to run in the Rayon pool for parallelism, and,
    // more importantly, to avoid any async context in the caller.
    to_copy.shuffle(&mut rand::thread_rng());
    to_copy
        .into_par_iter()
        .map(|path| copy_spool_path(&path))
        .collect::<Result<()>>()?;
    Ok(())
}

#[repr(C)]
pub struct ForeignReplicationInfo {
    blob_name: *mut c_char,

    header_fprint: [u64; 2],
    contents_fprint: [u64; 2],
    ctime: u64,
    ctime_ns: u32,

    num_bytes: usize,
    bytes: *mut c_char,
}

fn populate_replication_info(
    dst: &mut ForeignReplicationInfo,
    blob_name: String,
    info_or: Option<ReplicationProtoData>,
) {
    dst.blob_name = unsafe { CString::from_vec_unchecked(blob_name.into_bytes()) }.into_raw();

    if let Some(info) = info_or {
        dst.header_fprint = info.header_fprint.hash;
        dst.contents_fprint = info.contents_fprint.hash;

        let ctime = info
            .ctime
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        dst.ctime = ctime.as_secs();
        dst.ctime_ns = ctime.subsec_nanos();

        let mut bytes = info.bytes.into_boxed_slice();
        dst.num_bytes = bytes.len();
        dst.bytes = bytes.as_mut_ptr() as *mut _;

        // Leak `bytes`: we will reconstruct it in
        // `verneuil_replication_info_deinit`.
        let _ = Box::into_raw(bytes);
    }
}

/// Populates `dst_ptr` with the replication information for
/// sqlite file `c_db`, inside the `c_prefix` spooling directory
/// (or `NULL` for the global default).
///
/// # Safety
///
/// This function assumes its arguments are valid pointers.
#[no_mangle]
pub unsafe extern "C" fn verneuil_replication_info_for_db(
    dst_ptr: *mut ForeignReplicationInfo,
    c_db: *const c_char,
    c_prefix: *const c_char,
) -> i32 {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    if dst_ptr.is_null() {
        tracing::error!("invalid dst (null)");
        return -1;
    }

    let dst = dst_ptr.as_mut().expect("should be non-null");
    *dst = std::mem::zeroed();

    if c_db.is_null() {
        tracing::error!("invalid db argument (null)");
        return -1;
    }

    let db = CStr::from_ptr(c_db);
    let prefix = if c_prefix.is_null() {
        None
    } else {
        Some(Path::new(OsStr::from_bytes(CStr::from_ptr(c_prefix).to_bytes())).to_path_buf())
    };

    let (blob_name, info_or) = match current_replication_proto_for_db(
        Path::new(OsStr::from_bytes(db.to_bytes())),
        prefix.clone(),
    ) {
        Err(e) => {
            let _ = chain_error!(e, "failed to fetch replication info for db", ?db, ?prefix);
            return -1;
        }
        Ok(ret) => ret,
    };

    populate_replication_info(dst, blob_name, info_or);
    0
}

/// Populates `dst_ptr` with the replication information for
/// manifest `manifest_name` in our remote replication targets.
///
/// # Safety
///
/// This function assumes its arguments are valid pointers.
#[no_mangle]
pub unsafe extern "C" fn verneuil_replication_info_for_manifest(
    dst_ptr: *mut ForeignReplicationInfo,
    c_manifest_name: *const c_char,
) -> i32 {
    if dst_ptr.is_null() {
        tracing::error!("invalid dst (null)");
        return -1;
    }

    let dst = dst_ptr.as_mut().expect("should be non-null");
    *dst = std::mem::zeroed();

    if c_manifest_name.is_null() {
        tracing::error!("invalid manifest_name (null)");
        return -1;
    }

    let manifest_name = match CStr::from_ptr(c_manifest_name).to_str() {
        Ok(name) => name.to_string(),
        Err(e) => {
            let _ = chain_error!(e, "hostname is invalid utf-8");
            return -1;
        }
    };

    let targets = replication_target::get_default_replication_targets();
    let tap_path = replication_buffer::tap_path_in_spool_prefix(None);
    let info_or = match loader::fetch_manifest(
        &manifest_name,
        &tap_path
            .iter()
            .map(std::path::PathBuf::as_path)
            .collect::<Vec<&Path>>(),
        &targets.replication_targets,
    ) {
        Ok(Some(bytes)) => match ReplicationProtoData::new(bytes) {
            Ok(info_or) => info_or,
            Err(e) => {
                let _ = chain_error!(e, "failed to parse manifest bytes", %manifest_name, ?targets);
                return -1;
            }
        },
        Ok(None) => None,
        Err(e) => {
            let _ = chain_error!(e, "failed to fetch manifest bytes", %manifest_name, ?targets);
            return -1;
        }
    };

    populate_replication_info(dst, manifest_name, info_or);
    0
}

/// Releases resources owned by `info_ptr`
///
/// # Safety
///
/// This function assumes its argument is a valid pointer or NULL.
#[no_mangle]
pub unsafe extern "C" fn verneuil_replication_info_deinit(info_ptr: *mut ForeignReplicationInfo) {
    if let Some(info) = info_ptr.as_mut() {
        if !info.blob_name.is_null() {
            std::mem::drop(CString::from_raw(info.blob_name));
        }

        if !info.bytes.is_null() {
            let slice = std::slice::from_raw_parts_mut(info.bytes, info.num_bytes);
            std::mem::drop(Box::from_raw(slice))
        }

        *info = std::mem::zeroed();
    }
}

/// Returns the name of the manifest blob for `c_hostname` and `c_path`, as a C string.
///
/// # Safety
///
/// Assumes that `c_hostname` is NULL or a valid C string, and that
/// `c_path` is a valid C string.
#[no_mangle]
pub unsafe extern "C" fn verneuil_manifest_name_for_hostname_path(
    c_hostname: *const c_char,
    c_path: *const c_char,
) -> *mut c_char {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    let hostname = if c_hostname.is_null() {
        None
    } else {
        Some(CStr::from_ptr(c_hostname))
    };

    let path_str = if c_path.is_null() {
        return std::ptr::null_mut();
    } else {
        CStr::from_ptr(c_path)
    };

    let path = PathBuf::from(OsStr::from_bytes(path_str.to_bytes()));

    let hostname_str = match hostname.map(|cstr| cstr.to_str()).transpose() {
        Ok(str_or) => str_or,
        Err(e) => {
            let _ = chain_error!(e, "hostname is invalid utf-8");
            return std::ptr::null_mut();
        }
    };

    match manifest_name_for_hostname_path(hostname_str, &path) {
        Ok(name) => CString::new(name.into_bytes())
            .expect("URI-encoded path should not contain NUL")
            .into_raw(),
        Err(e) => {
            let _ = chain_error!(e, "failed to construct manifest name", ?hostname_str, ?path);
            std::ptr::null_mut()
        }
    }
}

/// Releases the `CStr` that backs `name` if non-NULL.
///
/// # Safety
///
/// This function assumes that `name` is NULL, or was returned by
/// `verneuil_manifest_name_for_hostname_path` and not destroyed
/// since.
#[no_mangle]
pub unsafe extern "C" fn verneuil_manifest_name_destroy(name: *mut c_char) {
    std::mem::drop(CString::from_raw(name));
}
