mod copier;
mod directory_schema;
mod instance_id;
mod ofd_lock;
mod process_id;
mod racy_time;
mod replication_buffer;
mod replication_target;
mod result;
mod sqlite_code;
mod tracker;
mod vfs_ops;

use std::ffi::c_void;
use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;
use std::path::Path;

/// Initialization options for the Verneuil VFS.
#[derive(Default, serde::Deserialize)]
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
    #[serde(default)]
    pub replication_targets: Vec<replication_target::ReplicationTarget>,
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
    fn verneuil_init_impl(db: *mut c_void, errmsg: *mut *mut c_char, api: *const c_void) -> i32;
    #[cfg(feature = "verneuil_test_vfs")]
    fn verneuil_test_only_register() -> i32;
}

/// Configures the Verneuil VFS
pub fn configure(options: Options) -> Result<(), i32> {
    let c_path;
    let mut foreign_options = ForeignOptions {
        make_default: options.make_default,
        tempdir: std::ptr::null(),
        replication_spooling_dir: std::ptr::null(),
        replication_spooling_dir_permissions: 0,
        json_options: std::ptr::null(),
    };

    if let Some(path) = options.tempdir {
        c_path = CString::new(path).map_err(|_| -1)?;
        foreign_options.tempdir = c_path.as_ptr();
    }

    // The C VFS only cares about `make_default` and `tempdir`.
    let ret = unsafe { verneuil_configure_impl(&foreign_options) };
    if ret != 0 {
        return Err(ret);
    }

    if let Some(spooling_dir) = options.replication_spooling_dir {
        let mode = options.replication_spooling_dir_permissions.unwrap_or(0) | 0o700;

        replication_buffer::set_default_spooling_directory(
            Path::new(&spooling_dir),
            std::os::unix::fs::PermissionsExt::from_mode(mode),
        )
        .map_err(|_| -1)?;
    }

    replication_target::set_default_replication_targets(options.replication_targets);
    Ok(())
}

/// This is the C-visible configuration function.
///
/// # Safety
///
/// Assumes the `options_ptr` is NULL or valid.
#[no_mangle]
pub unsafe extern "C" fn verneuil_configure(options_ptr: *const ForeignOptions) -> i32 {
    let mut options: Options = Default::default();

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

    if !options_ptr.is_null() {
        let foreign_options = &*options_ptr;

        if let Some(json) = cstr_to_string(foreign_options.json_options) {
            match serde_json::from_str(&json) {
                Ok(parsed) => options = parsed,
                Err(_) => return -1,
            }
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

/// Sqlite3 will invoke this function if Verneuil is loaded as a
/// dynamic extension.  We define this wrapper in Rust because cargo
/// hides C definitions in cdylib builds.
///
/// # Safety
///
/// The arguments must be valid, as defined by sqlite.  This function
/// should only be called by sqlite, which is aware of its own
/// preconditions.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_verneuil_init(
    db: *mut c_void,
    err_msg: *mut *mut c_char,
    api: *const c_void,
) -> i32 {
    verneuil_init_impl(db, err_msg, api)
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
#[cfg(feature = "verneuil_test_vfs")]
pub unsafe extern "C" fn sqlite3_verneuil_test_only_register(_: *const c_char) -> i32 {
    use replication_target::*;

    // Send tracing calls to stdout, and converts any log! call to
    // traces.
    let _ = tracing_subscriber::fmt::try_init();
    tracing::info!("tracing initialized");

    crate::replication_buffer::ENABLE_AUTO_CLEANUP
        .store(true, std::sync::atomic::Ordering::Relaxed);

    if let Err(code) = configure(Options {
        make_default: true,
        tempdir: None,
        replication_spooling_dir: Some("/tmp".into()),
        replication_spooling_dir_permissions: None,
        replication_targets: vec![
            #[cfg(feature = "verneuil_test_minio")]
            ReplicationTarget::S3(S3ReplicationTarget {
                region: "minio".into(),
                endpoint: Some("http://127.0.0.1:7777".into()),
                chunk_bucket: "chunks".into(),
                directory_bucket: "directories".into(),
                domain_addressing: false,
                create_buckets_on_demand: true,
            }),
        ],
    }) {
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

    // Directory proto bytes.
    pub bytes: Vec<u8>,
}

/// Attempts to return the name for the directory proto blob associated
/// with `source_db`, and our local snapshot of that blob if available.
///
/// The contents of the file are looked up in a subdirectory of
/// `spool_prefix`, or in the default prefix if None.
pub fn current_replication_proto_for_db(
    source_db: &std::path::Path,
    spool_prefix: Option<std::path::PathBuf>,
) -> result::Result<(String, Option<ReplicationProtoData>)> {
    let meta_path = replication_buffer::tapped_meta_path_in_spool_prefix(spool_prefix, source_db)?;

    let blob_name = meta_path
        .file_name()
        .expect("meta_path must have file name")
        .to_str()
        .expect("url-encoded blob name must be valid utf-8")
        .to_string();

    let proto_data = (|| {
        use prost::Message;

        let bytes = match std::fs::read(&meta_path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(chain_error!(
                    e,
                    "failed to read tapped meta file",
                    ?meta_path
                ))
            }
            Ok(bytes) => bytes,
        };

        let v1 = match directory_schema::Directory::decode(&*bytes) {
            Err(e) => {
                let _ = chain_error!(e, "failed to decode proto bytes", ?meta_path);
                return Ok(None);
            }
            Ok(directory) => {
                if let Some(v1) = directory.v1 {
                    v1
                } else {
                    return Ok(None);
                }
            }
        };

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
    })()?;

    Ok((blob_name, proto_data))
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

    dst.blob_name = CString::from_vec_unchecked(blob_name.into_bytes()).into_raw();
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
