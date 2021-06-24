mod copier;
mod directory_schema;
mod instance_id;
mod process_id;
mod replication_buffer;
mod replication_target;
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
    /// subdirectories of that staging directory.
    pub replication_staging_dir: Option<String>,

    /// List of default replication targets.
    #[serde(default)]
    pub replication_targets: Vec<replication_target::ReplicationTarget>,
}

#[repr(C)]
pub struct ForeignOptions {
    pub make_default: bool,
    pub tempdir: *const c_char,
    pub replication_staging_dir: *const c_char,
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
        replication_staging_dir: std::ptr::null(),
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

    if let Some(staging_dir) = options.replication_staging_dir {
        replication_buffer::set_default_staging_directory(Path::new(&staging_dir))
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

        if let Some(dir) = cstr_to_string(foreign_options.replication_staging_dir) {
            options.replication_staging_dir = Some(dir);
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

    crate::replication_buffer::ENABLE_AUTO_CLEANUP
        .store(true, std::sync::atomic::Ordering::Relaxed);

    if let Err(code) = configure(Options {
        make_default: true,
        tempdir: None,
        replication_staging_dir: Some("/tmp".into()),
        replication_targets: vec![ReplicationTarget::S3(S3ReplicationTarget {
            region: "minio".into(),
            endpoint: Some("http://127.0.0.1:7777".into()),
            chunk_bucket: "chunks".into(),
            directory_bucket: "directories".into(),
            domain_addressing: false,
            create_buckets_on_demand: true,
        })],
    }) {
        return code;
    }

    verneuil_test_only_register()
}
