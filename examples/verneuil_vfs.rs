//! The Verneuil extension can be loaded by sqlite at runtime to
//! enable the Verneuil replicating VFS.  The VFS looks for its
//! configuration JSON in the `VERNEUIL_CONFIG` environment variable.
use std::ffi::c_void;
use std::os::raw::c_char;

use verneuil::chain_error;
use verneuil::drop_result;
use verneuil::fresh_warn;
use verneuil::VERNEUIL_CONFIG_ENV_VAR;

// See `c/vfs.h`.
extern "C" {
    fn verneuil_init_impl(db: *mut c_void, errmsg: *mut *mut c_char, api: *const c_void) -> i32;
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
pub unsafe extern "C" fn sqlite3_verneuilvfs_init(
    db: *mut c_void,
    err_msg: *mut *mut c_char,
    api: *const c_void,
) -> i32 {
    // Send tracing calls to stderr, and convert any log! call to
    // traces.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .compact()
        .with_writer(std::io::stderr)
        .try_init()
        .err()
        .map(|e| format!("{:?}", e));

    tracing::info!("tracing initialized");
    match verneuil::load_configuration_from_env(None) {
        Some(options) => drop_result!(verneuil::configure_replication(options),
                                      e => chain_error!(e, "failed to configure verneuil")),
        None => {
            let _ = fresh_warn!("no verneuil configuration found", %VERNEUIL_CONFIG_ENV_VAR);
        }
    }

    verneuil_init_impl(db, err_msg, api)
}
