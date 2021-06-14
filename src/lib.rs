use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;
use std::path::Path;

/// Initialization options for the Verneuil VFS.
#[derive(Default)]
pub struct Options<'a> {
    /// If true, the Verneuil VFS overrides the default sqlite VFS.
    pub make_default: bool,
    /// All temporary file will live in this directory, or a default
    /// value if `None`.
    pub tempdir: Option<&'a Path>,
}

#[repr(C)]
pub struct ForeignOptions {
    pub make_default: bool,
    pub tempdir: *const c_char,
}

/// Configures the Verneuil VFS
pub fn configure(options: Options) -> Result<(), i32> {
    use std::os::unix::ffi::OsStrExt;

    extern "C" {
        fn verneuil_configure_impl(options: *const ForeignOptions) -> i32;
    }

    let cstr;
    let mut foreign_options = ForeignOptions {
        make_default: options.make_default,
        tempdir: std::ptr::null(),
    };

    if let Some(path) = options.tempdir {
        cstr = CString::new(path.as_os_str().as_bytes()).map_err(|_| -1)?;
        foreign_options.tempdir = cstr.as_ptr();
    }

    let ret = unsafe { verneuil_configure_impl(&foreign_options) };

    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}

/// This is the C-visible configuration function.
///
/// # Safety
///
/// Assumes the `options_ptr` is NULL or valid.
#[no_mangle]
pub unsafe extern "C" fn verneuil_configure(options_ptr: *const ForeignOptions) -> i32 {
    let path_str;
    let mut options: Options = Default::default();

    if !options_ptr.is_null() {
        let foreign_options = &*options_ptr;

        options.make_default = foreign_options.make_default;
        if !foreign_options.tempdir.is_null() {
            path_str = CStr::from_ptr(foreign_options.tempdir)
                .to_str()
                .expect("path must be valid")
                .to_owned();

            options.tempdir = Some(Path::new(&path_str));
        }
    }

    match configure(options) {
        Ok(()) => 0,
        Err(code) => code,
    }
}
