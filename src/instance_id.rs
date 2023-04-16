//! Describes the current incarnation of the running image textually.
//! The description should be constant until the next reboot, and
//! change after each reboot.
use std::boxed::Box;
#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::BufRead;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

const DEFAULT_HOSTNAME: &str = "no.hostname.verneuil";

/// On Linux, we use `/proc` (sysctl has been deprecated since 2.6,
/// and was *removed* in 5.5).  BSDs probably expect to use sysctl;
/// Darwin certainly does.

/// Finds the mib for sysctl `name`.
#[cfg(not(target_os = "linux"))]
fn resolve_sysctl(name: &str) -> Result<Vec<libc::c_int>> {
    let c_name = std::ffi::CString::new(name)?;
    let mut name = Vec::<libc::c_int>::new();

    // All the names I've seen top out at a depth of 4.
    name.resize(16, 0);
    loop {
        let mut len = name.len();
        let rc = unsafe { libc::sysctlnametomib(c_name.as_ptr(), name.as_mut_ptr(), &mut len) };

        if rc == 0 {
            name.resize(len, 0);
            return Ok(name);
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// Calls `sysctl(3)` to store the value of `mib` in `value` / `len`.
///
/// In practice, `mib` is immutable, but the C prototype says it's
/// mutable.
#[cfg(not(target_os = "linux"))]
fn robust_sysctl<T>(mib: &mut [libc::c_int], value: *mut T, len: &mut usize) -> Result<()> {
    loop {
        let rc = unsafe {
            libc::sysctl(
                mib.as_mut_ptr() as *mut _,
                mib.len() as _, // this is c_int or uint on different OSes
                value as *mut _,
                len as *mut usize,
                std::ptr::null_mut(),
                0,
            )
        };

        if rc == 0 {
            return Ok(());
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// Gets the string-valued sysctl at `mib`.
#[cfg(not(target_os = "linux"))]
fn get_sysctl_string(mib: &mut [libc::c_int]) -> Result<String> {
    // Repeat a bounded number of times.
    for _ in 0..4 {
        let mut len = 0usize;

        // Get the value's size.
        robust_sysctl(mib, std::ptr::null_mut() as *mut u8, &mut len)?;

        let mut val = Vec::<u8>::new();
        val.resize(len, 0u8);

        // Grab the value.
        match robust_sysctl(mib, val.as_mut_ptr(), &mut len) {
            Ok(()) => {
                // Drop NUL terminators.
                while val.last() == Some(&0) {
                    val.pop();
                }
                return Ok(String::from_utf8_lossy(&val).into_owned());
            }
            Err(e) if e.kind() == ErrorKind::OutOfMemory => {
                // Try again, looks like the size changed?!
                continue;
            }
            e => e?,
        }
    }

    Err(Error::last_os_error())
}

#[cfg(target_os = "linux")]
fn compute_boot_time_slow() -> Result<u64> {
    let file = File::open("/proc/stat")?;

    // Look for a line that looks like
    // `btime 1623415789` in `/proc/stat`.
    for line_or in std::io::BufReader::new(file).lines() {
        let line = line_or?;
        if let Some(suffix) = line.strip_prefix("btime ") {
            return suffix
                .parse::<u64>()
                .map_err(|_| Error::new(ErrorKind::Other, "failed to parse btime"));
        }
    }

    Err(Error::new(ErrorKind::Other, "btime not in `/proc/stat`"))
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn compute_boot_time_slow() -> Result<u64> {
    #[repr(C)]
    struct Timeval {
        tv_sec: i64,
        tv_usec: i32,
    }

    let tv_len = std::mem::size_of::<Timeval>();
    let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
    let mut tv = Timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut len = tv_len;
    robust_sysctl(&mut mib, &mut tv as *mut _, &mut len)?;

    if len != tv_len {
        return Err(Error::new(
            ErrorKind::Other,
            format!("sysctl(boottime) returned an invalid size: {}", len),
        ));
    }

    Ok(tv.tv_sec.max(0) as u64)
}

/// Returns the Unix timestamp at which the machine booted up.  This
/// value is only advisory, and useful to improve debuggability, but
/// not for correctness.
pub(crate) fn boot_timestamp() -> u64 {
    lazy_static::lazy_static! {
        static ref TIMESTAMP: u64 = compute_boot_time_slow().unwrap_or(0);
    }

    *TIMESTAMP
}

#[cfg(target_os = "linux")]
fn find_boot_id() -> Result<&'static str> {
    let file = File::open("/proc/sys/kernel/random/boot_id")?;

    match std::io::BufReader::new(file).lines().next() {
        None => Err(Error::new(ErrorKind::Other, "boot_id is empty")),
        Some(Err(e)) => Err(e),
        Some(Ok(line)) => Ok(Box::leak(line.into_boxed_str())),
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn find_boot_id() -> Result<&'static str> {
    // Or should we use kern.bootuuid? bootsessionuuid seems more
    // conservative (more likely to change).
    let mut mib = resolve_sysctl("kern.bootsessionuuid")?;
    Ok(Box::leak(get_sysctl_string(&mut mib)?.into_boxed_str()))
}

/// Returns the randomly generated UUID for this boot.
pub(crate) fn boot_id() -> &'static str {
    lazy_static::lazy_static! {
        static ref ID: &'static str = find_boot_id().expect("boot id should be set");
    }

    &ID
}

#[cfg(target_os = "linux")]
fn find_hostname() -> Result<&'static str> {
    let file = File::open("/etc/hostname")?;

    match std::io::BufReader::new(file).lines().next() {
        None => Err(Error::new(ErrorKind::Other, "hostname is empty")),
        Some(Err(e)) => Err(e),
        Some(Ok(line)) => Ok(Box::leak(line.into_boxed_str())),
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn find_hostname() -> Result<&'static str> {
    let mut mib = resolve_sysctl("kern.hostname")?;
    Ok(Box::leak(get_sysctl_string(&mut mib)?.into_boxed_str()))
}

/// Returns the machine's hostname, or a default placeholder if none.
pub fn hostname() -> &'static str {
    lazy_static::lazy_static! {
        static ref NAME: &'static str = find_hostname().unwrap_or(DEFAULT_HOSTNAME);
    }

    &NAME
}

/// Returns a high-entropy short string hash of the hostname.
pub(crate) fn hostname_hash(hostname: &str) -> String {
    lazy_static::lazy_static! {
        static ref PARAMS: umash::Params = umash::Params::derive(0, b"verneuil hostname params");
    }

    let hash = PARAMS.hasher(0).write(hostname.as_bytes()).digest();
    format!("{:04x}", hash % (1 << (4 * 4)))
}

/// Returns the verneuil instance id for this incarnation of the
/// current machine: the first component is the boot timestamp, to
/// help operations, and the second is the boot UUID, which is
/// expected to always change between reboots.
pub(crate) fn instance_id() -> &'static str {
    lazy_static::lazy_static! {
        static ref INSTANCE: &'static str = Box::leak(format!("{}.{}", boot_timestamp(), boot_id()).into_boxed_str());
    }

    &INSTANCE
}

/// Returns a list of instance ids within `range` seconds of our
/// `boot_timestamp()`, from most to least similar to `instance_id()`.
/// The `boot_id()` suffices for correctness, while the
/// `boot_timestamp()` mostly exists to help operators, so it can make
/// sense to probe for `boot_timestamp()`.
///
/// The `boot_timestamp()` is subject to time adjustments, so we may
/// want to probe for instance ids similar to the one we think we
/// have.  If we ever allow configuration without `boot_id()`, this
/// function should probably detect that situation and inconditionally
/// return an empty list.
pub(crate) fn likely_instance_ids(range: u64) -> Vec<String> {
    let base_ts = boot_timestamp();
    let boot_id = boot_id();

    let mut ret = vec![instance_id().to_string()];

    for delta in 1..=range {
        if let Some(ts) = base_ts.checked_sub(delta) {
            ret.push(format!("{}.{}", ts, boot_id));
        }

        if let Some(ts) = base_ts.checked_add(delta) {
            ret.push(format!("{}.{}", ts, boot_id));
        }
    }

    ret
}

#[test]
fn print_boot_time() {
    assert_ne!(compute_boot_time_slow().expect("should have boot time"), 0);
    assert_ne!(boot_timestamp(), 0);
    println!("Boot time = {}", boot_timestamp());
}

#[test]
fn print_boot_id() {
    assert_ne!(find_boot_id().expect("should have boot id"), "");
    println!("Boot id = {}", boot_id());
}

#[test]
fn print_instance_id() {
    println!("instance id = '{}'", instance_id());
}

#[test]
fn print_hostname() {
    assert_ne!(find_hostname().expect("should have hostname"), "");
    assert_ne!(hostname(), DEFAULT_HOSTNAME);
    println!(
        "hostname = '{}', hash = '{}'",
        hostname(),
        hostname_hash(hostname())
    );
}

/// Changing the hostname hash function is a backward incompatible
/// change in storage format.  Test against that.
#[test]
fn test_hostname_hash() {
    assert_eq!(hostname_hash("example.com"), "7010");
}
