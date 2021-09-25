//! Describes the current incarnation of the running image textually.
//! The description should be constant until the next reboot, and
//! change after each reboot.
use std::boxed::Box;
use std::fs::File;
use std::io::BufRead;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

const DEFAULT_HOSTNAME: &str = "no.hostname.verneuil";

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

/// Returns the Unix timestamp at which the machine booted up.  This
/// value is only advisory, and useful to improve debuggability, but
/// not for correctness.
pub(crate) fn boot_timestamp() -> u64 {
    lazy_static::lazy_static! {
        static ref TIMESTAMP: u64 = compute_boot_time_slow().unwrap_or(0);
    }

    *TIMESTAMP
}

fn find_boot_id() -> Result<&'static str> {
    let file = File::open("/proc/sys/kernel/random/boot_id")?;

    match std::io::BufReader::new(file).lines().next() {
        None => Err(Error::new(ErrorKind::Other, "boot_id is empty")),
        Some(Err(e)) => Err(e),
        Some(Ok(line)) => Ok(Box::leak(line.into_boxed_str())),
    }
}

/// Returns the randomly generated UUID for this boot.
pub(crate) fn boot_id() -> &'static str {
    lazy_static::lazy_static! {
        static ref ID: &'static str = find_boot_id().expect("`/proc/sys/kernel/random/boot_id` should be set");
    }

    &*ID
}

fn find_hostname() -> Result<&'static str> {
    let file = File::open("/etc/hostname")?;

    match std::io::BufReader::new(file).lines().next() {
        None => Err(Error::new(ErrorKind::Other, "hostname is empty")),
        Some(Err(e)) => Err(e),
        Some(Ok(line)) => Ok(Box::leak(line.into_boxed_str())),
    }
}

/// Returns the machine's hostname, or a default placeholder if none.
pub fn hostname() -> &'static str {
    lazy_static::lazy_static! {
        static ref NAME: &'static str = find_hostname().unwrap_or(DEFAULT_HOSTNAME);
    }

    &*NAME
}

/// Returns a high-entropy short string hash of the hostname.
pub(crate) fn hostname_hash(hostname: &str) -> String {
    lazy_static::lazy_static! {
        static ref PARAMS: umash::Params = umash::Params::derive(0, "verneuil hostname params");
    }

    let hash = umash::full_str(&PARAMS, 0, 0, hostname);
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

    &*INSTANCE
}

#[test]
fn print_boot_time() {
    assert_ne!(boot_timestamp(), 0);
    println!("Boot time = {}", boot_timestamp());
}

#[test]
fn print_boot_id() {
    assert_ne!(boot_id(), "");
    println!("Boot id = {}", boot_id());
}

#[test]
fn print_instance_id() {
    println!("instance id = '{}'", instance_id());
}

#[test]
fn print_hostname() {
    assert_ne!(hostname(), DEFAULT_HOSTNAME);
    println!(
        "hostname = '{}', hash = '{}'",
        hostname(),
        hostname_hash(hostname())
    );
}
