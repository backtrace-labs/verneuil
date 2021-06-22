//! Descrives the current process textually.  Operating systems
//! can reuse pids, so we couple that with the time at which
//! the process was spawned.
use regex::Regex;
use std::fs::File;
use std::io::BufRead;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

lazy_static::lazy_static! {
    // A stat line is "pid comm state ...", where `pid` is an
    // integer, `comm` an arbitrary string (which could include
    // newlines), and `state` a single letter.  Finally, the
    // remainder is all integers.  We want to parse even when the
    // `comm` string is weird, so we assume the previous line
    // could have been broken anywhere in `comm`.
    static ref STAT_RE: Regex = Regex::new(r"^(?:(?:\d+ )?.*?)? [A-Za-z](?: -?\d+){18} (\d+)(?: -?\d+)+ .*$").expect("/proc/pid/stat regex should compile");
}

/// Find the time at which `pid` was spawned, via `/proc/[pid]/stat`.
/// The return value should be treated like an opaque counter; in
/// practice, it counts clock ticks between boot and spawning.
fn compute_birth(pid: u32) -> Result<u64> {
    let file = File::open(format!("/proc/{}/stat", pid))?;

    for line in std::io::BufReader::new(file).lines().flatten() {
        if let Some(captures) = STAT_RE.captures_iter(&line).next() {
            return captures[1]
                .parse()
                .map_err(|_| Error::new(ErrorKind::Other, "failed to parse birth tick"));
        }
    }

    Err(Error::new(
        ErrorKind::Other,
        "failed to parse /proc/pid/stat",
    ))
}

/// Returns a string that (should) uniquely identify the current
/// process for this machine instance.
///
/// The string consists of `$pid.$birth_tick`, where `birth_tick`
/// represents the time at which the process was created.
pub(crate) fn process_id() -> String {
    static PID: AtomicU32 = AtomicU32::new(0);
    static BIRTH: AtomicU64 = AtomicU64::new(0);

    let current_pid = std::process::id();
    let mut cached_pid = PID.load(Ordering::Acquire);
    let mut cached_birth = BIRTH.load(Ordering::Acquire);

    if cached_pid != current_pid {
        cached_birth = compute_birth(current_pid).unwrap_or(u64::MAX);
        cached_pid = current_pid;

        BIRTH.store(cached_birth, Ordering::Release);
        PID.store(cached_pid, Ordering::Release);
    }

    format!("{}.{}", cached_pid, cached_birth)
}

#[test]
fn test_simple_line() {
    const SIMPLE: &str = "12189 (cat) R 11919 12189 11919 34817 12189 4210688 99 0 0 0 0 0 0 0 20 0 1 0 96791036 5525504 188 18446744073709551615 96766470836224 96766470861193 140732840343824 0 0 0 0 0 0 0 0 0 17 3 0 0 0 0 0 96766470880336 96766470881888 96766488211456 140732840350995 140732840351015 140732840351015 140732840353775 0";

    let values = STAT_RE
        .captures_iter(SIMPLE)
        .map(|cap| cap[1].to_owned())
        .collect::<Vec<_>>();
    assert_eq!(values, ["96791036"]);
}

#[test]
fn test_funny_comm_line() {
    // Assume a newline in comm, before the end.
    const BROKEN_LINE: &str = "asd S 0 1 1 0 -1 1077952768 57853 9890418 44 3013 399 704 48385 118027 20 0 1 0 1118 173748224 1855 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 1 0 0 290 0 0 0 0 0 0 0 0 0 0";

    let values = STAT_RE
        .captures_iter(BROKEN_LINE)
        .map(|cap| cap[1].to_owned())
        .collect::<Vec<_>>();
    assert_eq!(values, ["1118"]);
}

#[test]
fn test_funny_comm_break_end() {
    // Assume a newline at the end of comm.
    const BROKEN_LINE: &str = " S 0 1 1 0 -1 1077952768 57853 9890418 44 3013 399 704 48385 118027 20 0 1 0 1118 173748224 1855 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 1 0 0 290 0 0 0 0 0 0 0 0 0 0";

    let values = STAT_RE
        .captures_iter(BROKEN_LINE)
        .map(|cap| cap[1].to_owned())
        .collect::<Vec<_>>();
    assert_eq!(values, ["1118"]);
}

#[test]
fn print_pid1_birth() {
    println!(
        "pid1 birth tick = {}",
        compute_birth(1).expect("pid1 should parse")
    );
}

#[test]
fn print_self_id() {
    println!("self_id = {}", process_id());
}
