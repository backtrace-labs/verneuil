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

fn find_btime_in_line(line: &str) -> Option<&str> {
    // We expect a comm field terminated by a closing parenthesis.  If
    // we have a parenthesis, parse everything after the last one: the
    // remaining fields in the stat line are all single letters or
    // integers.
    let haystack = match line.rsplit_once(')') {
        Some((_prefix, suffix)) => suffix,
        None => line,
    };
    let captures = STAT_RE.captures_iter(haystack).next()?;
    Some(captures.get(1)?.as_str())
}

/// Find the time at which `pid` was spawned, via `/proc/[pid]/stat`.
/// The return value should be treated like an opaque counter; in
/// practice, it counts clock ticks between boot and spawning.
fn compute_birth(pid: u32) -> Result<u64> {
    let file = File::open(format!("/proc/{}/stat", pid))?;

    let mut btime: Option<Result<u64>> = None;
    // Use the last match: someone could in theory stash something
    // that looks like a valid stat line in the comm field.
    for line in std::io::BufReader::new(file).lines().flatten() {
        if let Some(tick) = find_btime_in_line(&line) {
            let value = tick
                .parse()
                .map_err(|_| Error::new(ErrorKind::Other, "failed to parse birth tick"));
            btime = Some(value);
        }
    }

    btime.unwrap_or_else(|| {
        Err(Error::new(
            ErrorKind::Other,
            "failed to parse /proc/pid/stat",
        ))
    })
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

    assert_eq!(find_btime_in_line(SIMPLE), Some("96791036"));
}

#[test]
fn test_funny_comm_line() {
    // Assume a newline in comm, before the end.
    const BROKEN_LINE: &str = "asd S 0 1 1 0 -1 1077952768 57853 9890418 44 3013 399 704 48385 118027 20 0 1 0 1118 173748224 1855 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 1 0 0 290 0 0 0 0 0 0 0 0 0 0";

    assert_eq!(find_btime_in_line(BROKEN_LINE), Some("1118"));
}

#[test]
fn test_funny_comm_break_end() {
    // Assume a newline at the end of comm.
    const BROKEN_LINE: &str = " S 0 1 1 0 -1 1077952768 57853 9890418 44 3013 399 704 48385 118027 20 0 1 0 1118 173748224 1855 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 1 0 0 290 0 0 0 0 0 0 0 0 0 0";

    assert_eq!(find_btime_in_line(BROKEN_LINE), Some("1118"));
}

#[test]
fn test_funny_comm_break_end_with_paren() {
    // Assume a newline at the end of comm, just before the closing parenthesis
    const BROKEN_LINE: &str = ") S 0 1 1 0 -1 1077952768 57853 9890418 44 3013 399 704 48385 118027 20 0 1 0 1118 173748224 1855 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 1 0 0 290 0 0 0 0 0 0 0 0 0 0";

    assert_eq!(find_btime_in_line(BROKEN_LINE), Some("1118"));
}

#[test]
fn test_funny_comm_space() {
    // Assume a space in the middle of comm
    const BROKEN_LINE: &str = "asd sdf) S 0 1 1 0 -1 1077952768 57853 9890418 44 3013 399 704 48385 118027 20 0 1 0 1118 173748224 1855 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 1 0 0 290 0 0 0 0 0 0 0 0 0 0";

    assert_eq!(find_btime_in_line(BROKEN_LINE), Some("1118"));
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
