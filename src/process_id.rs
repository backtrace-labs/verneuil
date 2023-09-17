//! Describes the current process textually.  Operating systems can
//! reuse pids, so we couple that with the time at which the process
//! was spawned, when possible, and with a randomly generated integer
//! otherwise.
use std::fs::File;
use std::io::BufRead;

use regex::Regex;

use crate::atomic_kv32::AtomicKV32;

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
///
/// Returns `None` on any kind of failure: its caller has a graceful
/// fallback mechanism.
///
/// This is only expected to work on Linux, but a spurious success
/// seems unlikely; if an operating system has a linux-compatible
/// procfs, we might as well try to use it.
fn compute_birth(pid: u32) -> Option<u64> {
    let file = File::open(format!("/proc/{}/stat", pid)).ok()?;

    let mut btime: Option<u64> = None;
    // Use the last match: someone could in theory stash something
    // that looks like a valid stat line in the comm field.
    for line in std::io::BufReader::new(file).lines().flatten() {
        if let Some(tick) = find_btime_in_line(&line) {
            btime = tick.parse().ok();
        }
    }

    btime
}

/// Returns a string that (should) uniquely identify the current
/// process for this machine instance.
///
/// The string consists of `$pid.$birth_tick`, where `birth_tick`
/// represents the time at which the process was created.
///
/// When the birth time isn't available, we generate a random u32.
pub(crate) fn process_id() -> String {
    use rand::Rng;

    static PID_INFO: AtomicKV32 = AtomicKV32::new(0, 0);

    let (mut cached_pid, mut cached_birth) = PID_INFO.get();

    let current_pid = std::process::id();
    // Only execute the next block:
    //   - on the first call (cached_id is initially 0, reserved on all OSes).
    //   - after a fork (as the PID as changed)
    if current_pid != cached_pid {
        // Use the low 32 bits of the btime if we have one: that has
        // more entropy than the high bits.
        let btime = match compute_birth(current_pid) {
            Some(value) => value as u32,
            // Otherwise, generate a random value (it'll be cached for
            // the lifetime of the current process).
            None => rand::thread_rng().gen::<u32>(),
        };

        // We can't assume our write won.  Get the actual resulting
        // key-value pair from PID_INFO.
        (cached_pid, cached_birth) = PID_INFO.set(current_pid, btime);
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

#[cfg(target_os = "linux")]
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
