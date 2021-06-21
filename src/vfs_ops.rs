use std::boxed::Box;
use std::ffi::c_void;
use std::os::raw::c_char;

use crate::sqlite_code::SqliteCode;
use crate::tracker::Tracker;

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
enum LockLevel {
    None,
    Shared,
    Reserved,
    Pending,
    Exclusive,
}

// See vfs.c
#[derive(Debug)]
#[repr(C)]
struct LinuxFile {
    methods: *const c_void,
    fd: i32,
    lock_level: LockLevel,
    path: *const c_char,
    device: u64,
    inode: u64,
    tracker: *mut c_void, // Really a &mut crate::tracker::Tracker.
    lock_timeout_ms: u32,
    dirsync_pending: bool,
}

impl LinuxFile {
    /// Returns a reference to this `LinuxFile`'s `Tracker`, if
    /// it is populated.
    #[allow(dead_code)]
    #[inline]
    fn tracker(&self) -> Option<&mut Tracker> {
        unsafe { (self.tracker as *mut Tracker).as_mut() }
    }

    /// Replaces the `tracker` pointer in this `LinuxFile` with a
    /// NULL pointer, and returns the old `Tracker`, if it was
    /// populated.
    fn consume_tracker(&mut self) -> Option<Box<Tracker>> {
        let ptr = self.tracker as *mut Tracker;
        self.tracker = std::ptr::null_mut();

        let tracker = unsafe { ptr.as_mut() }?;
        Some(unsafe { Box::from_raw(tracker) })
    }
}

// See vfs.h
extern "C" {
    fn verneuil__file_close_impl(file: &mut LinuxFile) -> i32;
    fn verneuil__file_read_impl(file: &LinuxFile, buf: *mut c_void, n: i32, off: i64) -> i32;
    fn verneuil__file_write_impl(file: &LinuxFile, buf: *const c_void, n: i32, off: i64) -> i32;
    fn verneuil__file_truncate_impl(file: &LinuxFile, size: i64) -> i32;
    fn verneuil__file_sync_impl(file: &mut LinuxFile, flags: i32) -> i32;
    fn verneuil__file_size_impl(file: &LinuxFile, OUT_size: &mut i64) -> i32;
    fn verneuil__file_lock_impl(file: &mut LinuxFile, level: LockLevel) -> i32;
    fn verneuil__file_unlock_impl(file: &mut LinuxFile, level: LockLevel) -> i32;
}

#[no_mangle]
extern "C" fn verneuil__file_post_open(file: &mut LinuxFile) -> SqliteCode {
    // If the file doesn't have a name, or the fd is invalid, we can't
    // track it.  Assume that's by design, and let the caller handle
    // that state itself.
    if file.path == std::ptr::null() || file.fd < 0 {
        return SqliteCode::Ok;
    }

    match Tracker::new(file.path, file.fd) {
        Err(_) => SqliteCode::CantOpen,
        Ok(tracker) => {
            file.tracker = Box::leak(Box::new(tracker)) as *mut Tracker as *mut _;
            SqliteCode::Ok
        }
    }
}

#[no_mangle]
extern "C" fn verneuil__file_close(file: &mut LinuxFile) -> i32 {
    std::mem::drop(file.consume_tracker());
    unsafe { verneuil__file_close_impl(file) }
}

#[no_mangle]
extern "C" fn verneuil__file_read(
    file: &mut LinuxFile,
    dst: *mut c_void,
    n: i32,
    offset: i64,
) -> i32 {
    assert!(n >= 0, "n={}", n);
    assert!(offset >= 0, "offset={}", offset);
    // We can *mostly* assume that either the read is in the first
    // page, or it's page-aligned, but some of the shim VFSes that
    // sqlite uses in its tests violate that assumption.
    //
    // Similarly, some test VFSes read without holding a shared lock
    // on the file.

    unsafe { verneuil__file_read_impl(file, dst, n, offset) }
}

#[no_mangle]
extern "C" fn verneuil__file_write(
    file: &LinuxFile,
    src: *const c_void,
    n: i32,
    offset: i64,
) -> i32 {
    assert!(n >= 0, "n={}", n);
    assert!(offset >= 0, "offset={}", offset);
    // We can *mostly* assume that writes are page-aligned, but some
    // of the shim VFSes that sqlite uses in its tests violate that
    // assumption.
    //
    // Similarly, some test VFSes write without holding an exclusive
    // lock on the file.

    unsafe { verneuil__file_write_impl(file, src, n, offset) }
}

#[no_mangle]
extern "C" fn verneuil__file_truncate(file: &LinuxFile, size: i64) -> i32 {
    // We can mostly assume truncations are page-aligned, except some
    // sqlite tests likes to do fun stuff.

    unsafe { verneuil__file_truncate_impl(file, size) }
}

#[no_mangle]
extern "C" fn verneuil__file_sync(file: &mut LinuxFile, flags: i32) -> i32 {
    unsafe { verneuil__file_sync_impl(file, flags) }
}

#[no_mangle]
extern "C" fn verneuil__file_size(file: &LinuxFile, size: &mut i64) -> i32 {
    unsafe { verneuil__file_size_impl(file, size) }
}

#[no_mangle]
extern "C" fn verneuil__file_lock(file: &mut LinuxFile, level: LockLevel) -> i32 {
    if level <= file.lock_level {
        return 0;
    }

    unsafe { verneuil__file_lock_impl(file, level) }
}

#[no_mangle]
extern "C" fn verneuil__file_unlock(file: &mut LinuxFile, level: LockLevel) -> i32 {
    if level >= file.lock_level {
        return 0;
    }

    // We want to trigger a snapshot whenever we downgrade a lock:
    // sqlite only downgrades when a db file is in a valid state.
    // However, we don't want to run the snapshotting logic with an
    // exclusive lock held, so always downgrade to a shared lock
    // before snapshotting.
    //
    // If the file is already LockLevel::Shared or less, the call
    // will no-op.
    if level <= LockLevel::Shared {
        let ret = unsafe { verneuil__file_unlock_impl(file, LockLevel::Shared) };

        if ret != 0 {
            return ret;
        }
    }

    if file.lock_level == LockLevel::Shared {
        if let Some(tracker) = file.tracker() {
            // TODO: consider logging snapshot failures here.
            let _ = tracker.snapshot();
        }
    }

    unsafe { verneuil__file_unlock_impl(file, level) }
}
