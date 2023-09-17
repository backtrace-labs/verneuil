use std::boxed::Box;
use std::ffi::c_void;
use std::os::raw::c_char;

use crate::chain_error;
use crate::chain_warn;
use crate::drop_result;
use crate::sqlite_code::SqliteCode;
use crate::sqlite_lock_level::LockLevel;
use crate::tracker::Tracker;

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
    first_write_in_transaction: bool,
    flush_on_close: bool,
}

impl LinuxFile {
    /// Returns a reference to this `LinuxFile`'s `Tracker`, if
    /// it is populated.
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
    if file.path.is_null() || file.fd < 0 {
        return SqliteCode::Ok;
    }

    match Tracker::new(file.path, file.fd) {
        Err(_) => SqliteCode::CantOpen,
        Ok(None) => SqliteCode::Ok,
        Ok(Some(tracker)) => {
            file.tracker = Box::leak(Box::new(tracker)) as *mut Tracker as *mut _;
            SqliteCode::Ok
        }
    }
}

#[no_mangle]
extern "C" fn verneuil__file_flush_replication_data(file: &mut LinuxFile) -> i32 {
    let tracker = if let Some(tracker) = file.tracker() {
        tracker
    } else {
        return 0;
    };

    match tracker
        .flush_spooled_data()
        .map_err(|e| chain_warn!(e, "failed to force flush replication data"))
    {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
extern "C" fn verneuil__file_close(file: &mut LinuxFile) -> i32 {
    let tracker = file.consume_tracker();

    if file.flush_on_close {
        if let Some(tracker) = tracker.as_ref() {
            drop_result!(tracker.flush_spooled_data(),
                         e => chain_warn!(e, "failed to flush replication data on close"));
        }
    }

    std::mem::drop(tracker);
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
    file: &mut LinuxFile,
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

    if let Some(tracker) = file.tracker() {
        tracker.flag_write(src as *const u8, offset as u64, n as u64);
    } else if file.first_write_in_transaction && file.fd >= 0 {
        use std::fs::File;
        use std::mem::ManuallyDrop;
        use std::os::unix::io::FromRawFd;

        let db = ManuallyDrop::new(unsafe { File::from_raw_fd(file.fd) });

        // A tracker has state to invoke this function more
        // efficiently.  If we don't have one, let's still update the
        // db file's version id, the slow way.
        drop_result!(crate::manifest_schema::update_version_id(&db, None),
                     e => chain_error!(e, "failed to mark db file as updated",
                                       %file.device, %file.inode));
    }

    file.first_write_in_transaction = false;
    unsafe { verneuil__file_write_impl(file, src, n, offset) }
}

#[no_mangle]
extern "C" fn verneuil__file_truncate(file: &LinuxFile, size: i64) -> i32 {
    // We can mostly assume truncations are page-aligned, except some
    // sqlite tests likes to do fun stuff.

    if let Some(tracker) = file.tracker() {
        let mut current_size = 0i64;
        let ret = verneuil__file_size(file, &mut current_size);

        if ret != 0 {
            return ret;
        }

        // Mark the range between the old and new size as dirty.
        let min = size.min(current_size);
        let max = size.max(current_size);
        if min >= 0 && max >= 0 {
            tracker.flag_write(std::ptr::null(), min as u64, (max - min) as u64);
        }
    }

    unsafe { verneuil__file_truncate_impl(file, size) }
}

#[no_mangle]
extern "C" fn verneuil__file_sync(file: &mut LinuxFile, flags: i32) -> i32 {
    // If there's something to sync, there was a write.
    if let Some(tracker) = file.tracker() {
        tracker.flag_write(std::ptr::null(), 0, 0);
    }

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

    // We're upgrading from no lock to some lock.  Kick off the
    // pre-lock checks.
    if file.lock_level == LockLevel::None {
        if let Some(tracker) = file.tracker() {
            tracker.pre_lock_checks();
        }
    }

    let ret = unsafe { verneuil__file_lock_impl(file, level) };
    if file.lock_level == LockLevel::Exclusive {
        if let Some(tracker) = file.tracker() {
            tracker.note_exclusive_lock();
        }

        file.first_write_in_transaction = true;
    }

    ret
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
            drop_result!(tracker.snapshot(),
                         e => chain_warn!(e, "failed to snapshot db file", ?tracker));
        }
    }

    unsafe { verneuil__file_unlock_impl(file, level) }
}
