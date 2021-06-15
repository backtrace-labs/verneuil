use std::ffi::c_void;
use std::os::raw::c_char;

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
    lock_timeout_ms: u32,
    dirsync_pending: bool,
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
extern "C" fn verneuil__file_close(file: &mut LinuxFile) -> i32 {
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
    unsafe { verneuil__file_lock_impl(file, level) }
}

#[no_mangle]
extern "C" fn verneuil__file_unlock(file: &mut LinuxFile, level: LockLevel) -> i32 {
    unsafe { verneuil__file_unlock_impl(file, level) }
}
