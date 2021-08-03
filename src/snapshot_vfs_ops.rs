use std::boxed::Box;
use std::ffi::c_void;
use std::os::raw::c_char;

use crate::chain_error;
use crate::fresh_error;
use crate::sqlite_code::SqliteCode;
use crate::sqlite_lock_level::LockLevel;
use crate::Result;
use crate::Snapshot;

// See vfs.c
#[derive(Debug)]
#[repr(C)]
struct SnapshotFile {
    methods: *const c_void,
    snapshot: *mut c_void, // Really a &mut crate::Snapshot.
}

impl SnapshotFile {
    /// Returns a reference to this `SnapshotFile`'s `Snapshot`, if
    /// it is populated.
    #[inline]
    fn snapshot(&self) -> Option<&mut Snapshot> {
        unsafe { (self.snapshot as *mut Snapshot).as_mut() }
    }

    /// Replaces the `snapshot` pointer in this `SnapshotFile` with a
    /// NULL pointer, and returns the old `Snapshot`, if it was
    /// populated.
    fn consume_snapshot(&mut self) -> Option<Box<Snapshot>> {
        let ptr = self.snapshot as *mut Snapshot;
        self.snapshot = std::ptr::null_mut();

        let snapshot = unsafe { ptr.as_mut() }?;
        Some(unsafe { Box::from_raw(snapshot) })
    }
}

#[no_mangle]
extern "C" fn verneuil__snapshot_open(file: &mut SnapshotFile, path: *const c_char) -> SqliteCode {
    #[tracing::instrument]
    fn open(file: &mut SnapshotFile, c_path: *const c_char) -> Result<()> {
        use prost::Message;

        let string = unsafe { std::ffi::CStr::from_ptr(c_path) }
            .to_str()
            .map_err(|e| chain_error!(e, "path is not valid utf-8"))?
            .to_owned();

        let bytes = std::fs::read(&string)
            .map_err(|e| chain_error!(e, "failed to read directory file", path=%string))?;

        let directory = crate::Directory::decode(&*bytes)
            .map_err(|e| chain_error!(e, "failed to parse directory file", path=%string))?;
        let snapshot = Snapshot::new_with_default_targets(&directory)?;

        file.snapshot = Box::leak(Box::new(snapshot)) as *mut Snapshot as *mut _;
        Ok(())
    }

    match open(file, path) {
        Ok(_) => SqliteCode::Ok,
        Err(e) => {
            let _ = chain_error!(e, "failed to open verneuil snapshot");
            SqliteCode::CantOpen
        }
    }
}

#[no_mangle]
extern "C" fn verneuil__snapshot_close(file: &mut SnapshotFile) -> SqliteCode {
    std::mem::drop(file.consume_snapshot());
    SqliteCode::Ok
}

#[no_mangle]
extern "C" fn verneuil__snapshot_read(
    file: &SnapshotFile,
    dst: *mut u8,
    n: i32,
    offset: i64,
) -> SqliteCode {
    let slice = unsafe { std::slice::from_raw_parts_mut(dst, n as usize) };
    match file.read(slice, offset as u64) {
        Ok(copied) => {
            if copied < (n as u64) {
                SqliteCode::IoErrShortRead
            } else {
                SqliteCode::Ok
            }
        }
        Err(e) => {
            let _ = chain_error!(e, "failed to read verneuil snapshot", n, offset);
            SqliteCode::IoErrRead
        }
    }
}

#[no_mangle]
extern "C" fn verneuil__snapshot_write(
    _file: &SnapshotFile,
    _src: *const u8,
    _n: i32,
    _offset: i64,
) -> SqliteCode {
    SqliteCode::ReadOnly
}

#[no_mangle]
extern "C" fn verneuil__snapshot_truncate(_file: &SnapshotFile, _size: i64) -> SqliteCode {
    SqliteCode::ReadOnly
}

#[no_mangle]
extern "C" fn verneuil__snapshot_sync(_file: &SnapshotFile, _flags: i32) -> SqliteCode {
    SqliteCode::ReadOnly
}

#[no_mangle]
extern "C" fn verneuil__snapshot_size(file: &SnapshotFile, out_size: &mut i64) -> SqliteCode {
    match file.size() {
        Ok(size) => {
            *out_size = size as i64;
            SqliteCode::Ok
        }
        Err(e) => {
            let _ = chain_error!(e, "failed to find verneuil snapshot size");
            SqliteCode::IoErrFstat
        }
    }
}

#[no_mangle]
extern "C" fn verneuil__snapshot_lock(_file: &SnapshotFile, level: LockLevel) -> SqliteCode {
    if level <= LockLevel::Shared {
        SqliteCode::Ok
    } else {
        SqliteCode::Perm
    }
}

#[no_mangle]
extern "C" fn verneuil__snapshot_unlock(_file: &SnapshotFile, _level: LockLevel) -> SqliteCode {
    SqliteCode::Ok
}

impl SnapshotFile {
    fn read(&self, mut dst: &mut [u8], offset: u64) -> Result<u64> {
        let snapshot = self
            .snapshot()
            .ok_or_else(|| fresh_error!("attempted to read uninitialised SnapshotFile"))?;

        let copied = std::io::copy(&mut snapshot.as_read(offset, dst.len() as u64), &mut dst)
            .map_err(|e| chain_error!(e, "failed to read from snapshot"))?;
        dst.fill(0);
        Ok(copied)
    }

    fn size(&self) -> Result<u64> {
        let snapshot = self
            .snapshot()
            .ok_or_else(|| fresh_error!("attempted to size uninitialised SnapshotFile"))?;

        Ok(snapshot.len())
    }
}
