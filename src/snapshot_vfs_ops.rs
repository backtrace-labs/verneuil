use std::boxed::Box;
use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use crate::chain_error;
use crate::fresh_error;
use crate::sqlite_code::SqliteCode;
use crate::sqlite_lock_level::LockLevel;
use crate::Result;
use crate::Snapshot;

struct Data {
    path: String,
    data: Snapshot,
}

// See vfs.c
#[derive(Debug)]
#[repr(C)]
struct SnapshotFile {
    methods: *const c_void,
    snapshot: *mut c_void, // Really a &mut Arc<Data>.
}

impl SnapshotFile {
    /// Returns a reference to this `SnapshotFile`'s `Data`, if
    /// it is populated.
    #[inline]
    fn snapshot(&self) -> Option<&mut Arc<Data>> {
        unsafe { (self.snapshot as *mut Arc<Data>).as_mut() }
    }

    /// Replaces the `snapshot` pointer in this `SnapshotFile` with a
    /// NULL pointer, and returns the old `Data`, if it was
    /// populated.
    fn consume_snapshot(&mut self) -> Option<Box<Arc<Data>>> {
        let ptr = self.snapshot as *mut Arc<Data>;
        self.snapshot = std::ptr::null_mut();

        let snapshot = unsafe { ptr.as_mut() }?;
        Some(unsafe { Box::from_raw(snapshot) })
    }
}

// This internal cache maps from path to latest live snapshot data.
lazy_static::lazy_static! {
    static ref LIVE_DATA: Mutex<HashMap<String, Weak<Data>>> = Default::default();
}

impl Drop for Data {
    fn drop(&mut self) {
        // Remove this `Data` from the global cache.
        let mut map = LIVE_DATA.lock().expect("mutex should be valid");

        // By the time this `Data` is dropped, the Arc's strong count
        // has already been decremented to 0.  If we can upgrade the
        // value, it must have been inserted for a different copy of
        // this `Data`.
        if let Some(weak) = map.remove(&self.path) {
            if let Some(_strong) = weak.upgrade() {
                let mut path = String::new();
                std::mem::swap(&mut path, &mut self.path);
                map.insert(path, weak);
            }
        }
    }
}

fn fetch_new_data(path: String) -> Result<Arc<Data>> {
    use prost::Message;

    let bytes = match crate::manifest_bytes_for_path(None, &path)? {
        Some(bytes) => bytes,
        None => return Err(fresh_error!("manifest not found", %path)),
    };

    let manifest = crate::Manifest::decode(&*bytes)
        .map_err(|e| chain_error!(e, "failed to parse manifest file", %path))?;

    let data = Arc::new(Data {
        path: path.clone(),
        data: Snapshot::new_with_default_targets(&manifest)?,
    });

    LIVE_DATA
        .lock()
        .expect("mutex should be valid")
        .insert(path, Arc::downgrade(&data));

    Ok(data)
}

#[no_mangle]
extern "C" fn verneuil__snapshot_open(file: &mut SnapshotFile, path: *const c_char) -> SqliteCode {
    #[tracing::instrument]
    fn open(file: &mut SnapshotFile, c_path: *const c_char) -> Result<()> {
        let string = unsafe { std::ffi::CStr::from_ptr(c_path) }
            .to_str()
            .map_err(|e| chain_error!(e, "path is not valid utf-8"))?
            .to_owned();

        let data = fetch_new_data(string)?;
        file.snapshot = Box::leak(Box::new(data)) as *mut Arc<Data> as *mut _;
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
        let snapshot = &self
            .snapshot()
            .ok_or_else(|| fresh_error!("attempted to read uninitialised SnapshotFile"))?
            .data;

        let copied = std::io::copy(&mut snapshot.as_read(offset, dst.len() as u64), &mut dst)
            .map_err(|e| chain_error!(e, "failed to read from snapshot"))?;
        dst.fill(0);
        Ok(copied)
    }

    fn size(&self) -> Result<u64> {
        let snapshot = &self
            .snapshot()
            .ok_or_else(|| fresh_error!("attempted to size uninitialised SnapshotFile"))?
            .data;

        Ok(snapshot.len())
    }
}
