use std::borrow::Cow;
use std::boxed::Box;
use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::SystemTime;

use crate::chain_error;
use crate::fresh_error;
use crate::sqlite_code::SqliteCode;
use crate::sqlite_lock_level::LockLevel;
use crate::Result;
use crate::Snapshot;

/// Number of background refresh workers.
const REFRESH_POOL_SIZE: usize = 4;

struct Data {
    /// URI for the manifest.
    path: String,

    /// A lower bound on the time at which we updated the manifest for `data`.
    updated: SystemTime,

    /// ctime on the replicated source db file, as advertised by the
    /// manifest.
    ctime: u64,
    ctime_ns: u32,

    /// This flag is set when a reload has been queued to replace
    /// this snapshot data, and cleared when the reload is complete.
    reload_queued: AtomicBool,

    /// Replica data.
    data: Snapshot,
}

// See vfs.c
#[derive(Debug)]
#[repr(C)]
struct SnapshotFile {
    methods: *const c_void,
    locked: AtomicBool,

    /// Whether to refresh the snapshot before acquiring a read lock.
    auto_refresh: AtomicBool,
    snapshot: AtomicPtr<c_void>, // Really a &mut Arc<Data>.
}

// See vfs.h
#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
struct Timestamp {
    seconds: u64,
    nanos: u32,
}

impl std::convert::From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        use std::time::Duration;

        let since_epoch = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0));

        Timestamp {
            seconds: since_epoch.as_secs(),
            nanos: since_epoch.subsec_nanos(),
        }
    }
}

impl SnapshotFile {
    /// Returns a reference to this `SnapshotFile`'s `Data`, if
    /// it is populated.
    #[inline]
    fn snapshot(&self) -> Option<&Arc<Data>> {
        unsafe { (self.snapshot.load(Ordering::Relaxed) as *const Arc<Data>).as_ref() }
    }

    /// Exchanges the snapshot pointer with `new` and returns the old
    /// value, if any.
    #[inline]
    fn exchange(&self, new: *mut Arc<Data>) -> Option<Box<Arc<Data>>> {
        let ptr = self.snapshot.swap(new as *mut _, Ordering::Relaxed) as *mut Arc<Data>;

        let snapshot = unsafe { ptr.as_mut() }?;
        Some(unsafe { Box::from_raw(snapshot) })
    }

    /// Replaces the data in this `SnapshotFile` with `data`.
    ///
    /// Replacing the underlying data snapshot while sqlite holds a
    /// read lock would violate invariants, so we no-op in that case.
    #[inline]
    fn set_snapshot(&self, data: Arc<Data>) {
        if !self.locked.load(Ordering::Relaxed) {
            self.exchange(Box::leak(Box::new(data)) as *mut Arc<Data>);
        }
    }

    /// Replaces the `snapshot` pointer in this `SnapshotFile` with a
    /// NULL pointer, and returns the old `Data`, if it was
    /// populated.
    #[inline]
    fn consume_snapshot(&self) -> Option<Box<Arc<Data>>> {
        self.exchange(std::ptr::null_mut())
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

/// Constructs a fresh snapshot for `path`, updates the global cache,
/// and returns the result.
fn fetch_new_data(path: String) -> Result<Arc<Data>> {
    use prost::Message;

    let start = SystemTime::now();
    let bytes = match crate::manifest_bytes_for_path(None, &path)? {
        Some(bytes) => bytes,
        None => return Err(fresh_error!("manifest not found", %path)),
    };

    let manifest = crate::Manifest::decode(&*bytes)
        .map_err(|e| chain_error!(e, "failed to parse manifest file", %path))?;

    let (ctime, ctime_ns) = match &manifest.v1 {
        Some(v1) => (
            v1.ctime.clamp(0, i64::MAX) as u64,
            v1.ctime_ns.clamp(0, 999_999_999) as u32,
        ),
        None => (0, 0),
    };

    let data = Arc::new(Data {
        path: path.clone(),
        updated: start,
        ctime,
        ctime_ns,
        reload_queued: AtomicBool::new(false),
        data: Snapshot::new_with_default_targets(&manifest)?,
    });

    LIVE_DATA
        .lock()
        .expect("mutex should be valid")
        .insert(path, Arc::downgrade(&data));

    Ok(data)
}

/// Gets a snapshot for `path`; only constructs a fresh one on cache
/// misses.
fn get_data(path: Cow<str>) -> Result<Arc<Data>> {
    {
        if let Some(ret) = LIVE_DATA
            .lock()
            .expect("mutex should be valid")
            .get(&*path)
            .and_then(Weak::upgrade)
        {
            return Ok(ret);
        }
    }

    fetch_new_data(path.into_owned())
}

/// Ensures an asynchronous refresh for `data`'s manifest path is
/// scheduled or already executing.
fn async_update(data: Arc<Data>) {
    lazy_static::lazy_static! {
        static ref REFRESH_POOL: rayon::ThreadPool = rayon::ThreadPoolBuilder::new().num_threads(REFRESH_POOL_SIZE)
            .thread_name(|i| format!("verneuil-refresh-worker/{}", i))
            .build()
            .expect("failed to build global refresh pool");
    }

    // Bail if there's already a reload in flight.
    if data.reload_queued.swap(true, Ordering::Relaxed) {
        return;
    }

    REFRESH_POOL.spawn_fifo(move || {
        let _ = fetch_new_data(data.path.clone());
        data.reload_queued.store(false, Ordering::Relaxed);
    });
}

#[no_mangle]
extern "C" fn verneuil__snapshot_open(file: &mut SnapshotFile, path: *const c_char) -> SqliteCode {
    #[tracing::instrument]
    fn open(file: &mut SnapshotFile, c_path: *const c_char) -> Result<()> {
        let string = unsafe { std::ffi::CStr::from_ptr(c_path) }
            .to_str()
            .map_err(|e| chain_error!(e, "path is not valid utf-8"))?
            .to_owned();

        file.set_snapshot(get_data(string.into())?);
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
extern "C" fn verneuil__snapshot_lock(file: &SnapshotFile, level: LockLevel) -> SqliteCode {
    if level > LockLevel::Shared {
        // We can't take a write lock on a snapshot: we can't write to
        // snapshots.
        return SqliteCode::Perm;
    }

    if level == LockLevel::None {
        // Locking to nothing is a no-op.
        return SqliteCode::Ok;
    }

    if !file.locked.load(Ordering::Relaxed) && file.auto_refresh.load(Ordering::Relaxed) {
        if let Some(data) = file.snapshot() {
            if let Ok(update) = get_data((&data.path).into()) {
                file.set_snapshot(update);
            }
        }
    }

    file.locked.store(true, Ordering::Relaxed);
    SqliteCode::Ok
}

#[no_mangle]
extern "C" fn verneuil__snapshot_unlock(file: &SnapshotFile, level: LockLevel) -> SqliteCode {
    if level == LockLevel::None {
        file.locked.store(false, Ordering::Relaxed);
    }

    SqliteCode::Ok
}

/// Refreshes the snapshot data.  If `force`, always gets a fresh
/// snapshot; otherwise, uses the latest available data.
#[no_mangle]
extern "C" fn verneuil__snapshot_refresh(
    file: &SnapshotFile,
    update_ts: &mut Timestamp,
    len: &mut usize,
    force: bool,
) -> *const c_char {
    fn doit(file: &SnapshotFile, force: bool) -> Result<SystemTime> {
        let path = &file
            .snapshot()
            .ok_or_else(|| fresh_error!("attempted to read uninitialised SnapshotFile"))?
            .path;

        let data = if force {
            fetch_new_data(path.clone())
        } else {
            get_data(path.into())
        }?;

        let ts = data.updated;
        file.set_snapshot(data);
        Ok(ts)
    }

    *update_ts = Default::default();
    *len = 0;
    match doit(file, force) {
        Ok(updated) => {
            *update_ts = updated.into();
            std::ptr::null()
        }
        Err(e) => {
            let msg: &'static str = e.message;
            *len = msg.len();
            msg.as_ptr() as *const c_char
        }
    }
}

/// Refreshes the snapshot data.  If the connection is already up to
/// date, schedules a background force update.
///
/// Returns whether we found fresh data without having to trigger an
/// async reload.
#[no_mangle]
extern "C" fn verneuil__snapshot_async_reload(file: &SnapshotFile) -> bool {
    let data = match file.snapshot() {
        Some(data) => data,
        None => return false,
    };

    let update = match get_data((&data.path).into()) {
        Ok(update) => update,
        Err(_) => return false,
    };

    if Arc::ptr_eq(data, &update) {
        async_update(update);
        // No fresh snapshot available, we have to trigger an async
        // update.
        false
    } else {
        file.set_snapshot(update);
        true
    }
}

/// Returns the `ctime` for the file's current snapshot data,
/// and stores the fractional nanosecond part in `ns`.
#[no_mangle]
extern "C" fn verneuil__snapshot_ctime(file: &SnapshotFile) -> Timestamp {
    file.snapshot()
        .map(|data| Timestamp {
            seconds: data.ctime,
            nanos: data.ctime_ns,
        })
        .unwrap_or_default()
}

/// Returns the `updated` time for the file's current snapshot.
#[no_mangle]
extern "C" fn verneuil__snapshot_updated(file: &SnapshotFile) -> Timestamp {
    file.snapshot()
        .map(|data| data.updated.into())
        .unwrap_or_default()
}

/// Overrides the `auto_refresh` (before read lock) flag on `file`.
///
/// Returns the old value.
#[no_mangle]
extern "C" fn verneuil__snapshot_auto_refresh(file: &SnapshotFile, update: bool) -> bool {
    file.auto_refresh.swap(update, Ordering::Relaxed)
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
