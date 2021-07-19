use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

/// A `RacyDuration` can be updated without holding a lock, but reads
/// and writes will tear.  We expect but don't enforce a single
/// writer, in which case any tearing will only affect the sub-second
/// part of the duration.
#[derive(Default, Debug)]
pub(crate) struct RacyDuration {
    seconds: AtomicU64,
    nanos: AtomicU32,
}

#[allow(unused)]
impl RacyDuration {
    pub fn new(from: Duration) -> RacyDuration {
        let ret: RacyDuration = Default::default();

        ret.store(from);
        ret
    }

    pub fn load(&self) -> Duration {
        let sec = self.seconds.load(Ordering::Relaxed);
        let ns = self.nanos.load(Ordering::Relaxed);

        Duration::new(sec, ns)
    }

    pub fn store(&self, update: Duration) {
        self.nanos.store(update.subsec_nanos(), Ordering::Relaxed);
        self.seconds.store(update.as_secs(), Ordering::Relaxed);
    }
}

/// A `RacySystemTime` can be updated without holding a lock, but
/// reads and writes will tear.  We expect but don't enforce a single
/// writer, in which case any tearing will only affect the sub-second
/// part of the timestamp.
#[derive(Default, Debug)]
pub(crate) struct RacySystemTime {
    since_epoch: RacyDuration,
}

#[allow(unused)]
impl RacySystemTime {
    pub fn new(from: SystemTime) -> RacySystemTime {
        let ret: RacySystemTime = Default::default();

        ret.store(from);
        ret
    }

    pub fn load(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + self.since_epoch.load()
    }

    pub fn store(&self, update: SystemTime) {
        self.since_epoch.store(
            update
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default(),
        );
    }

    pub fn store_now(&self) {
        self.store(SystemTime::now());
    }
}
