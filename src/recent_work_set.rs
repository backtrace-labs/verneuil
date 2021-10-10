use std::time::Duration;
use std::time::Instant;

use umash::Fingerprint;

/// A `RecentWorkSet` is a bounded set of recently processed work
/// units.  It lets us determine whether we have (nearly) definitely
/// performed a given work unit in the recent past, or give up if that
/// information isn't known.
///
/// The interface is type-erased at the `WorkUnit` level, for
/// convenience.  However, when storing `WorkUnit`s for different
/// types in the same `RecentWorkSet`, each type should include
/// a type-specific value.
pub(crate) struct RecentWorkSet {
    /// A bounded map from work unit fingerprint to the time it was
    /// processed.
    recent: lru::LruCache<Fingerprint, Instant>,

    /// Subtract a random duration up to `offset_range` from the time
    /// at which the work unit was performed.  This avoids thundering
    /// herds by ensuring work units expire at different times.
    offset_range: Duration,
}

/// `RecentWorkSet`s track `WorkUnit`s that can be constructed from
/// any hashable value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkUnit {
    fprint: Fingerprint,
    pub time: Instant,
}

impl WorkUnit {
    /// Returns a `WorkUnit` descriptor for `task` performed at `now`.
    ///
    /// While the type name is included in the `WorkUnit`'s
    /// fingerprint, there is no guarantee that type names are unique.
    pub fn new<T: std::hash::Hash>(task: T) -> Self {
        lazy_static::lazy_static! {
            // We can use a pseudorandom `Params`, as long as it's the
            // same for all `RecentWorkSet`; we use the same `Params`
            // for the whole process.
            static ref PARAMS: umash::Params = umash::Params::new();
        }

        let key = (std::any::type_name::<T>(), task);

        Self {
            fprint: PARAMS.fingerprint(key),
            time: Instant::now(),
        }
    }
}

impl RecentWorkSet {
    /// Returns a new `RecentWorkSet` that will remember up to
    /// `capacity` work units.
    ///
    /// Whenever a work unit is inserted in this `RecentWorkSet`, up
    /// to `offset_range` will be subtracted from the work unit's
    /// start timestamp.  This avoids thundering herds by ensuring
    /// work units expire at different times.
    pub fn new(capacity: usize, offset_range: Duration) -> RecentWorkSet {
        RecentWorkSet {
            recent: lru::LruCache::new(capacity),
            offset_range,
        }
    }

    /// Forgets all recent work units.
    pub fn clear(&mut self) {
        self.recent.clear();
    }

    /// Remembers that we performed `work_unit` at (or before) `time`.
    pub fn observe(&mut self, work_unit: &WorkUnit) {
        use rand::Rng;

        let random = rand::thread_rng().gen_range(0.0..=1.0);
        let time = work_unit
            .time
            .checked_sub(self.offset_range.mul_f64(random))
            .unwrap_or(work_unit.time);
        self.recent.put(work_unit.fprint, time);
    }

    /// Determines whether we definitely performed `work_unit` at most
    /// `age` ago.
    ///
    /// If we did, returns the time at which we did.  Otherwise, returns None.
    pub fn has_recent(&mut self, work_unit: &WorkUnit, age: Duration) -> Option<Instant> {
        match self.recent.get(&work_unit.fprint) {
            Some(time) if time.elapsed() <= age => Some(*time),
            _ => None,
        }
    }
}
