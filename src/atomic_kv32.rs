use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// An AtomicKV32 is an atomically-updatable pair of two u32s:
/// a key and a value.
///
/// When the key changes, an AtomicKV32 lets the *last* write win,
/// but, otherwise, the *first* write wins for a series of updates
/// with the same key (and potentially different values).
pub(crate) struct AtomicKV32 {
    // The low half is the key, high half the value.
    value: AtomicU64,
}

impl AtomicKV32 {
    /// Creates a fresh AtomicKV32 with this key and value.
    pub const fn new(key: u32, value: u32) -> AtomicKV32 {
        AtomicKV32 {
            value: AtomicU64::new(Self::encode(key, value)),
        }
    }

    /// Returns the current key-value pair stored in this `AtomicKV32`.
    ///
    /// The first element of the tuple is the key, the second the value.
    pub fn get(&self) -> (u32, u32) {
        // Acquire matches the compare_exchange in `set`.
        Self::decode(self.value.load(Ordering::Acquire))
    }

    /// Updates this `AtomicKV32`'s contents to the key/value pair,
    /// unless the current contents' key is identical.
    ///
    /// Returns the potentially updated contents on exit.
    pub fn set(&self, key: u32, value: u32) -> (u32, u32) {
        let update = Self::encode(key, value);

        // Acquire matches the compare_exchange below
        let mut current = self.value.load(Ordering::Acquire);
        while Self::decode(current).0 != key {
            // The ordering here is stronger than necessary, but we
            // only call `set()` about once per process.
            match self
                .value
                .compare_exchange(current, update, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_prev) => current = update, // success!
                Err(actual) => current = actual,
            }
        }

        Self::decode(current)
    }

    /// Packs a key-value pair in a u64.
    const fn encode(key: u32, value: u32) -> u64 {
        // Stick the key in the low half for easier extraction.
        ((value as u64) << 32) | (key as u64)
    }

    /// Unpacks a u64 into a key-value pair.
    const fn decode(bits: u64) -> (u32, u32) {
        (bits as u32, (bits >> 32) as u32)
    }
}

#[test]
fn test_encode_decode() {
    let ints = [0, 1, 2, 3, 1000, u32::MAX - 2, u32::MAX - 1, u32::MAX];

    for key in ints {
        for value in ints {
            assert_eq!(
                AtomicKV32::decode(AtomicKV32::encode(key, value)),
                (key, value)
            );
        }
    }
}

#[test]
fn test_init() {
    let ints = [0, 1, 2, 3, 1000, u32::MAX - 2, u32::MAX - 1, u32::MAX];

    for key in ints {
        for value in ints {
            let pair = AtomicKV32::new(key, value);

            assert_eq!(pair.get(), (key, value));

            // Make sure we can update it.
            pair.set(42, 1234);
            assert_eq!(pair.get(), (42u32, 1234u32));
        }
    }
}

#[test]
fn test_set() {
    let pair = AtomicKV32::new(0, 0);

    // Same key -> should no-op
    pair.set(0, 42);
    assert_eq!(pair.get(), (0u32, 0u32));

    // Different key -> should succeed
    assert_eq!(pair.set(1000, u32::MAX), (1000u32, u32::MAX));
    assert_eq!(pair.get(), (1000u32, u32::MAX));

    // Same key -> should no-op
    assert_eq!(pair.set(1000, 0), (1000u32, u32::MAX));
    assert_eq!(pair.get(), (1000u32, u32::MAX));

    // Different key -> should succeed
    pair.set(0, 42);
    assert_eq!(pair.get(), (0u32, 42u32));
}
