//! The Verneuil library uses `Result`s with a simple error type, and relies
//! on tracing / logging to track information about provenance and to map
//! low-level errors to higher-level operations.
pub use tracing::Level;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

/// Emit a backtrace whenever we capture an errors at least as severe
/// as BACKTRACE_SEVERITY.
const BACKTRACE_SEVERITY: Level = Level::ERROR;

/// An `Error` is a lightweight struct that relies on the `tracing` crate
/// to stitch up context together after an error.
#[derive(Debug)]
pub struct Error {
    // The uuid for the initial ("root") error.
    initial_id: Uuid,
    pub message: &'static str,
}

impl Error {
    /// Creates a new `Error` struct; this constructor should onl
    /// be called via the macros.
    #[allow(dead_code)]
    #[inline(always)]
    pub fn new(initial_id: Uuid, message: &'static str) -> Self {
        Error {
            initial_id,
            message,
        }
    }

    /// Converts `self` to a `std::io::Error`.
    #[allow(dead_code)]
    #[inline(always)]
    pub fn to_io(&self) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, self.message)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

/// Only exported for macro use
///
/// Computes a backtrace if `level` is severe enough.
#[inline(always)]
pub fn __maybe_compute_backtrace(level: Level) -> Option<backtrace::Backtrace> {
    if level > BACKTRACE_SEVERITY {
        return None;
    }

    Some(backtrace::Backtrace::new())
}

/// Only exported for macro use.
///
/// If `T == Error`, returns `x`'s `initial_id and `(None, None)`.
/// Otherwise, returns a fresh uuid, `x`, and potentially a backtrace.
#[inline(always)]
pub fn __extract_cause_info<T: std::any::Any>(
    x: T,
    level: Level,
) -> (Uuid, Option<T>, Option<backtrace::Backtrace>) {
    use std::any::Any;

    match (&x as &dyn Any).downcast_ref::<Error>() {
        Some(as_error) => (as_error.initial_id, None, None),
        None => (Uuid::new_v4(), Some(x), __maybe_compute_backtrace(level)),
    }
}

/// If `value` evaluates to `Err`, matches the error payload against
/// the patterns, evaluates the corresponding handling expression, and
/// drops the result.
#[macro_export]
macro_rules! drop_result {
    ($value:expr, $($($pattern:pat)|+ $(if $guard:expr)? => $handler:expr),+) => {
        if let Err(name) = $value {
            match name {
                $($($pattern)|+ $(if $guard)? => { let _ = $handler; }),+
            }
        }
    };
}

/// Returns a fresh `Error` struct, after tracing it at level `level`,
/// with `message` and additional fields passed as a `tracing::event`.
#[macro_export]
macro_rules! fresh {
    ($level:expr, $message:expr $(,)?) => {{
        #[allow(unused)]
        const LEVEL: tracing::Level = $level;
        let root_id = uuid::Uuid::new_v4();
        let bt = $crate::result::__maybe_compute_backtrace(LEVEL);
        let message = $message;
        let ret = $crate::result::Error::new(root_id, message);

        tracing::event!(LEVEL, %root_id, ?bt, $message);
        ret
    }};
    ($level:expr, $message:expr, $($fields:tt)+) => {{
        #[allow(unused)]
        const LEVEL: tracing::Level = $level;
        let root_id = uuid::Uuid::new_v4();
        let bt = $crate::result::__maybe_compute_backtrace(LEVEL);
        let message = $message;
        let ret = $crate::result::Error::new(root_id, message);

        tracing::event!(LEVEL, $($fields)+, %root_id, ?bt, $message);
        ret
    }};
}

#[macro_export]
macro_rules! fresh_error {
    ($($message_and_fields:tt)+) => { $crate::fresh!(tracing::Level::ERROR, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! fresh_warn {
    ($($message_and_fields:tt)+) => { $crate::fresh!(tracing::Level::WARN, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! fresh_info {
    ($($message_and_fields:tt)+) => { $crate::fresh!(tracing::Level::INFO, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! fresh_debug {
    ($($message_and_fields:tt)+) => { $crate::fresh!(tracing::Level::DEBUG, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! fresh_trace {
    ($($message_and_fields:tt)+) => { $crate::fresh!(tracing::Level::TRACE, $($message_and_fields)+) };
}

/// Returns an `Error` struct derived from `initial`, after tracing it
/// at level `level`, with `message` and additional fields passed as a
/// `tracing::event`.
#[macro_export]
macro_rules! chain {
    ($initial:expr, $level:expr, $message:expr $(,)?) => {{
        #[allow(unused)]
        const LEVEL: tracing::Level = $level;
        let (root_id, cause, bt) = $crate::result::__extract_cause_info($initial, LEVEL);
        let message = $message;
        let ret = $crate::result::Error::new(root_id, message);

        tracing::event!(LEVEL, %root_id, ?cause, ?bt, $message);
        ret
    }};
    ($initial:expr, $level:expr, $message:expr, $($fields:tt)+) => {{
        #[allow(unused)]
        const LEVEL: tracing::Level = $level;
        let (root_id, cause, bt) = $crate::result::__extract_cause_info($initial, LEVEL);
        let message = $message;
        let ret = $crate::result::Error::new(root_id, message);

        tracing::event!(LEVEL, $($fields)+, %root_id, ?cause, ?bt, $message);
        ret
    }};
}

#[macro_export]
macro_rules! chain_error {
    ($initial:expr, $($message_and_fields:tt)+) => { $crate::chain!($initial, tracing::Level::ERROR, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! chain_warn {
    ($initial:expr, $($message_and_fields:tt)+) => { $crate::chain!($initial, tracing::Level::WARN, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! chain_info {
    ($initial:expr, $($message_and_fields:tt)+) => { $crate::chain!($initial, tracing::Level::INFO, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! chain_debug {
    ($initial:expr, $($message_and_fields:tt)+) => { $crate::chain!($initial, tracing::Level::DEBUG, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! chain_trace {
    ($initial:expr, $($message_and_fields:tt)+) => { $crate::chain!($initial, tracing::Level::TRACE, $($message_and_fields)+) };
}

/// Logs and creates a fresh `Error` struct from the last OS error.
#[macro_export]
macro_rules! from_os {
    ($level:expr, $($message_and_fields:tt)+) => {
        $crate::chain!(std::io::Error::last_os_error(), $level, $($message_and_fields)+)
    };
}

#[macro_export]
macro_rules! error_from_os {
    ($($message_and_fields:tt)+) => { $crate::from_os!(tracing::Level::ERROR, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! warn_from_os {
    ($($message_and_fields:tt)+) => { $crate::from_os!(tracing::Level::WARN, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! info_from_os {
    ($($message_and_fields:tt)+) => { $crate::from_os!(tracing::Level::INFO, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! debug_from_os {
    ($($message_and_fields:tt)+) => { $crate::from_os!(tracing::Level::DEBUG, $($message_and_fields)+) };
}
#[macro_export]
macro_rules! trace_from_os {
    ($($message_and_fields:tt)+) => { $crate::from_os!(tracing::Level::TRACE, $($message_and_fields)+) };
}

/// Creates a fresh `Error` struct from the `std::io::Error` `error`,
/// and logs it at a dynamic level: if the error's kind matches
/// the pattern, the level is `benign_level`, otherwise it's `ERROR`.
#[macro_export]
macro_rules! filtered_io_error {
    ($error:expr, $($benign_kind:pat)|+ $(if $guard:expr)? => $benign_level:expr, $($message_and_fields:tt)+) => {{
        let err = $error;
        match err.kind() {
            $($benign_kind)|+ $(if $guard)? => $crate::chain!(err, $benign_level, $($message_and_fields)+),
            _ => $crate::chain!(err, tracing::Level::ERROR, $($message_and_fields)+),
        }
    }};
}

/// Creates a fresh `Error` struct from the last OS error, and logs it
/// at a dynamic level: if the OS error's kind matches the pattern,
/// the level is `benign_level`, otherwise it's `ERROR`.
#[macro_export]
macro_rules! filtered_os_error {
    ($($pattern_message_and_fields:tt)+) => {
        $crate::filtered_io_error!(std::io::Error::last_os_error(), $($pattern_message_and_fields)+)
    };
}
