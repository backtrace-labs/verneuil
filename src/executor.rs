//! The executor module manages a cached thread-local "current thread"
//! tokio runtime.
use tokio::runtime;
use tokio::runtime::Runtime;

/// Invokes `fun` with a reference to the thread-local tokio runtime,
/// or panics if the thread is being unwound.
pub(crate) fn call_with_executor<T>(fun: impl FnOnce(&Runtime) -> T) -> T {
    std::thread_local! {
        static RUNTIME: Runtime = runtime::Builder::new_current_thread().enable_all().build().expect("failed to create a new tokio runtime");
    }

    RUNTIME.with(fun)
}
