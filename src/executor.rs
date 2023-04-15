//! The executor module manages a cached thread-local "current thread"
//! tokio runtime.
use tokio::runtime;
use tokio::runtime::Runtime;

/// Invokes `fun` with a reference to the thread-local tokio runtime,
/// or panics if the thread is being unwound.  The function is called
/// within the runtime's context
/// <https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.enter>.
///
/// `tokio::runtime::Handle`s are broken when wrapping "current
/// thread" executors.  Always use this function to get an executor.
pub(crate) fn call_with_executor<T>(fun: impl FnOnce(&Runtime) -> T) -> T {
    std::thread_local! {
        static RUNTIME: Runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create a new tokio runtime");
    }

    RUNTIME.with(|rt| {
        let _scope = rt.enter();
        fun(rt)
    })
}

/// Invokes `fun` within the context of the current thread's executor,
/// and blocks on the resulting future.
pub(crate) fn block_on_with_executor<T, F: std::future::Future<Output = T>>(
    fun: impl FnOnce() -> F,
) -> T {
    call_with_executor(|rt| rt.block_on(fun()))
}
