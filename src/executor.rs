//! The executor module manages the process-wide tokio runtime that
//! verneuil uses to drive its async SDK work.
use tokio::runtime;
use tokio::runtime::Runtime;

/// Verneuil drives all of its async SDK calls (aws-sdk-s3, google-cloud-storage)
/// through [`block_on_with_executor`].  We back it with a single, process-wide
/// **multi-thread** tokio runtime rather than a per-thread *current-thread* one.
///
/// Why a shared multi-thread runtime: the aws-sdk-s3 (and google-cloud-storage)
/// clients hold a persistent connection pool whose background I/O tasks live on
/// the tokio runtime that drives their requests.  A client is built once (e.g.
/// in [`crate::loader::Loader::new`]) and then reused for many chunk fetches
/// that fan out across a rayon pool.  With a *thread-local* current-thread
/// runtime, each rayon worker had its own runtime, so a request issued from one
/// worker could depend on connection/background tasks pinned to a different
/// worker's runtime that wasn't being driven -- stalling parallel GETs until the
/// per-attempt timeout fired (observed against the GCS S3-interop endpoint).  A
/// shared multi-thread runtime drives every request and every connection task on
/// its own worker threads, so concurrent `block_on` calls from many rayon
/// threads all make progress and share one connection pool.  (rust-s3 avoided
/// this by building a fresh, lightweight client per call.)
fn shared_runtime() -> &'static Runtime {
    lazy_static::lazy_static! {
        static ref RUNTIME: Runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build the shared verneuil tokio runtime");
    }

    &RUNTIME
}

/// Invokes `fun` with a reference to verneuil's shared tokio runtime, within the
/// runtime's context
/// <https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.enter>.
pub(crate) fn call_with_executor<T>(fun: impl FnOnce(&Runtime) -> T) -> T {
    let rt = shared_runtime();
    let _scope = rt.enter();
    fun(rt)
}

/// Invokes `fun` within the context of verneuil's shared executor, and blocks
/// on the resulting future.
pub(crate) fn block_on_with_executor<T, F: std::future::Future<Output = T>>(
    fun: impl FnOnce() -> F,
) -> T {
    call_with_executor(|rt| rt.block_on(fun()))
}
