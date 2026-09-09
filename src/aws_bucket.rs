//! Verneuil's bucket facade, dispatching to either an `aws-sdk-s3` 1.x
//! backend (real S3 and GCS via S3-interop) or a native [google-cloud-storage]
//! backend (keyless GCS via Application Default Credentials).
//!
//! Why preserve the rust-s3-shaped `(Vec<u8>, u16)` return: `copier.rs`/
//! `loader.rs` match on the HTTP status code to classify success /
//! permanent-failure / retryable.  Keeping the shape lets us swap the
//! underlying SDK without rewriting the retry loops.
//!
//! Service errors (we reached the server and got a non-2xx response)
//! become `Ok((body, status))` so the existing match arms work;
//! transport-level errors (no response) propagate as `Err`.

use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::imds;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as SdkClient;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_types::region::Region;

use crate::gcs_bucket::GcsInner;

/// Selects which backend the [`Bucket`] facade dispatches to.
enum BackendInner {
    /// AWS SDK: real S3, and GCS over the S3-interop endpoint (HMAC auth).
    S3(SdkClient),
    /// Google's official `google-cloud-storage` SDK: native GCS via
    /// keyless ADC.  See [`crate::gcs_bucket`].
    Gcs(GcsInner),
}

/// Which object-store backend a [`Bucket`] talks to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Backend {
    Aws,
    Gcs,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Aws => "aws",
            Backend::Gcs => "gcs",
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A bucket-bound handle for object operations.
pub(crate) struct Bucket {
    inner: BackendInner,
    // pub(crate) so the existing verneuil call sites that use field access
    // (e.g. `%target.name` in tracing macros) keep working without changes;
    // the `name()` / `region()` accessors below are equivalent.
    pub(crate) name: String,
    /// The store region, when it is meaningful/known: the AWS region for S3,
    /// or the configured GCS location for a native-GCS target.  `None` for a
    /// native-GCS target with no configured region (native GCS reads/writes
    /// don't need one).
    pub(crate) region: Option<Region>,
}

impl std::fmt::Debug for Bucket {
    /// Redact-safe: we never carry credentials in the struct (the SDK's
    /// config holds them privately), so debug-print is just name + backend +
    /// region.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("name", &self.name)
            .field("backend", &self.backend())
            .field("region", &self.region)
            .finish()
    }
}

/// What `parse_s3_region_specification` produces and what [`Bucket::new`]
/// consumes -- aws-sdk-s3 separates region and endpoint, unlike rust-s3's
/// `Region` enum that bundled both.
#[derive(Clone, Debug)]
pub(crate) struct ParsedRegion {
    pub region: Region,
    pub endpoint: Option<String>,
}

/// Boxed error: thin wrapper for the variety of error origins we surface.
pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Eagerly resolve default-chain credentials and discard the result.
///
/// rust-s3's `Credentials::default()` was eager (it fetched values up front
/// and failed if none were available), and the copier uses that as a
/// pre-flight signal -- on failure it sleeps before propagating the error so
/// we don't hot-loop hammering the metadata service.  aws-sdk-s3's provider
/// is *lazy* (constructed without I/O, resolves on first use), which would
/// move credential failures to the first request and skip that sleep.  This
/// helper restores the eager check for the S3 default chain only.  Targets
/// using their own `credentials_process` or the GCS native backend don't
/// rely on the default chain and bypass this verification (see
/// `crate::replication_target::any_target_uses_default_credentials_chain`).
/// IMDS connect/read timeout for credential resolution.
///
/// The SDK default is **1 second**, which is too tight when the host *process*
/// is under load: IMDS itself answers in a few milliseconds (verified with a
/// standalone probe), but a saturated coronerd can fail to poll the connect
/// future within 1 s, so the fetch times out even though the metadata service
/// is healthy. Five seconds is still trivially short next to the real ~5 ms
/// round-trip, but survives scheduling jitter under load.
const IMDS_CREDENTIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// The standard default credential chain (env / profile / IMDS / …) but with an
/// IMDS client that uses [`IMDS_CREDENTIAL_TIMEOUT`] instead of the SDK's 1 s
/// default. Shared by the pre-flight check and by `Bucket::new`'s S3 client.
async fn default_chain_with_imds_timeout() -> DefaultCredentialsChain {
    let imds_client = imds::Client::builder()
        .connect_timeout(IMDS_CREDENTIAL_TIMEOUT)
        .read_timeout(IMDS_CREDENTIAL_TIMEOUT)
        .build();
    DefaultCredentialsChain::builder()
        .imds_client(imds_client)
        .build()
        .await
}

/// Process-wide cache of the last successfully-resolved default-chain
/// credentials. The pre-flight check runs on *every* replication cycle and
/// manifest load; without this it rebuilt the chain and hit IMDS each time, and
/// under load those repeated fetches are exactly what time out. Serving a
/// still-valid cached value keeps IMDS traffic to roughly one refresh per
/// credential lifetime (instance-role creds last hours).
static CACHED_CREDENTIALS: Mutex<Option<Credentials>> = Mutex::new(None);

/// Re-resolve this far ahead of expiry so a cached value is never handed out
/// right as it lapses.
const CREDENTIAL_REFRESH_SLACK: Duration = Duration::from_secs(300);

pub(crate) fn verify_default_credentials() -> Result<(), BoxError> {
    use aws_credential_types::provider::ProvideCredentials;

    // Fast path: a cached credential that isn't about to expire. Credentials
    // with no expiry (e.g. static env creds) never need re-resolution.
    if let Some(creds) = CACHED_CREDENTIALS.lock().unwrap().as_ref() {
        let still_valid = creds
            .expiry()
            .is_none_or(|exp| exp > SystemTime::now() + CREDENTIAL_REFRESH_SLACK);
        if still_valid {
            return Ok(());
        }
    }

    // Slow path: resolve via the default chain (with a generous IMDS timeout)
    // and cache the result for subsequent pre-flights.
    crate::executor::block_on_with_executor(|| async {
        let chain = default_chain_with_imds_timeout().await;
        let creds = chain.provide_credentials().await.map_err(|e| -> BoxError {
            Box::new(VerneuilSdkError(format!("credentials discovery: {e}")))
        })?;
        *CACHED_CREDENTIALS.lock().unwrap() = Some(creds);
        Ok(())
    })
}

impl Bucket {
    /// Build an S3 client against the given region (and optional custom
    /// endpoint, e.g. minio or the GCS S3-interop URL).  Credentials come
    /// from aws-config's default provider chain (env, IMDS, ...), matching
    /// what `s3::creds::Credentials::default()` used to do.
    ///
    /// `force_path_style` selects path-style addressing (bucket in URI
    /// rather than as a hostname prefix); rust-s3 expressed the same choice
    /// via `set_path_style()` / `set_subdomain_style()`.
    ///
    /// `request_timeout` mirrors rust-s3's `set_request_timeout`: it bounds
    /// the per-attempt request duration, mapped to aws-sdk-s3's
    /// `operation_attempt_timeout`.  Internal SDK retries are also
    /// disabled here so that verneuil's own retry loops (in copier.rs /
    /// loader.rs) remain the only retry layer -- matching the behaviour
    /// rust-s3 had (it doesn't retry internally).
    ///
    /// When `credentials_process` is `Some(cmd)`, that shell command is
    /// invoked per the AWS credential_process spec to source credentials,
    /// in place of the default provider chain.  See
    /// [`crate::credentials_process::CredentialsProcessProvider`].
    ///
    /// Synchronous (block-on internally) to match rust-s3's `Bucket::new`
    /// shape, so the verneuil call sites that already create Buckets
    /// synchronously don't have to be restructured.
    pub fn new(
        name: &str,
        parsed: ParsedRegion,
        force_path_style: bool,
        request_timeout: Duration,
        credentials_process: Option<String>,
    ) -> Result<Self, BoxError> {
        crate::executor::block_on_with_executor(|| async move {
            // Per-attempt timeout matches what rust-s3's per-request timeout
            // bounded; disabled retries because verneuil's copier/loader
            // already wrap every op in its own retry loop and we don't want
            // to double-up.
            let timeout = TimeoutConfig::builder()
                .operation_attempt_timeout(request_timeout)
                .build();

            let mut loader = aws_config::defaults(BehaviorVersion::latest())
                .region(parsed.region.clone())
                .timeout_config(timeout)
                .retry_config(RetryConfig::disabled());

            // If the target specifies a credentials_process script, swap it
            // in for the default provider chain.  The SDK's credentials
            // cache wraps the provider automatically, so the script isn't
            // invoked per-request -- it's called when creds are needed and
            // re-called when the cached value expires (per the spec's
            // `Expiration` field).
            if let Some(command) = credentials_process {
                let provider = crate::credentials_process::CredentialsProcessProvider::new(command);
                loader = loader.credentials_provider(SharedCredentialsProvider::new(provider));
            } else {
                // Otherwise use the same default chain, but with a longer IMDS
                // connect timeout than the SDK's 1 s default (too tight under
                // load — see `verify_default_credentials`). The client's own
                // credentials cache still refreshes lazily near expiry.
                let chain = default_chain_with_imds_timeout().await;
                loader = loader.credentials_provider(SharedCredentialsProvider::new(chain));
            }

            let sdk_cfg = loader.load().await;

            // 1.x exposes endpoint override and addressing style directly on
            // the S3-specific config builder (no more crafting an
            // aws_smithy_http::endpoint::Endpoint by hand).
            let mut s3_cfg_b =
                aws_sdk_s3::config::Builder::from(&sdk_cfg).force_path_style(force_path_style);
            if let Some(endpoint) = parsed.endpoint {
                s3_cfg_b = s3_cfg_b.endpoint_url(endpoint);
            }

            Ok(Bucket {
                inner: BackendInner::S3(SdkClient::from_conf(s3_cfg_b.build())),
                name: name.to_string(),
                region: Some(parsed.region),
            })
        })
    }

    /// Build a GCS-native bucket facade.  Uses ADC for auth (keyless)
    /// when `credentials_process` is `None` -- a host with Workload
    /// Identity / an attached SA gets credentials automatically.  When
    /// `Some(cmd)`, the external command sources OAuth tokens instead
    /// (see [`crate::gcs_credentials_process`]).
    ///
    /// `region` (the bucket's GCS location, e.g. `us-west4`) and `project_id`
    /// are optional: native-GCS reads/writes address the bucket by name and
    /// need neither.  `region`, when set, populates the honest
    /// [`Bucket::region`] tracing field; together with `project_id` it also
    /// lets [`Bucket::create_bucket`] create the bucket on demand.  Backend
    /// identity is reported separately via [`Bucket::backend`].
    ///
    /// Synchronous (block-on internally) to match [`Bucket::new`]'s shape.
    pub fn new_gcs(
        name: &str,
        region: Option<String>,
        project_id: Option<String>,
        credentials_process: Option<String>,
    ) -> Result<Self, BoxError> {
        Ok(Bucket {
            inner: BackendInner::Gcs(GcsInner::new(
                project_id,
                region.clone(),
                credentials_process,
            )?),
            name: name.to_string(),
            region: region.map(Region::new),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The backend this bucket talks to (`aws` / `gcs`) -- the honest
    /// tracing/metrics label (see [`Backend`]).
    pub fn backend(&self) -> Backend {
        match &self.inner {
            BackendInner::S3(_) => Backend::Aws,
            BackendInner::Gcs(_) => Backend::Gcs,
        }
    }

    /// The store region, if known (see the [`Bucket::region`] field).
    pub fn region(&self) -> Option<&Region> {
        self.region.as_ref()
    }

    /// The region as a tracing label, falling back to `"unset"` when the
    /// backend has no meaningful/configured region (a native-GCS target
    /// without a configured location).
    pub fn region_label(&self) -> &str {
        self.region.as_ref().map(|r| r.as_ref()).unwrap_or("unset")
    }

    /// PUT an object with a content type.  Returns `(body, status)` (the
    /// body is empty on success -- verneuil only reads `status`).
    pub async fn put_object_with_content_type(
        &self,
        key: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16), BoxError> {
        match &self.inner {
            BackendInner::S3(client) => {
                match client
                    .put_object()
                    .bucket(&self.name)
                    .key(key)
                    .content_type(content_type)
                    .body(ByteStream::from(bytes.to_vec()))
                    .send()
                    .await
                {
                    Ok(_) => Ok((Vec::new(), 200)),
                    Err(e) => sdk_err_to_body_code(e),
                }
            }
            BackendInner::Gcs(gcs) => {
                gcs.put_object_with_content_type(&self.name, key, bytes, content_type)
                    .await
            }
        }
    }

    /// GET an object.  Returns `(body, status)`; on success, body is the
    /// object's bytes.
    pub async fn get_object(&self, key: &str) -> Result<(Vec<u8>, u16), BoxError> {
        match &self.inner {
            BackendInner::S3(client) => {
                match client.get_object().bucket(&self.name).key(key).send().await {
                    Ok(resp) => {
                        let agg = resp
                            .body
                            .collect()
                            .await
                            .map_err(|e| format!("read GET body: {e}"))?;
                        Ok((agg.into_bytes().to_vec(), 200))
                    }
                    Err(e) => sdk_err_to_body_code(e),
                }
            }
            BackendInner::Gcs(gcs) => gcs.get_object(&self.name, key).await,
        }
    }

    /// HEAD the bucket -- used to check whether it exists before deciding
    /// whether to create it.  On the GCS path this is `get_bucket`.
    pub async fn head_bucket(&self) -> Result<(Vec<u8>, u16), BoxError> {
        match &self.inner {
            BackendInner::S3(client) => {
                match client.head_bucket().bucket(&self.name).send().await {
                    Ok(_) => Ok((Vec::new(), 200)),
                    Err(e) => sdk_err_to_body_code(e),
                }
            }
            BackendInner::Gcs(gcs) => gcs.head_bucket(&self.name).await,
        }
    }

    /// CreateBucket -- on-demand creation, the same intent as rust-s3's
    /// `Bucket::create[_with_path_style]`.  Not supported on the GCS
    /// native path (see [`GcsInner::create_bucket`] for why).
    pub async fn create_bucket(&self) -> Result<(Vec<u8>, u16), BoxError> {
        match &self.inner {
            BackendInner::S3(client) => {
                match client.create_bucket().bucket(&self.name).send().await {
                    Ok(_) => Ok((Vec::new(), 200)),
                    Err(e) => sdk_err_to_body_code(e),
                }
            }
            BackendInner::Gcs(gcs) => gcs.create_bucket(&self.name).await,
        }
    }

    /// "Touch" an object -- on the AWS path, CopyObject self-to-self with
    /// `MetadataDirective::Replace`; on the GCS path, patch `custom_time`.
    /// Either way the object's modification timestamp is bumped without
    /// re-uploading bytes (verneuil uses this to nudge consumers that a
    /// manifest is fresh).
    pub async fn copy_object_to_self(
        &self,
        key: &str,
        content_type: &str,
    ) -> Result<(Vec<u8>, u16), BoxError> {
        match &self.inner {
            BackendInner::S3(client) => {
                // copy_source = "{bucket}/{url-encoded-key}".  Match rust-s3's
                // encoding choice: NON_ALPHANUMERIC encodes slashes too, since
                // CopyObject's source header treats them as separators if
                // left bare.
                let encoded =
                    percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC)
                        .to_string();
                let copy_source = format!("{}/{}", self.name, encoded);

                match client
                    .copy_object()
                    .bucket(&self.name)
                    .key(key)
                    .copy_source(copy_source)
                    .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
                    .content_type(content_type)
                    .send()
                    .await
                {
                    Ok(_) => Ok((Vec::new(), 200)),
                    Err(e) => sdk_err_to_body_code(e),
                }
            }
            BackendInner::Gcs(gcs) => gcs.copy_object_to_self(&self.name, key, content_type).await,
        }
    }
}

/// Map an `SdkError<E>` back into the `(body, status)` shape verneuil's
/// retry loops expect.  Service / response errors (we reached S3 and got a
/// non-2xx) become `Ok((body, status))`; transport / dispatch / construction
/// errors (no HTTP response) propagate as `Err`.
///
/// `R` is fixed to `aws_smithy_runtime_api::http::Response` -- aws-sdk-s3
/// 1.x parameterises every operation's `SdkError` over that concrete raw-
/// response type, so fixing it here lets us call `.status()` without an
/// additional trait bound.
fn sdk_err_to_body_code<E>(
    err: SdkError<E, aws_smithy_runtime_api::http::Response>,
) -> Result<(Vec<u8>, u16), BoxError>
where
    E: std::error::Error + Send + Sync + 'static,
{
    if let Some(http) = err.raw_response() {
        let status = http.status().as_u16();
        // In 1.x, raw body access on errors is awkward (it's been buffered
        // by the SDK to parse the error and isn't easily re-readable here);
        // verneuil only matches on the status code, so empty body is fine.
        return Ok((Vec::new(), status));
    }
    Err(Box::new(VerneuilSdkError(format!("{err:?}"))))
}

#[derive(Debug)]
struct VerneuilSdkError(String);

impl std::fmt::Display for VerneuilSdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for VerneuilSdkError {}
