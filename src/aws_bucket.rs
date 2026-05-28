//! A thin wrapper around `aws-sdk-s3` exposing the operations verneuil
//! actually uses (put / get / head-bucket / create-bucket / self-copy),
//! preserving the `(Vec<u8>, u16)` response shape that rust-s3's API had.
//!
//! Why preserve that shape: `copier.rs`/`loader.rs` match on the HTTP status
//! code to classify success / permanent-failure / retryable.  Keeping the
//! shape lets us swap the SDK without rewriting the retry loops.
//!
//! Service errors (a request reached S3 and got a non-2xx response) become
//! `Ok((body, status))` so the existing match arms work; transport-level
//! errors (no response) propagate as `Err`.

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as SdkClient;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_types::region::Region;

/// A bucket-bound handle for object operations.
pub(crate) struct Bucket {
    client: SdkClient,
    // Public(crate) so the existing verneuil call sites that use field
    // access (e.g. `%target.name` in tracing macros) keep working without
    // changes; the `name()` / `region()` accessors below are equivalent.
    pub(crate) name: String,
    pub(crate) region: Region,
}

impl std::fmt::Debug for Bucket {
    /// Redact-safe: we never carry credentials in the struct (the SDK's
    /// config holds them privately), so debug-print is just name + region.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("name", &self.name)
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
/// helper restores the eager check.
pub(crate) fn verify_default_credentials() -> Result<(), BoxError> {
    use aws_credential_types::provider::ProvideCredentials;
    crate::executor::block_on_with_executor(|| async {
        let sdk_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let provider = sdk_cfg.credentials_provider().ok_or_else(|| {
            Box::new(VerneuilSdkError(
                "no credentials provider in default chain".to_string(),
            )) as BoxError
        })?;
        provider
            .provide_credentials()
            .await
            .map(|_| ())
            .map_err(|e| -> BoxError {
                Box::new(VerneuilSdkError(format!("credentials discovery: {e}")))
            })
    })
}

impl Bucket {
    /// Build a client against the given region (and optional custom
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
    /// Synchronous (block-on internally) to match rust-s3's `Bucket::new`
    /// shape, so the verneuil call sites that already create Buckets
    /// synchronously don't have to be restructured.
    pub fn new(
        name: &str,
        parsed: ParsedRegion,
        force_path_style: bool,
        request_timeout: Duration,
    ) -> Result<Self, BoxError> {
        crate::executor::block_on_with_executor(|| async move {
            // Per-attempt timeout matches what rust-s3's per-request timeout
            // bounded; disabled retries because verneuil's copier/loader
            // already wrap every op in its own retry loop and we don't want
            // to double-up.
            let timeout = TimeoutConfig::builder()
                .operation_attempt_timeout(request_timeout)
                .build();

            let sdk_cfg = aws_config::defaults(BehaviorVersion::latest())
                .region(parsed.region.clone())
                .timeout_config(timeout)
                .retry_config(RetryConfig::disabled())
                .load()
                .await;

            // 1.x exposes endpoint override and addressing style directly on
            // the S3-specific config builder (no more crafting an
            // aws_smithy_http::endpoint::Endpoint by hand).
            let mut s3_cfg_b =
                aws_sdk_s3::config::Builder::from(&sdk_cfg).force_path_style(force_path_style);
            if let Some(endpoint) = parsed.endpoint {
                s3_cfg_b = s3_cfg_b.endpoint_url(endpoint);
            }

            Ok(Bucket {
                client: SdkClient::from_conf(s3_cfg_b.build()),
                name: name.to_string(),
                region: parsed.region,
            })
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn region(&self) -> &Region {
        &self.region
    }

    /// PUT an object with a content type.  Returns `(body, status)` (the
    /// body is empty here -- verneuil only reads `status` from this method).
    pub async fn put_object_with_content_type(
        &self,
        name: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16), BoxError> {
        match self
            .client
            .put_object()
            .bucket(&self.name)
            .key(name)
            .content_type(content_type)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
        {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => sdk_err_to_body_code(e),
        }
    }

    /// GET an object.  Returns `(body, status)`; on success, body is the
    /// object's bytes (the SDK has already buffered them via `body.collect`).
    pub async fn get_object(&self, name: &str) -> Result<(Vec<u8>, u16), BoxError> {
        match self
            .client
            .get_object()
            .bucket(&self.name)
            .key(name)
            .send()
            .await
        {
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

    /// HEAD the bucket -- used to check whether it exists before deciding
    /// whether to create it.  Replaces rust-s3's `bucket.location()`.
    pub async fn head_bucket(&self) -> Result<(Vec<u8>, u16), BoxError> {
        match self.client.head_bucket().bucket(&self.name).send().await {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => sdk_err_to_body_code(e),
        }
    }

    /// CreateBucket -- on-demand creation, the same intent as rust-s3's
    /// `Bucket::create[_with_path_style]`.  `force_path_style` was selected
    /// at construction; nothing extra to do here.
    pub async fn create_bucket(&self) -> Result<(Vec<u8>, u16), BoxError> {
        match self.client.create_bucket().bucket(&self.name).send().await {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => sdk_err_to_body_code(e),
        }
    }

    /// Copy an object onto itself with `MetadataDirective::Replace`.  This
    /// is the verneuil "touch" idiom -- nudges the object's LastModified
    /// without re-uploading bytes.  It replaces rust-s3's add_header dance
    /// (`x-amz-copy-source` + `x-amz-metadata-directive` + empty-body PUT)
    /// with a single CopyObject call.
    pub async fn copy_object_to_self(
        &self,
        name: &str,
        content_type: &str,
    ) -> Result<(Vec<u8>, u16), BoxError> {
        // copy_source = "{bucket}/{url-encoded-key}".  Match rust-s3's
        // encoding choice: NON_ALPHANUMERIC encodes slashes too, since
        // CopyObject's source header treats them as separators if left bare.
        let encoded =
            percent_encoding::utf8_percent_encode(name, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let copy_source = format!("{}/{}", self.name, encoded);

        match self
            .client
            .copy_object()
            .bucket(&self.name)
            .key(name)
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
