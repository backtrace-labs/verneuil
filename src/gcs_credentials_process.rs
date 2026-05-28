//! A [`google_cloud_auth::credentials::CredentialsProvider`] impl that
//! invokes an external shell command to source GCP OAuth credentials.
//!
//! This is the GCS-native counterpart to [`crate::credentials_process`]
//! (which sources AWS-style credentials for the aws-sdk-s3 backend).
//! Same external-command lifecycle (`command → JSON → parsed → cached
//! until expiry → re-invoked`), different output shape: the AWS variant
//! emits an access-key + secret used to sign requests with SigV4, this
//! one emits an OAuth bearer token attached to requests via the
//! `Authorization` header.
//!
//! See `https://github.com/salrashid123/gcp_process_credentials_go` for
//! the Go-side inspiration for this pattern.
//!
//! ## Output schema (version 1)
//!
//! The command must write a single JSON object to stdout:
//!
//! ```json
//! {
//!   "version": 1,
//!   "access_token": "ya29.a0AfH6S...",
//!   "token_type": "Bearer",
//!   "expiration": "2026-05-28T15:00:00Z"
//! }
//! ```
//!
//! Field names mirror the AWS credential_process spec's casing convention
//! (PascalCase there, snake_case here following Google's API style).
//! `token_type` defaults to "Bearer" when absent.  `expiration` is RFC
//! 3339 and bounds how long the provider caches the token before
//! re-invoking the command.

use std::time::{Duration, SystemTime};

use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use google_cloud_gax::error::CredentialsError;
use http::{Extensions, HeaderMap, HeaderValue};
use tokio::sync::Mutex;

/// Per the spec, version is currently 1; reject unknown versions rather
/// than guessing how a new version's output may differ.
const SUPPORTED_OUTPUT_VERSION: u32 = 1;

/// Refresh tokens this far before their declared expiry to avoid races
/// against the resource server clock and the in-flight request budget.
const REFRESH_BUFFER: Duration = Duration::from_secs(30);

/// Default token lifetime to assume when the command omits `expiration`.
/// One hour matches the typical Google OAuth access-token lifetime.
const DEFAULT_LIFETIME: Duration = Duration::from_secs(3600);

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
struct ProcessOutput {
    version: u32,
    access_token: String,
    #[serde(default = "default_token_type")]
    token_type: String,
    /// RFC 3339 timestamp; absent => we synthesise an expiry one hour
    /// from now so the provider still re-invokes the command periodically.
    expiration: Option<String>,
}

fn default_token_type() -> String {
    "Bearer".to_string()
}

#[derive(Debug, Clone)]
struct CachedToken {
    /// Ready-to-attach `Authorization` header value: "<token_type> <access_token>".
    bearer: HeaderValue,
    /// SystemTime at which the cached token is considered expired.
    expires_at: SystemTime,
}

#[derive(Debug)]
pub(crate) struct GcsCredentialsProcessProvider {
    command: String,
    cached: Mutex<Option<CachedToken>>,
}

impl GcsCredentialsProcessProvider {
    pub fn new(command: String) -> Self {
        Self {
            command,
            cached: Mutex::new(None),
        }
    }

    /// Run the external command, parse the JSON output, return a
    /// `CachedToken`.  No locking here -- callers manage the cache slot.
    async fn fetch_fresh(&self) -> Result<CachedToken, CredentialsError> {
        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .output()
            .await
            .map_err(|e| {
                CredentialsError::from_msg(
                    /*is_retryable=*/ true,
                    &format!("credentials_process failed to execute: {e}"),
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CredentialsError::from_msg(
                /*is_retryable=*/ true,
                &format!(
                    "credentials_process exited with {}: {stderr}",
                    output.status
                ),
            ));
        }

        let parsed: ProcessOutput = serde_json::from_slice(&output.stdout).map_err(|e| {
            CredentialsError::from_msg(
                /*is_retryable=*/ false,
                &format!("credentials_process: invalid JSON: {e}"),
            )
        })?;

        if parsed.version != SUPPORTED_OUTPUT_VERSION {
            return Err(CredentialsError::from_msg(
                /*is_retryable=*/ false,
                &format!(
                    "credentials_process: unsupported output version {} (expected {})",
                    parsed.version, SUPPORTED_OUTPUT_VERSION
                ),
            ));
        }

        // Format the `Authorization: <token_type> <access_token>` header
        // value once so per-request `headers()` calls are cheap.
        let header_str = format!("{} {}", parsed.token_type, parsed.access_token);
        let bearer = HeaderValue::from_str(&header_str).map_err(|e| {
            CredentialsError::from_msg(
                /*is_retryable=*/ false,
                &format!("credentials_process: invalid bearer header value: {e}"),
            )
        })?;

        let expires_at = match parsed.expiration.as_deref() {
            Some(rfc3339) => {
                let dt = chrono::DateTime::parse_from_rfc3339(rfc3339).map_err(|e| {
                    CredentialsError::from_msg(
                        /*is_retryable=*/ false,
                        &format!("credentials_process: invalid RFC3339 expiration: {e}"),
                    )
                })?;
                SystemTime::from(dt)
            }
            None => SystemTime::now() + DEFAULT_LIFETIME,
        };

        Ok(CachedToken { bearer, expires_at })
    }
}

impl CredentialsProvider for GcsCredentialsProcessProvider {
    async fn headers(
        &self,
        _extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        let now = SystemTime::now();

        // Fast path: see if the cached token is still fresh.  Lock held
        // briefly (no `.await` between acquire and drop) so concurrent
        // request paths don't serialize on it.
        {
            let guard = self.cached.lock().await;
            if let Some(cached) = guard.as_ref() {
                if cached.expires_at > now + REFRESH_BUFFER {
                    let mut headers = HeaderMap::new();
                    headers.insert(http::header::AUTHORIZATION, cached.bearer.clone());
                    return Ok(CacheableResource::New {
                        entity_tag: EntityTag::default(),
                        data: headers,
                    });
                }
            }
        }

        // Slow path: invoke the command without holding the cache lock so
        // we don't block other readers if they happen to want the cached
        // value while we're refreshing.  There's a small thundering-herd
        // window where multiple concurrent calls all see "expired" and
        // re-invoke; the script is cheap enough that we don't bother
        // serialising further.
        let fresh = self.fetch_fresh().await?;

        {
            let mut guard = self.cached.lock().await;
            *guard = Some(fresh.clone());
        }

        let mut headers = HeaderMap::new();
        headers.insert(http::header::AUTHORIZATION, fresh.bearer);
        Ok(CacheableResource::New {
            entity_tag: EntityTag::default(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        // We don't operate in custom universes; the SDK falls back to
        // the default ("googleapis.com") when this returns None.
        None
    }
}
