//! A [`ProvideCredentials`] impl that invokes an external process per
//! the AWS [credential_process] JSON spec (Version 1).
//!
//! [credential_process]: https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html
//!
//! Wired into [`crate::aws_bucket::Bucket::new`] as an alternative to the
//! default credential provider chain.

use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{future, ProvideCredentials};
use aws_credential_types::Credentials;

/// Per the AWS credential_process spec, "Version" must currently be 1 and
/// may change as the output structure evolves; consumers should reject
/// unknown versions rather than guessing.
const SUPPORTED_OUTPUT_VERSION: u32 = 1;

/// JSON output schema produced by a credential_process command.
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ProcessOutput {
    version: u32,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    expiration: Option<String>,
}

/// A credentials provider that invokes an external command via `sh -c`
/// and parses its stdout per the AWS credential_process spec.
#[derive(Debug, Clone)]
pub(crate) struct CredentialsProcessProvider {
    command: String,
}

impl CredentialsProcessProvider {
    pub fn new(command: String) -> Self {
        Self { command }
    }
}

impl ProvideCredentials for CredentialsProcessProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        let command = self.command.clone();
        future::ProvideCredentials::new(async move {
            let output = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&command)
                .output()
                .await
                .map_err(|e| {
                    CredentialsError::provider_error(format!(
                        "credentials_process failed to execute: {e}"
                    ))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(CredentialsError::provider_error(format!(
                    "credentials_process exited with {}: {stderr}",
                    output.status
                )));
            }

            let parsed: ProcessOutput = serde_json::from_slice(&output.stdout).map_err(|e| {
                CredentialsError::provider_error(format!("credentials_process: invalid JSON: {e}"))
            })?;

            if parsed.version != SUPPORTED_OUTPUT_VERSION {
                return Err(CredentialsError::provider_error(format!(
                    "credentials_process: unsupported output version {} (expected {})",
                    parsed.version, SUPPORTED_OUTPUT_VERSION
                )));
            }

            // RFC3339 expiration -> SystemTime; if absent, the SDK treats
            // the credentials as non-expiring within its cache window.
            let expiry = parsed.expiration.as_deref().and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(std::time::SystemTime::from)
            });

            Ok(Credentials::new(
                parsed.access_key_id,
                parsed.secret_access_key,
                parsed.session_token,
                expiry,
                "credentials_process",
            ))
        })
    }
}
