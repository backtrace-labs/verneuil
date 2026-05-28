//! Keyless native-GCS backend for [`crate::aws_bucket::Bucket`].
//!
//! Authenticates to Google Cloud Storage via the host's Application
//! Default Credentials (Workload Identity / attached service account) --
//! no HMAC secret on disk.  Reaches GCS through Google's official Rust
//! SDK ([`google_cloud_storage`], v1.x), so we can call the JSON API
//! directly for the verneuil operations.  Selected by
//! [`crate::replication_target::ReplicationTarget::Gcs`].
//!
//! Held inside the dispatcher [`crate::aws_bucket::Bucket`] as one of the
//! `BackendInner` variants.  Methods take the bucket *name* as a parameter
//! (the wrapper passes its own `name` field) so this struct itself isn't
//! bucket-bound; that mirrors how the AWS-SDK client is also bucket-agnostic.

use bytes::Bytes;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::{Bucket as GcsBucketResource, Object};
use google_cloud_wkt::FieldMask;

use crate::aws_bucket::BoxError;
use crate::gcs_credentials_process::GcsCredentialsProcessProvider;

/// GCS v2 API uses `projects/_/buckets/<name>` resource names; `_` means
/// "use whatever project owns the bucket."
fn bucket_resource_name(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

/// Holds the data-plane and control-plane GCS clients.  Both lazy-resolve
/// ADC credentials internally on first use.
pub(crate) struct GcsInner {
    storage: Storage,
    control: StorageControl,
    /// GCP project id (bare) and GCS location (e.g. `us-west4`) used to create
    /// the bucket on demand.  Both optional: native-GCS reads/writes address the
    /// bucket by name and need neither -- only [`Self::create_bucket`] does.
    /// When either is absent, `create_bucket` refuses (with a warning-shaped
    /// response) instead of creating something in the wrong place.
    project_id: Option<String>,
    location: Option<String>,
}

impl GcsInner {
    /// Construct both clients (data plane and control plane).
    ///
    /// When `credentials_process` is `None`, both clients use Application
    /// Default Credentials -- the keyless-GCS analogue of [`Bucket::new`]'s
    /// `aws_config::defaults(...).load()` path.  When `Some(cmd)`, the
    /// external command sources OAuth tokens for both clients per
    /// [`GcsCredentialsProcessProvider`].
    ///
    /// Sync (block-on internally) to match the S3 variant's construction
    /// shape -- both backends present a sync `new` so verneuil's existing
    /// sync construction sites don't need restructuring.
    pub fn new(
        project_id: Option<String>,
        location: Option<String>,
        credentials_process: Option<String>,
    ) -> Result<Self, BoxError> {
        crate::executor::block_on_with_executor(|| async move {
            // Build a fresh provider per client so each Storage /
            // StorageControl has its own internal token cache (the
            // command-output cache lives on the provider).  Cheap to
            // duplicate -- they don't talk to the network at construction
            // time, only on first auth header request.
            let storage_builder = Storage::builder();
            let control_builder = StorageControl::builder();

            let (storage_builder, control_builder) = match credentials_process {
                Some(cmd) => (
                    storage_builder
                        .with_credentials(GcsCredentialsProcessProvider::new(cmd.clone())),
                    control_builder.with_credentials(GcsCredentialsProcessProvider::new(cmd)),
                ),
                None => (storage_builder, control_builder),
            };

            let storage = storage_builder.build().await.map_err(|e| -> BoxError {
                Box::new(GcsError(format!("Storage::builder().build(): {e}")))
            })?;
            let control = control_builder.build().await.map_err(|e| -> BoxError {
                Box::new(GcsError(format!("StorageControl::builder().build(): {e}")))
            })?;
            Ok(GcsInner {
                storage,
                control,
                project_id,
                location,
            })
        })
    }

    /// PUT (`Storage::write_object`).  Returns `(body, status)` to match
    /// the rust-s3-shaped surface verneuil's retry loops match on.
    pub async fn put_object_with_content_type(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(Vec<u8>, u16), BoxError> {
        // `Storage::write_object` does a resumable upload via send_buffered.
        // For verneuil's chunks / manifests (small), the simple path is fine.
        let res = self
            .storage
            .write_object(
                bucket_resource_name(bucket),
                key.to_string(),
                Bytes::copy_from_slice(bytes),
            )
            .set_content_type(content_type.to_string())
            .send_buffered()
            .await;
        match res {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => Ok(gcs_err_to_body_code(&e)),
        }
    }

    /// GET (`Storage::read_object`).  Streams the object body and
    /// collects it; verneuil callers want the full bytes.
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, u16), BoxError> {
        // google-cloud-storage 1.x builds the ReadObject URL (data plane =
        // JSON/HTTP) as `/storage/v1/b/{bucket}/o/{enc(key)}`, and its `enc()`
        // percent-encoder escapes '/', ':', etc. but *not* '%' (see the crate's
        // storage/client.rs `ENCODED_CHARS`).  Verneuil object names are already
        // percent-encoded and contain literal "%3A"/"%2F", so GCS's URL layer
        // would decode those a *second* time and look up the wrong (decoded)
        // name -> 404.  Pre-escape '%' -> '%25' so the on-wire path decodes back
        // to our exact object name.  Only the read path needs this: write_object
        // passes the name as a query parameter (reqwest escapes '%'), and the
        // control-plane ops (update_object / get_bucket) are gRPC (name sent
        // verbatim).
        let key = key.replace('%', "%25");
        let mut resp = match self
            .storage
            .read_object(bucket_resource_name(bucket), key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return Ok(gcs_err_to_body_code(&e)),
        };
        let mut buf = Vec::new();
        while let Some(chunk) = resp.next().await {
            match chunk {
                Ok(bytes) => buf.extend_from_slice(&bytes),
                Err(e) => return Ok(gcs_err_to_body_code(&e)),
            }
        }
        Ok((buf, 200))
    }

    /// HEAD bucket -- `StorageControl::get_bucket` is the closest equivalent.
    pub async fn head_bucket(&self, bucket: &str) -> Result<(Vec<u8>, u16), BoxError> {
        match self
            .control
            .get_bucket()
            .set_name(bucket_resource_name(bucket))
            .send()
            .await
        {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => Ok(gcs_err_to_body_code(&e)),
        }
    }

    /// Create the bucket with default settings, if `project_id` + `location`
    /// were supplied on the target (GCS's create API requires both -- unlike
    /// reads/writes, which only need the bucket name).  When either is
    /// missing we can't create in the right place, so we return a
    /// warning-shaped `(body, 412)` that `ensure_bucket_exists` logs rather
    /// than actually creating something; ops must then pre-create the bucket.
    /// A 409 (already exists) from GCS is treated as success by the caller.
    pub async fn create_bucket(&self, bucket: &str) -> Result<(Vec<u8>, u16), BoxError> {
        let (project_id, location) = match (&self.project_id, &self.location) {
            (Some(p), Some(l)) => (p, l),
            _ => {
                return Ok((
                    format!(
                        "cannot create GCS bucket {bucket}: project_id and region/location \
                         must both be set on the target to create on demand \
                         (have project_id={}, region={})",
                        self.project_id.is_some(),
                        self.location.is_some(),
                    )
                    .into_bytes(),
                    412,
                ));
            }
        };

        // GCS v2 CreateBucket: parent = the owning project, bucket_id = the
        // globally-unique name, and a Bucket resource carrying the location.
        match self
            .control
            .create_bucket()
            .set_parent(format!("projects/{project_id}"))
            .set_bucket_id(bucket.to_string())
            .set_bucket(GcsBucketResource::new().set_location(location.clone()))
            .send()
            .await
        {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => Ok(gcs_err_to_body_code(&e)),
        }
    }

    /// "Touch" an object -- the verneuil idiom that bumps a manifest's
    /// LastModified so consumers see a fresh blob.  On the AWS path this
    /// is a CopyObject self-to-self with MetadataDirective::Replace; on
    /// GCS native we patch the object's `custom_time` field to "now",
    /// which updates the `updated` timestamp (the GCS analogue of S3's
    /// LastModified) without disturbing user metadata.  Single call, no
    /// byte copy.
    pub async fn copy_object_to_self(
        &self,
        bucket: &str,
        key: &str,
        _content_type: &str,
    ) -> Result<(Vec<u8>, u16), BoxError> {
        use google_cloud_wkt::Timestamp;

        // google-cloud-wkt 1.x Timestamp has no SystemTime conversion; build
        // it from the proto seconds/nanos directly.  Lower-bound at the
        // Unix epoch since SystemTime can in theory be before it.
        let now_dur = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let now = Timestamp::new(now_dur.as_secs() as i64, now_dur.subsec_nanos() as i32)
            .map_err(|e| -> BoxError { Box::new(GcsError(format!("Timestamp::new: {e}"))) })?;

        let object = Object::new()
            .set_bucket(bucket_resource_name(bucket))
            .set_name(key.to_string())
            .set_custom_time(now);

        match self
            .control
            .update_object()
            .set_object(object)
            .set_update_mask(FieldMask::default().set_paths(["custom_time"]))
            .send()
            .await
        {
            Ok(_) => Ok((Vec::new(), 200)),
            Err(e) => Ok(gcs_err_to_body_code(&e)),
        }
    }
}

/// Map a `google_cloud_storage::Error` into `(body, status)` so verneuil's
/// retry loops can classify it.  Translates the gax/HTTP code best-effort;
/// when we can't extract a code, fall back to 500 (which verneuil treats
/// as a retryable transient error).
fn gcs_err_to_body_code(err: &google_cloud_storage::Error) -> (Vec<u8>, u16) {
    // `http_status_code()` already returns `Option<u16>` in 1.x; fall back
    // to 500 when absent so verneuil's retry loop treats it as transient.
    let status = err.http_status_code().unwrap_or(500);
    (err.to_string().into_bytes(), status)
}

#[derive(Debug)]
struct GcsError(String);

impl std::fmt::Display for GcsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for GcsError {}
