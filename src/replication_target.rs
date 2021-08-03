//! A replication target tells Verneuil where to copy the
//! content-addressed chunks and directory files it finds
//! in "ready" directories.
//!
//! Targets only describe location, not credentials: they are
//! persisted on disk in globally-readable files!
//!
//! We serialize to json because the data is small and short-lived,
//! so schema evolution isn't an important concern.  We also only
//! expect our own Rust code to deserialize the JSON we write, so
//! we can use all the flexibility offered by serde_json.
use serde::Deserialize;
use serde::Serialize;
use std::sync::RwLock;

/// A S3 replication target sends content-addressed chunks to one
/// S3-compatible bucket, and named directory blobs to another.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct S3ReplicationTarget {
    /// Region for the blob store.  Either one of the hardcoded strings
    /// supported by Rust-S3 (https://github.com/durch/rust-s3/blob/0.26.3/aws-region/src/region.rs#L132-L160),
    /// or a local domain like "minio".
    pub region: String,

    /// Endpoint override for custom regions, e.g.,
    /// "http://127.0.0.1:9000".  Targets with custom regions should
    /// specify an endpoint, but if they don't, the endpoint will
    /// default to the same string as the region.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Bucket name for content-addressed chunks.
    pub chunk_bucket: String,

    /// Bucket name for manifest blobs.
    pub manifest_bucket: String,

    /// If true, address buckets as subdomains (modern); otherwise,
    /// use the legacy bucket-as-path mode.
    pub domain_addressing: bool,

    /// If true, the buckets will be created with default "private"
    /// permissions if they don't already exist or disappear.
    #[serde(default)]
    pub create_buckets_on_demand: bool,
}

/// A replication target tells us where to replicate data, but not
/// with what credentials.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationTarget {
    S3(S3ReplicationTarget),
}

/// Verneuil will replicate content-addressed chunks and named
/// directory blobs to all the replication targets in the list.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicationTargetList {
    // Use a long and distinctive name for this field because, while
    // the list of replication targets is the only metadata we
    // currently track, that could change.
    pub replication_targets: Vec<ReplicationTarget>,
}

lazy_static::lazy_static! {
    static ref DEFAULT_REPLICATION_TARGETS: RwLock<Vec<ReplicationTarget>> = Default::default();
}

/// Sets the global default list of replication targets.
pub(crate) fn set_default_replication_targets(targets: Vec<ReplicationTarget>) {
    *DEFAULT_REPLICATION_TARGETS.write().unwrap() = targets;
}

/// Returns the global default list of replication targets.
pub(crate) fn get_default_replication_targets() -> ReplicationTargetList {
    ReplicationTargetList {
        replication_targets: DEFAULT_REPLICATION_TARGETS.read().unwrap().clone(),
    }
}

#[test]
fn test_serialization_smoke_test() {
    let targets = ReplicationTargetList {
        replication_targets: vec![ReplicationTarget::S3(S3ReplicationTarget {
            region: "minio".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            chunk_bucket: "chunks".into(),
            directory_bucket: "directories".into(),
            domain_addressing: true,
            create_buckets_on_demand: false,
        })],
    };

    let expected = "{\"replication_targets\":[{\"s3\":{\"region\":\"minio\",\"endpoint\":\"http://127.0.0.1:9000\",\"chunk_bucket\":\"chunks\",\"directory_bucket\":\"directories\",\"domain_addressing\":true,\"create_buckets_on_demand\":false}}]}";

    assert_eq!(
        serde_json::to_string(&targets).expect("should serialize"),
        expected
    );

    assert_eq!(
        serde_json::from_str::<ReplicationTargetList>(expected).expect("should deserialize"),
        targets
    );
}
