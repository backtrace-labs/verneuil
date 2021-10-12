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
use std::path::PathBuf;
use std::sync::RwLock;

use kismet_cache::CacheBuilder;
use serde::Deserialize;
use serde::Serialize;

use crate::instance_id::instance_id;

/// A S3 replication target sends content-addressed chunks to one
/// S3-compatible bucket, and named directory blobs to another.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
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

fn return_true() -> bool {
    true
}

/// A read-only cache replication target loads content-addressed
/// chunks from a directory of cached files.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct ReadOnlyCacheReplicationTarget {
    /// The root directory for the cache.
    pub directory: String,

    /// The number of shards in the cache (0 or 1 for a plain
    /// directory of files).
    #[serde(default)]
    pub num_shards: u32,

    /// Whether to append the instance id to the `directory` path;
    /// when `true` (the default), the path generation logic matches
    /// that of read-write `LocalReplicationTarget`s.
    #[serde(default = "return_true")]
    pub append_instance_id: bool,
}

/// A local replication target loads and stores content-addressed
/// chunks in a directory of cached files.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct LocalReplicationTarget {
    /// The root directory for the cache; the instance id will be
    /// appended to this path.
    pub directory: String,

    /// The number of shards in the cache (0 or 1 for a plain
    /// directory of files).
    pub num_shards: u32,

    /// The total number of files approximately allowed in the cache,
    /// across all shards.
    pub capacity: u64,
}

/// A replication target tells us where to find or replicate data, but
/// not with what credentials.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationTarget {
    S3(S3ReplicationTarget),
    ReadOnly(ReadOnlyCacheReplicationTarget),
    Local(LocalReplicationTarget),
}

/// Verneuil will replicate content-addressed chunks and named
/// directory blobs to all the replication targets in the list.
///
/// If there are multiple `Local` targets, an the first one is used
/// for writes, and the rest for reads.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
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

/// Updates `builder` with all the cache specs in `targets`.
pub(crate) fn apply_cache_replication_targets(
    mut builder: CacheBuilder,
    targets: &[ReplicationTarget],
) -> CacheBuilder {
    use ReplicationTarget::*;

    let mut has_local = false;

    for target in targets {
        match target {
            ReadOnly(ro) => {
                let mut target: PathBuf = ro.directory.clone().into();

                if ro.append_instance_id {
                    target.push(instance_id());
                };

                builder.reader(&target, ro.num_shards as usize);
            }
            Local(rw) => {
                let mut target: PathBuf = rw.directory.clone().into();
                target.push(instance_id());

                let num_shards = rw.num_shards as usize;
                let capacity = rw.capacity.clamp(0, usize::MAX as u64) as usize;

                if has_local {
                    builder.reader(&target, num_shards);
                } else {
                    builder.writer(&target, num_shards, capacity);
                }

                has_local = true;
            }
            S3(_) => {}
        }
    }

    builder
}

/// Parses a S3-compatible region specification from a region and an
/// optional endpoint.
///
/// If the endpoint is provided, we use that region name and endpoint
/// verbatim.
///
/// Otherwise, we use the region if it's known to our S3 crate (e.g.,
/// standard AWS or yandex regions).
///
/// Finally, if we only have a non-standard region, we try to parse it
/// as a custom region / HTTPS endpoint pair, of the form
/// `region-name.domain.for.https.endpoint`; if there is no dot in the
/// "region" name, we assume the endpoint matches the region name
/// (e.g., when deployed to a local undotted domain entry).
pub(crate) fn parse_s3_region_specification(region: &str, endpoint: Option<&str>) -> s3::Region {
    if let Some(endpoint) = endpoint {
        return s3::Region::Custom {
            region: region.to_owned(),
            endpoint: endpoint.to_owned(),
        };
    }

    match region.parse() {
        Ok(region) if !matches!(region, s3::Region::Custom { .. }) => region,
        _ => {
            let (region_name, endpoint) = match region.split_once(".") {
                Some(pair) => pair,
                None => (region, region),
            };

            let endpoint = format!("https://{}", endpoint);
            tracing::debug!(string=%region, region=%region_name, %endpoint,
                            "unknown S3 region; assuming it is a custom `region[.endpoint]`.");
            s3::Region::Custom {
                region: region_name.to_owned(),
                endpoint,
            }
        }
    }
}

#[test]
fn test_serialization_smoke_test() {
    let targets = ReplicationTargetList {
        replication_targets: vec![ReplicationTarget::S3(S3ReplicationTarget {
            region: "minio".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            chunk_bucket: "chunks".into(),
            manifest_bucket: "manifests".into(),
            domain_addressing: true,
            create_buckets_on_demand: false,
        })],
    };

    let expected = "{\"replication_targets\":[{\"s3\":{\"region\":\"minio\",\"endpoint\":\"http://127.0.0.1:9000\",\"chunk_bucket\":\"chunks\",\"manifest_bucket\":\"manifests\",\"domain_addressing\":true,\"create_buckets_on_demand\":false}}]}";

    assert_eq!(
        serde_json::to_string(&targets).expect("should serialize"),
        expected
    );

    assert_eq!(
        serde_json::from_str::<ReplicationTargetList>(expected).expect("should deserialize"),
        targets
    );
}
