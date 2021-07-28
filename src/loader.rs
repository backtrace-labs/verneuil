//! `Loader`s are responsible for fetching chunks.
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use tracing::instrument;
use umash::Fingerprint;

use crate::chain_error;
use crate::chain_warn;
use crate::directory_schema::hash_file_chunk;
use crate::fresh_error;
use crate::replication_target::ReplicationTarget;
use crate::result::Result;

/// How many times do we retry on transient load errors?
const LOAD_RETRY_LIMIT: i32 = 3;

/// Add up to this fraction of the base delay to our sleep duration
/// when backing off before retrying a failed load call.
const LOAD_RETRY_JITTER_FRAC: f64 = 1.0;

/// Initial back-off delay (+/- jitter).
const LOAD_RETRY_BASE_WAIT: Duration = Duration::from_millis(50);

/// Grow the back-off delay by `LOAD_RETRY_MULTIPLIER` after
/// consecutive failures.
const LOAD_RETRY_MULTIPLIER: f64 = 10.0;

#[derive(Eq, PartialEq, Debug)]
// `#[non_exhaustive]` is too weak: it's a no-op within the crate.
#[allow(clippy::manual_non_exhaustive)]
pub(crate) struct Chunk {
    pub fprint: Fingerprint,
    pub payload: Box<[u8]>,

    _use_constructor: (),
}

#[derive(Debug)]
pub(crate) struct Loader {
    local_caches: Vec<PathBuf>,
    remote_sources: Vec<Bucket>,
}

lazy_static::lazy_static! {
    static ref LIVE_CHUNKS: Mutex<HashMap<Fingerprint, Weak<Chunk>>> = Default::default();
}

impl Chunk {
    /// Returns a fresh chunk for `fprint`.
    ///
    /// Returns `Err` when the payload's fingerprint does not match.
    pub(crate) fn new(fprint: Fingerprint, payload: Vec<u8>) -> Result<Chunk> {
        // The contents of a content-addressed chunk must have the same
        // fingerprint as the chunk's name.
        #[cfg(feature = "verneuil_test_vfs")]
        assert_eq!(
            fprint,
            crate::directory_schema::fingerprint_file_chunk(&payload)
        );

        // In production, only check the first 64-bit half of the
        // fingerprint, for speed.
        let actual_hash = hash_file_chunk(&payload);

        if actual_hash != fprint.hash[0] {
            return Err(fresh_error!(
                "mismatching file contents",
                ?fprint,
                ?actual_hash,
                len = payload.len()
            ));
        }

        Ok(Chunk {
            fprint,
            payload: payload.into(),
            _use_constructor: (),
        })
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // Remove this chunk from the global chunk map.
        let mut map = LIVE_CHUNKS.lock().expect("mutex should be valid");

        // By the time this `Chunk` is dropped, the Arc's strong count
        // has already been decremented to 0.  If we can upgrade the
        // value, it must have been inserted for a different copy of
        // this `Chunk`.
        if let Some(weak) = map.remove(&self.fprint) {
            if let Some(strong) = weak.upgrade() {
                map.insert(strong.fprint, weak);
            }
        }
    }
}

impl Loader {
    /// Creates a fresh `Loader` that looks for hits first in `local`,
    /// and then in `remote`.  The `Loader` always check the sources
    /// in order: low-to-high index in `local`, and then similar in
    /// `remote`.
    #[instrument]
    pub(crate) fn new(local: Vec<PathBuf>, remote: &[ReplicationTarget]) -> Result<Loader> {
        let mut remote_sources = Vec::new();

        if !remote.is_empty() {
            let creds =
                Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;

            for target in remote {
                remote_sources.push(create_chunk_source(&target, &creds)?);
            }
        }

        Ok(Loader {
            local_caches: local,
            remote_sources,
        })
    }

    /// Attempts to return the bytes for chunk `fprint`.
    ///
    /// Returns Ok(None) if nothing was found.
    #[instrument(level = "debug")]
    pub(crate) fn fetch_chunk(&self, fprint: Fingerprint) -> Result<Option<Arc<Chunk>>> {
        let mut contents = fetch_from_cache(fprint);

        // Don't fast path in tests.
        #[cfg(not(feature = "verneuil_test_vfs"))]
        if contents.is_some() {
            return Ok(contents);
        }

        let mut update_contents = |new_contents: Vec<u8>| {
            // If we already know the chunk's contents, the new ones
            // must match.
            if let Some(_old) = &contents {
                #[cfg(feature = "verneuil_test_vfs")]
                assert_eq!(&*_old.payload, new_contents.as_slice());
                return Ok(());
            }

            let chunk = Arc::new(Chunk::new(fprint, new_contents)?);
            update_cache(&chunk);
            contents = Some(chunk);
            Ok(())
        };

        let name = crate::replication_buffer::fingerprint_chunk_name(&fprint);
        for path in &self.local_caches {
            if let Some(cached) = load_from_cache(&path, &name)? {
                update_contents(cached)?;
                #[cfg(not(feature = "verneuil_test_vfs"))]
                return Ok(contents);
            }
        }

        for source in &self.remote_sources {
            if let Some(remote) = load_from_source(&source, &name)? {
                update_contents(remote)?;
                #[cfg(not(feature = "verneuil_test_vfs"))]
                return Ok(contents);
            }
        }

        Ok(contents)
    }
}

fn update_cache(chunk: &Arc<Chunk>) {
    let key = chunk.fprint;
    let weak = Arc::downgrade(chunk);

    let mut map = LIVE_CHUNKS.lock().expect("mutex should be valid");
    map.insert(key, weak);
}

/// Returns a cached chunk for `key`, if we have one.
fn fetch_from_cache(key: Fingerprint) -> Option<Arc<Chunk>> {
    let map = LIVE_CHUNKS.lock().expect("mutex should be valid");

    map.get(&key)?.upgrade()
}

#[instrument(level = "debug")]
fn create_chunk_source(source: &ReplicationTarget, creds: &Credentials) -> Result<Bucket> {
    use ReplicationTarget::*;

    match source {
        S3(s3) => {
            let region = if let Some(endpoint) = &s3.endpoint {
                s3::Region::Custom {
                    region: s3.region.clone(),
                    endpoint: endpoint.clone(),
                }
            } else {
                s3.region
                    .parse()
                    .map_err(|e| chain_error!(e, "failed to parse S3 region", ?s3))?
            };

            let bucket_name = &s3.chunk_bucket;
            let mut bucket = Bucket::new(bucket_name, region, creds.clone())
                .map_err(|e| chain_error!(e, "failed to create chunks S3 bucket object", ?s3))?;

            if s3.domain_addressing {
                bucket.set_subdomain_style();
            } else {
                bucket.set_path_style();
            }

            Ok(bucket)
        }
    }
}

/// Attempts to load file `name` in `prefix`.
#[instrument(level = "trace")]
fn load_from_cache(prefix: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    let path = prefix.join(name);

    match std::fs::read(&path) {
        Ok(buf) => Ok(Some(buf)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(chain_error!(e, "failed to load from cache", ?path)),
    }
}

/// Attempts to load the blob `name` from `source`.
fn load_from_source(source: &Bucket, name: &str) -> Result<Option<Vec<u8>>> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    for i in 0..=LOAD_RETRY_LIMIT {
        match source.get_object(name) {
            Ok((payload, 200)) => return Ok(Some(payload)),
            Ok((_, 404)) => return Ok(None),
            Ok((body, code)) if code < 500 && code != 429 => {
                return Err(
                    chain_error!((body, code), "failed to fetch chunk", %source.name, %name),
                )
            }
            err => {
                if i == LOAD_RETRY_LIMIT {
                    return Err(
                        chain_warn!(err, "reached load retry limit", %source.name, %name, LOAD_RETRY_LIMIT),
                    );
                }

                let sleep = LOAD_RETRY_BASE_WAIT.mul_f64(LOAD_RETRY_MULTIPLIER.powi(i));
                let jitter_scale = rng.gen_range(1.0..1.0 + LOAD_RETRY_JITTER_FRAC);
                let backoff = sleep.mul_f64(jitter_scale);

                tracing::info!(
                    ?err,
                    ?backoff,
                    %source.name,
                    %name,
                    "backing off after a failed GET."
                );
                std::thread::sleep(backoff);
            }
        }
    }

    std::unreachable!()
}
