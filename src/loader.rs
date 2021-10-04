//! `Loader`s are responsible for fetching chunks.
use s3::bucket::Bucket;
use s3::creds::Credentials;
use std::collections::HashMap;
use std::collections::HashSet;
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
use crate::fresh_error;
use crate::manifest_schema::fingerprint_file_chunk;
use crate::manifest_schema::hash_file_chunk;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::S3ReplicationTarget;
use crate::result::Result;

/// How long to wait for a request before giving up and trying again.
const LOAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Keep up to this many cached `Chunk`s alive, regardless of whether
/// there is any external strong reference to them.
const LRU_CACHE_SIZE: usize = 128;

/// Load chunks in a dedicated thread pool of this size.
const LOADER_POOL_SIZE: usize = 10;

/// Size limit for chunks after decompression.  We expect
/// all real chunks to be strictly shorter.
const DECODED_CHUNK_SIZE_LIMIT: usize = 1usize << 20;

/// A zstd frame starts with 0xFD2FB528 in 4 little-endian bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

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
    static ref LOADER_POOL: rayon::ThreadPool = rayon::ThreadPoolBuilder::new()
        .num_threads(LOADER_POOL_SIZE)
        .thread_name(|i| format!("verneuil-load-worker/{}", i))
        .build()
        .expect("failed to build global loader pool");
}

lazy_static::lazy_static! {
    static ref LIVE_CHUNKS: Mutex<HashMap<Fingerprint, Weak<Chunk>>> = Default::default();
}

lazy_static::lazy_static! {
    // This bounded LRU cache maintains strong references to hot
    // chunks, to avoid repeatedly dropping and re-fetching them.
    static ref CACHED_CHUNKS: Mutex<lru::LruCache<Fingerprint, Arc<Chunk>>> = Mutex::new(lru::LruCache::new(LRU_CACHE_SIZE));
}

lazy_static::lazy_static! {
    // Some database files are sparsely populated, and mostly consist
    // of zero-filled chunks.  Fast-path such chunks, and keep a copy
    // of the fingerprint outside the `Arc` to minimise indirection
    // when this optimisation does not trigger.
    static ref ZERO_FILLED_CHUNK: (Fingerprint, Arc<Chunk>) = {
        let payload = vec![0; crate::tracker::SNAPSHOT_GRANULARITY as usize];
        let fprint = fingerprint_file_chunk(&payload);
        (fprint, Arc::new(Chunk::new(fprint, payload).expect("fprint must match")))
    };
}

impl Chunk {
    /// Returns a fresh chunk for `fprint`.
    ///
    /// Returns `Err` when the payload's fingerprint does not match.
    pub(crate) fn new(fprint: Fingerprint, payload: Vec<u8>) -> Result<Chunk> {
        // The contents of a content-addressed chunk must have the same
        // fingerprint as the chunk's name.
        #[cfg(feature = "test_vfs")]
        assert_eq!(fprint, fingerprint_file_chunk(&payload));

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

/// Returns the bytes for manifest proto file `name`, after looking in
/// `local`, and then in `remote`'s manifest bucket.
pub(crate) fn fetch_manifest(
    name: &str,
    local: &[&Path],
    remote: &[ReplicationTarget],
) -> Result<Option<Vec<u8>>> {
    for path in local {
        if let Some(cached) = load_from_cache(path, name)? {
            return Ok(Some(cached));
        }
    }

    if !remote.is_empty() {
        let creds =
            Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;

        for source in remote {
            let bucket = create_source(source, &creds, |s3| &s3.manifest_bucket)?;
            if let Some(fetched) = load_from_source(&bucket, name)? {
                return Ok(Some(fetched));
            }
        }
    }

    Ok(None)
}

impl Loader {
    /// Creates a fresh `Loader` that looks for hits first in `local`,
    /// and then in `remote`.  The `Loader` always check the sources
    /// in order: low-to-high index in `local`, and then similarly in
    /// `remote`.
    #[instrument(err)]
    pub(crate) fn new(local: Vec<PathBuf>, remote: &[ReplicationTarget]) -> Result<Loader> {
        let mut remote_sources = Vec::new();

        if !remote.is_empty() {
            let creds =
                Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;

            for source in remote {
                remote_sources.push(create_source(&source, &creds, |s3| &s3.chunk_bucket)?);
            }
        }

        Ok(Loader {
            local_caches: local,
            remote_sources,
        })
    }

    /// Attempts to fetch the chunk for each fingerprint in `fprints`.
    ///
    /// Return `Err` if any fetch fails.  Even on success, the return
    /// value may be missing entries.
    pub(crate) fn fetch_all_chunks(
        &self,
        fprints: &[Fingerprint],
    ) -> Result<HashMap<Fingerprint, Arc<Chunk>>> {
        use rand::prelude::SliceRandom;

        // Deduplicate fprints before fetching them.
        let targets: HashSet<Fingerprint> = fprints.iter().cloned().collect();

        // Randomize the load order to avoid accidental hotspotting
        // when threads or processes load similar sets of chunks.
        let mut shuffled_targets: Vec<Fingerprint> = targets.into_iter().collect();
        shuffled_targets.shuffle(&mut rand::thread_rng());

        let chunks: Vec<Option<Arc<Chunk>>> = LOADER_POOL.install(move || {
            use rayon::prelude::*;

            shuffled_targets
                .into_par_iter()
                .map(|fprint| self.fetch_chunk(fprint))
                .collect::<Result<_>>()
        })?;

        Ok(chunks
            .into_iter()
            .filter_map(|chunk_or| chunk_or.map(|chunk| (chunk.fprint, chunk)))
            .collect())
    }

    /// Attempts to return the bytes for chunk `fprint`.
    ///
    /// Returns Ok(None) if nothing was found.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) fn fetch_chunk(&self, fprint: Fingerprint) -> Result<Option<Arc<Chunk>>> {
        if fprint == ZERO_FILLED_CHUNK.0 {
            return Ok(Some(ZERO_FILLED_CHUNK.1.clone()));
        }

        let mut contents = fetch_from_cache(fprint);

        // Don't fast path in tests.
        #[cfg(not(feature = "test_vfs"))]
        if contents.is_some() {
            return Ok(contents);
        }

        let mut update_contents = |new_contents: Vec<u8>| {
            // If we already know the chunk's contents, the new ones
            // must match.
            if let Some(_old) = &contents {
                #[cfg(feature = "test_vfs")]
                assert_eq!(&*_old.payload, new_contents.as_slice());
                return Ok(());
            }

            let chunk = Arc::new(Chunk::new(fprint, new_contents)?);
            update_cache(chunk.clone());
            contents = Some(chunk);
            Ok(())
        };

        let name = crate::replication_buffer::fingerprint_chunk_name(&fprint);
        for path in &self.local_caches {
            if let Some(cached) = load_from_cache(&path, &name)? {
                update_contents(cached)?;
                #[cfg(not(feature = "test_vfs"))]
                return Ok(contents);
            }
        }

        for source in &self.remote_sources {
            if let Some(remote) = load_from_source(&source, &name)? {
                update_contents(maybe_decompress(remote, fprint)?)?;
                #[cfg(not(feature = "test_vfs"))]
                return Ok(contents);
            }
        }

        Ok(contents)
    }
}

/// Attempts to decompress `payload` if that makes it match `fprint`.
fn maybe_decompress(payload: Vec<u8>, fprint: Fingerprint) -> Result<Vec<u8>> {
    let hash_matches = |payload: &[u8]| -> bool {
        // Only check the first 64-bit half of the fingerprint, for
        // speed.
        hash_file_chunk(&payload) == fprint.hash[0]
    };

    let compare_hash = |payload: &[u8]| -> Result<()> {
        let actual_hash = hash_file_chunk(&payload);

        if actual_hash == fprint.hash[0] {
            Ok(())
        } else {
            Err(fresh_error!(
                "mismatching file contents",
                ?fprint,
                ?actual_hash,
                len = payload.len()
            ))
        }
    };

    let decompress = |payload: &[u8]| -> std::io::Result<Vec<u8>> {
        use std::io::Read;

        let mut decoder = zstd::Decoder::new(&*payload)?;
        let mut decompressed = vec![0; DECODED_CHUNK_SIZE_LIMIT];

        match decoder.read(&mut decompressed) {
            Ok(n) if n < DECODED_CHUNK_SIZE_LIMIT => {
                decompressed.resize(n, 0u8);
                decompressed.shrink_to_fit();
                Ok(decompressed)
            }
            Ok(_) => Err(std::io::Error::new(
                ErrorKind::Other,
                "decoded zstd data >= DECODED_CHUNK_SIZE_LIMIT",
            )),
            Err(e) => Err(e),
        }
    };

    if !payload.starts_with(&ZSTD_MAGIC) {
        return Ok(payload);
    }

    match decompress(&payload) {
        Ok(decompressed) => {
            match compare_hash(&decompressed) {
                Ok(()) => Ok(decompressed),
                Err(e) => {
                    // Maybe the original data only accidentally
                    // looks like zstd-compressed bytes.  See
                    // if that matches.
                    if hash_matches(&payload) {
                        Ok(payload)
                    } else {
                        // We don't want the original data
                        // either.  Report that zstd
                        // corruption.
                        Err(chain_error!(
                            e,
                            "decoded zstd data does not match fingerprint",
                            ?fprint,
                            len = payload.len()
                        ))
                    }
                }
            }
        }
        Err(e) => {
            if hash_matches(&payload) {
                Ok(payload)
            } else {
                Err(chain_error!(
                    e,
                    "failed to decode zstd data",
                    ?fprint,
                    len = payload.len()
                ))
            }
        }
    }
}

fn touch_cached_chunk(chunk: Arc<Chunk>) {
    let key = chunk.fprint;

    let mut cache = CACHED_CHUNKS.lock().expect("mutex should be valid");
    cache.put(key, chunk);
}

fn update_cache(chunk: Arc<Chunk>) {
    let key = chunk.fprint;
    let weak = Arc::downgrade(&chunk);

    {
        let mut map = LIVE_CHUNKS.lock().expect("mutex should be valid");
        map.insert(key, weak);
    }

    touch_cached_chunk(chunk);
}

/// Returns a cached chunk for `key`, if we have one.
fn fetch_from_cache(key: Fingerprint) -> Option<Arc<Chunk>> {
    let upgraded = {
        let map = LIVE_CHUNKS.lock().expect("mutex should be valid");

        map.get(&key)?.upgrade()?
    };

    touch_cached_chunk(upgraded.clone());
    Some(upgraded)
}

#[instrument(level = "debug", skip(creds, bucket_extractor), err)]
fn create_source(
    source: &ReplicationTarget,
    creds: &Credentials,
    bucket_extractor: impl FnOnce(&S3ReplicationTarget) -> &str,
) -> Result<Bucket> {
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

            let bucket_name = bucket_extractor(&s3);
            let mut bucket = Bucket::new(bucket_name, region, creds.clone())
                .map_err(|e| chain_error!(e, "failed to create chunks S3 bucket object", ?s3))?;

            if s3.domain_addressing {
                bucket.set_subdomain_style();
            } else {
                bucket.set_path_style();
            }

            bucket.set_request_timeout(Some(LOAD_REQUEST_TIMEOUT));
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

/// Invokes `timed` and checks how long that call took.  If the time
/// exceeds the time limit, invokes `slow_logger` before returning as
/// usual.
fn call_with_slow_logging<T>(
    limit: Duration,
    timed: impl FnOnce() -> T,
    slow_logger: impl FnOnce(Duration),
) -> T {
    let start = std::time::Instant::now();
    let ret = timed();

    let elapsed = start.elapsed();
    if elapsed >= limit {
        slow_logger(elapsed);
    }

    ret
}

/// Attempts to load the blob `name` from `source`.
pub(crate) fn load_from_source(source: &Bucket, name: &str) -> Result<Option<Vec<u8>>> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    for i in 0..=LOAD_RETRY_LIMIT {
        match call_with_slow_logging(
            LOAD_REQUEST_TIMEOUT,
            || source.get_object_blocking(name),
            |duration| tracing::info!(?duration, ?name, "slow S3 GET"),
        ) {
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
                    "backing off after a failed GET"
                );
                std::thread::sleep(backoff);
            }
        }
    }

    std::unreachable!()
}
