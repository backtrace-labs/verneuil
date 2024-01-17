//! `Loader`s are responsible for fetching chunks.
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use tracing::instrument;
use umash::Fingerprint;

use crate::chain_error;
use crate::chain_info;
use crate::chain_warn;
use crate::drop_result;
use crate::executor::block_on_with_executor;
use crate::fresh_error;
use crate::manifest_schema::fingerprint_file_chunk;
use crate::manifest_schema::hash_file_chunk;
use crate::replication_target::apply_cache_replication_targets;
use crate::replication_target::parse_s3_region_specification;
use crate::replication_target::ReplicationTarget;
use crate::replication_target::S3ReplicationTarget;
use crate::result::Result;
use crate::unzstd::try_to_unzstd;

/// Writers clean up stale manifests in `.tap` on startup, but could
/// miss some.  We also try to touch these manifest files fairly
/// regularly.  If a tapped manifest is much more than a day old, it
/// was most likely left behind accidentally.
const CACHED_MANIFEST_MAX_AGE: Duration = Duration::from_secs(48 * 3600);

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
///
/// Even a base chunk for a 1TB database only needs 256 MB (2**28
/// bytes) for its references to data chunks.
const DECODED_CHUNK_SIZE_LIMIT: usize = 3usize << 27;

#[derive(Debug)]
pub struct Chunk {
    fprint: Fingerprint,
    payload: memmap2::Mmap,
}

impl Chunk {
    /// Returns the fingerprint for this chunk's data.
    pub fn fprint(&self) -> Fingerprint {
        self.fprint
    }

    /// Returns the chunk's byte data.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Workaround for the fact that rust-s3 doesn't redact credentials in debug
/// impls.
#[allow(clippy::ptr_arg)]
fn redacted_bucket_fmt(buckets: &Vec<Bucket>, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    #[derive(Debug)]
    #[allow(dead_code)] // Because we disregard Debug in dead code analysis.
    struct RedactedBucket<'a> {
        name: &'a str,
        region: &'a awsregion::Region,
    }

    let redacted = buckets
        .iter()
        .map(|x| RedactedBucket {
            name: x.name.as_str(),
            region: &x.region,
        })
        .collect::<Vec<_>>();

    write!(fmt, "{:?}", redacted)
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub(crate) struct Loader {
    cache: kismet_cache::Cache,

    #[derivative(Debug(format_with = "redacted_bucket_fmt"))]
    remote_sources: Vec<Bucket>,

    // These chunks will be returned without involving any cache nor
    // `remote_sources`.
    known_chunks: HashMap<Fingerprint, Arc<Chunk>>,
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
        let payload =
            memmap2::MmapMut::map_anon(
                crate::tracker::SNAPSHOT_GRANULARITY as usize)
            .expect("failed to create anonymous mapping")
            .make_read_only()
            .expect("failed to make mapping read-only");

        let fprint = fingerprint_file_chunk(&payload);
        (fprint, Arc::new(Chunk::new(fprint, payload).expect("fprint must match")))
    };
}

/// Returns the fingerprint for the zero-filled chunk.  We have
/// special knowledge of this chunk and know its contents without
/// fetching it from remote storage.
#[inline]
pub(crate) fn zero_fingerprint() -> Fingerprint {
    ZERO_FILLED_CHUNK.0
}

/// Returns whether `fp` is "known" to the chunk loader: we don't need
/// to hit remote storage to figure out what data they refer to.
/// Changing the set of well-known fingerprints may affect
/// compatibility.
#[inline]
pub(crate) fn is_well_known_fingerprint(fp: Fingerprint) -> bool {
    fp == zero_fingerprint()
}

/// Returns a vector of all well-known chunks.
fn well_known_chunks() -> Vec<Arc<Chunk>> {
    vec![ZERO_FILLED_CHUNK.1.clone()]
}

impl Chunk {
    fn new(fprint: Fingerprint, payload: memmap2::Mmap) -> Result<Chunk> {
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
                len = payload.as_ref().len()
            ));
        }

        Ok(Chunk { fprint, payload })
    }

    /// Returns a cached chunk for `fprint`, if we have one.
    pub(crate) fn arc_from_cache(fprint: Fingerprint) -> Option<Arc<Chunk>> {
        fetch_from_cache(fprint)
    }

    /// Returns a chunk (potentially cached) from `payload`.
    pub(crate) fn arc_from_bytes(payload: &[u8]) -> Arc<Chunk> {
        let fprint = fingerprint_file_chunk(payload);

        if let Some(ret) = fetch_from_cache(fprint) {
            return ret;
        }

        let mut map = memmap2::MmapMut::map_anon(payload.len()).unwrap();

        map.as_mut().copy_from_slice(payload);
        let ret = Arc::new(
            Chunk::new(
                fingerprint_file_chunk(payload),
                map.make_read_only().unwrap(),
            )
            .expect("must build: fingerprint matches"),
        );

        update_cache(ret.clone());
        ret
    }

    /// Returns a fresh chunk for `fprint`.
    #[cfg(test)]
    pub(crate) fn new_from_bytes(payload: &[u8]) -> Chunk {
        let mut map = memmap2::MmapMut::map_anon(payload.len()).unwrap();

        map.as_mut().copy_from_slice(payload);
        Chunk::new(
            fingerprint_file_chunk(payload),
            map.make_read_only().unwrap(),
        )
        .expect("must build: fingerprint matches")
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
        use std::io::Read;
        use std::os::unix::fs::MetadataExt;

        let path = path.join(name);

        let mut file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(chain_error!(e, "failed to load from cache", ?path)),
        };

        let meta = file
            .metadata()
            .map_err(|e| chain_error!(e, "failed to stat file", ?path))?;
        let ctime_secs = meta.ctime();
        if ctime_secs < 0 {
            continue;
        }

        let ctime = match std::time::UNIX_EPOCH.checked_add(Duration::from_secs(ctime_secs as u64))
        {
            Some(ctime) => ctime,
            None => continue,
        };

        match std::time::SystemTime::now().duration_since(ctime) {
            Ok(age) if age <= CACHED_MANIFEST_MAX_AGE => {
                let mut dst = Vec::new();

                file.read_to_end(&mut dst)
                    .map_err(|e| chain_error!(e, "failed to read tapped manifest", ?path))?;
                return Ok(Some(dst));
            }
            _ => {}
        }
    }

    if !remote.is_empty() {
        let creds =
            Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;

        for source in remote {
            let bucket = create_source(source, &creds, |s3| &s3.manifest_bucket)?;
            if let Some(bucket) = bucket {
                if let Some(fetched) = load_from_source(&bucket, name)? {
                    return Ok(Some(fetched));
                }
            }
        }
    }

    Ok(None)
}

impl Loader {
    /// Creates a fresh `Loader` that looks for hits first in the
    /// cache built with `cache_builder`, and fill misses by fetching
    /// from `remote`.  The `Loader` always fetches from `remote`
    /// source in low-to-high index order.
    #[instrument(err)]
    pub(crate) fn new(
        mut cache_builder: kismet_cache::CacheBuilder,
        remote: &[ReplicationTarget],
    ) -> Result<Loader> {
        let mut remote_sources = Vec::new();

        // We only care about remote S3 sources.
        if remote.iter().any(|x| matches!(x, ReplicationTarget::S3(_))) {
            let creds =
                Credentials::default().map_err(|e| chain_error!(e, "failed to get credentials"))?;

            for source in remote {
                if let Some(bucket) = create_source(source, &creds, |s3| &s3.chunk_bucket)? {
                    remote_sources.push(bucket);
                }
            }
        }

        cache_builder = apply_cache_replication_targets(cache_builder, remote);

        // In tests, check every potential cache hits to make sure
        // they have the same value.
        if cfg!(feature = "test_vfs") {
            cache_builder.panicking_byte_equality_checker();
        }

        // All cache directories should be tagged with the instance
        // id; we don't have to worry about OS crashes.
        cache_builder.auto_sync(false);

        Ok(Loader {
            cache: cache_builder.build(),
            remote_sources,
            known_chunks: well_known_chunks()
                .into_iter()
                .map(|chunk| (chunk.fprint(), chunk))
                .collect(),
        })
    }

    /// Registers a new chunk as "known" to the `Loader`, unless there
    /// already was a known chunk with that fingerprint.
    ///
    /// Known chunks are returned directly and bypass any I/O.
    pub(crate) fn adjoin_known_chunk(&mut self, chunk: Arc<Chunk>) {
        self.known_chunks.entry(chunk.fprint()).or_insert(chunk);
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
        use std::io::Write;

        if let Some(chunk) = self.known_chunks.get(&fprint) {
            return Ok(Some(chunk.clone()));
        }

        let contents = fetch_from_cache(fprint);
        let name = crate::replication_buffer::fingerprint_chunk_name(&fprint);
        let key = kismet_cache::Key::new(&name, fprint.hash(), fprint.secondary());

        // Don't fast path in tests.
        if contents.is_some() && !cfg!(feature = "test_vfs") {
            // Signal that we're still interested in this file.
            drop_result!(self.cache.touch(key),
                         e => chain_info!(e, "failed to touch cached chunk", ?fprint));
            return Ok(contents);
        }

        let cache_hit = self.cache.ensure(key, |dst| {
            use std::io::Error;
            use std::io::ErrorKind;

            for source in &self.remote_sources {
                if let Some(remote) = load_from_source(source, &name)
                    .map_err(|_| Error::new(ErrorKind::Other, "failed to fetch remote chunk"))?
                {
                    let bytes = maybe_decompress(remote, fprint)
                        .map_err(|_| Error::new(ErrorKind::Other, "failed to decode chunk"))?;

                    dst.write_all(&bytes)?;
                    return Ok(());
                }
            }

            Err(Error::new(ErrorKind::NotFound, "chunk not found"))
        });

        let data_file = match cache_hit {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(chain_error!(e, "failed to fetch chunk", ?fprint)),
        };

        #[cfg(feature = "test_vfs")]
        if let Some(cached) = contents {
            use std::io::Read;

            let mut data_file = data_file;
            let mut data = Vec::new();
            data_file
                .read_to_end(&mut data)
                .map_err(|e| chain_error!(e, "failed to read chunk data", ?fprint))?;

            assert_eq!(&*cached.payload, &data);
            return Ok(Some(cached));
        }

        let map = unsafe { memmap2::Mmap::map(&data_file) }
            .map_err(|e| chain_error!(e, "failed to map chunk file", ?fprint))?;

        let chunk = Arc::new(Chunk::new(fprint, map)?);
        update_cache(chunk.clone());
        Ok(Some(chunk))
    }
}

/// Attempts to decompress `payload` if that makes it match `fprint`.
fn maybe_decompress(payload: Vec<u8>, fprint: Fingerprint) -> Result<Vec<u8>> {
    let hash_matches = |payload: &[u8]| -> bool {
        // Only check the first 64-bit half of the fingerprint, for
        // speed.
        hash_file_chunk(payload) == fprint.hash[0]
    };

    let compare_hash = |payload: &[u8]| -> Result<()> {
        let actual_hash = hash_file_chunk(payload);

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

    let decompression_result = if let Some(r) = try_to_unzstd(&payload, DECODED_CHUNK_SIZE_LIMIT) {
        r
    } else {
        return Ok(payload);
    };

    match decompression_result {
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
) -> Result<Option<Bucket>> {
    use ReplicationTarget::*;

    match source {
        S3(s3) => {
            let region = parse_s3_region_specification(&s3.region, s3.endpoint.as_deref());
            let bucket_name = bucket_extractor(s3);
            let mut bucket = Bucket::new(bucket_name, region, creds.clone())
                .map_err(|e| chain_error!(e, "failed to create chunks S3 bucket object", ?s3))?;

            if s3.domain_addressing {
                bucket.set_subdomain_style();
            } else {
                bucket.set_path_style();
            }

            bucket.set_request_timeout(Some(LOAD_REQUEST_TIMEOUT));
            Ok(Some(bucket))
        }
        ReadOnly(_) | Local(_) => Ok(None),
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
    // Run `load_from_source_impl` in our private (rayon) thread pool
    // to avoid clashes between `impl`'s use of our internal runtime
    // and any tokio runtime in the caller's context...
    //
    // unless we're already running on that thread pool.
    if LOADER_POOL.current_thread_index().is_some() {
        load_from_source_impl(source, name)
    } else {
        LOADER_POOL.install(|| load_from_source_impl(source, name))
    }
}

fn load_from_source_impl(source: &Bucket, name: &str) -> Result<Option<Vec<u8>>> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    for i in 0..=LOAD_RETRY_LIMIT {
        match call_with_slow_logging(
            LOAD_REQUEST_TIMEOUT,
            || block_on_with_executor(|| source.get_object(name)),
            |duration| tracing::info!(?duration, ?name, "slow S3 GET"),
        ) {
            Ok((payload, 200)) => return Ok(Some(payload)),
            // All callers propagate 404s as hard failures.  We might
            // as well try a little bit harder if we get a 404.
            Ok((_, 404)) if i > 0 => return Ok(None),
            Ok((body, code)) if code < 500 && ![404, 429].contains(&code) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loader_no_credential() {
        let mut loader = Loader {
            cache: kismet_cache::CacheBuilder::new().build(),
            remote_sources: Vec::new(),
            known_chunks: HashMap::new(),
        };

        loader
            .remote_sources
            .push(Bucket::new_public("test-bucket", awsregion::Region::UsEast1).unwrap());

        println!("Loader: {:?}", loader);
        let debug_output = format!("{:?}", loader);
        assert!(!debug_output.to_lowercase().contains("credential"));
        assert!(!debug_output.to_lowercase().contains("key"));
        assert!(!debug_output.to_lowercase().contains("token"));
    }
}
