//! A caching, single-flight DNS resolver for verneuil's S3 clients.
//!
//! The SDK's stock HTTP connector resolves hostnames with a blocking
//! `getaddrinfo` per new connection.  Verneuil's replication traffic
//! talks to a handful of hostnames, over and over, from many rayon
//! workers at once -- and S3 closes idle connections after ~20s, so a
//! busy flush re-resolves the same names continually.  On EC2 that is
//! worse than wasteful: the VPC resolver shares the instance's
//! link-local packet allowance with IMDS and NTP, so under pressure
//! lookups are exactly what gets dropped, and each drop stalls a
//! worker for a full resolver timeout.  Measured during concurrent
//! whole-spool flushes on a production host: 31% of all off-CPU time
//! was threads parked inside `getaddrinfo`.
//!
//! One successful lookup per host per [`DNS_TTL`] per process is
//! plenty.  Concurrent lookups of the same host coalesce behind a
//! per-host flight lock, so a cold start issues one query, not one per
//! worker.  Failures are not cached: the flight lock already keeps a
//! failing host from stampeding, and the per-request error path is the
//! SDK's to retry or report.
//!
//! Two tradeoffs, named so nobody rediscovers them the hard way.  A
//! cached address is served for the full TTL with no eviction on
//! connect failure (the resolver never sees connect errors), so an
//! endpoint that moves mid-TTL is a brownout for up to a minute --
//! irrelevant for S3, which serves old addresses long past that, but
//! real for a custom endpoint (minio, an S3-interop gateway) behind
//! DNS failover.  And when the *resolver itself* is the outage, misses
//! for one host wait in line behind the flight lock, each paying the
//! full lookup timeout in turn rather than in parallel; that is the
//! packet-flood fix working as intended, and per-attempt timeouts
//! still bound each caller (a cancelled waiter leaves the queue
//! cleanly).
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use aws_smithy_http_client::tls;
use aws_smithy_http_client::Builder;
use aws_smithy_runtime_api::client::dns::DnsFuture;
use aws_smithy_runtime_api::client::dns::ResolveDns;
use aws_smithy_runtime_api::client::dns::ResolveDnsError;
use aws_smithy_runtime_api::client::http::SharedHttpClient;

/// How long a successful lookup is served from cache.  Short enough to
/// follow S3's DNS-based traffic steering eventually, long enough that
/// a flush burst resolves each host once.
const DNS_TTL: Duration = Duration::from_secs(60);

/// One cached lookup: when it was resolved, and what it resolved to.
type CacheEntry = (Instant, Arc<Vec<IpAddr>>);

/// `host -> (resolved_at, addresses)`.
static CACHE: Mutex<Option<HashMap<String, CacheEntry>>> = Mutex::new(None);

/// Per-host flight locks, so concurrent misses for one host coalesce
/// into a single `getaddrinfo`.  Hosts are few (bucket endpoints), so
/// entries are simply retained.
static IN_FLIGHT: Mutex<Option<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = Mutex::new(None);

fn cached(host: &str) -> Option<Arc<Vec<IpAddr>>> {
    let guard = CACHE.lock().expect("mutex should be valid");
    let (resolved_at, ips) = guard.as_ref()?.get(host)?;

    if resolved_at.elapsed() < DNS_TTL {
        Some(ips.clone())
    } else {
        None
    }
}

fn store(host: &str, ips: Arc<Vec<IpAddr>>) {
    CACHE
        .lock()
        .expect("mutex should be valid")
        .get_or_insert_with(HashMap::new)
        .insert(host.to_string(), (Instant::now(), ips));
}

fn flight_lock(host: &str) -> Arc<tokio::sync::Mutex<()>> {
    IN_FLIGHT
        .lock()
        .expect("mutex should be valid")
        .get_or_insert_with(HashMap::new)
        .entry(host.to_string())
        .or_default()
        .clone()
}

/// The lookup seam: a plain function pointer so tests can substitute a
/// stub for the real `getaddrinfo` (the only part of this module that
/// would otherwise exercise the host's resolver instead of the code).
type LookupFn = fn(&str) -> std::io::Result<Vec<IpAddr>>;

/// The same blocking `getaddrinfo` the stock resolver uses.
fn system_lookup(host: &str) -> std::io::Result<Vec<IpAddr>> {
    std::net::ToSocketAddrs::to_socket_addrs(&(host, 0u16))
        .map(|addrs| addrs.map(|addr| addr.ip()).collect())
}

/// The resolver itself; the cache is process-wide, so instances are
/// interchangeable apart from the injected lookup.
#[derive(Clone, Debug)]
struct CachingDns {
    lookup: LookupFn,
}

impl Default for CachingDns {
    fn default() -> Self {
        CachingDns {
            lookup: system_lookup,
        }
    }
}

impl ResolveDns for CachingDns {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move {
            if let Some(ips) = cached(name) {
                return Ok((*ips).clone());
            }

            let flight = flight_lock(name);
            let _flight = flight.lock().await;

            // Someone may have resolved it while we waited in line.
            if let Some(ips) = cached(name) {
                return Ok((*ips).clone());
            }

            // Off the async worker, and deduplicated.
            let host = name.to_string();
            let lookup = self.lookup;
            let started = Instant::now();
            let ips = tokio::task::spawn_blocking(move || lookup(&host))
                .await
                .map_err(ResolveDnsError::new)?
                .map_err(ResolveDnsError::new)?;

            // An empty-but-successful answer would otherwise be served
            // for a full TTL; let it fail per-request like any other
            // lookup failure instead.
            if ips.is_empty() {
                return Err(ResolveDnsError::new(std::io::Error::other(
                    "lookup returned no addresses",
                )));
            }

            tracing::debug!(%name, count = ips.len(), elapsed = ?started.elapsed(),
                            "resolved host");
            store(name, Arc::new(ips.clone()));
            Ok(ips)
        })
    }
}

/// Returns verneuil's process-wide HTTP client for the AWS SDK: the
/// stock connector, with [`CachingDns`] in place of a per-connection
/// `getaddrinfo`.
///
/// This mirrors, piece by piece, what the SDK's own
/// `default_https_client` builds under the current behavior version
/// (aws-smithy-runtime's `client/http.rs`) -- same TLS provider, same
/// per-request connector settings, same sleep impl, and crucially the
/// same `ProxyConfig::from_env()`: the convenience
/// `Builder::build_with_resolver` path quietly defaults the proxy to
/// *disabled*, which would strand any deployment that reaches S3
/// through `HTTPS_PROXY`.  (`build_with_connector_fn` is `doc(hidden)`
/// but is exactly what the SDK itself calls.)
///
/// Because this is a mirror of upstream code, it drifts silently:
/// re-diff it against `default_https_client` whenever
/// aws-smithy-runtime or aws-smithy-http-client is upgraded.
pub(crate) fn shared_http_client() -> SharedHttpClient {
    use aws_smithy_http_client::proxy::ProxyConfig;
    use aws_smithy_http_client::ConnectorBuilder;

    lazy_static::lazy_static! {
        static ref CLIENT: SharedHttpClient =
            Builder::new().build_with_connector_fn(|settings, components| {
                let mut conn_builder = ConnectorBuilder::default().tls_provider(
                    tls::Provider::Rustls(tls::rustls_provider::CryptoMode::AwsLc),
                );

                conn_builder.set_connector_settings(settings.cloned());
                if let Some(components) = components {
                    conn_builder.set_sleep_impl(components.sleep_impl());
                }
                conn_builder.set_proxy_config(Some(ProxyConfig::from_env()));

                conn_builder.build_with_resolver(CachingDns::default())
            });
    }

    CLIENT.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    static STUB_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    /// Slow enough that two concurrent misses demonstrably overlap,
    /// fast enough not to matter.
    fn stub_lookup(host: &str) -> std::io::Result<Vec<IpAddr>> {
        use std::sync::atomic::Ordering;

        STUB_CALLS.fetch_add(1, Ordering::SeqCst);
        if host.starts_with("fail") {
            return Err(std::io::Error::other("stub lookup failure"));
        }
        if host.starts_with("empty") {
            return Ok(Vec::new());
        }

        std::thread::sleep(Duration::from_millis(100));
        Ok(vec!["192.0.2.7".parse().unwrap()])
    }

    /// Drives `resolve_dns` itself -- not just the cache helpers --
    /// through the injected lookup seam: concurrent misses for one host
    /// must coalesce into a single lookup, the result must be the
    /// lookup's (non-empty) addresses, hits must not re-resolve, and a
    /// failed lookup must propagate without being cached.
    #[test]
    fn resolver_coalesces_caches_and_propagates() {
        use std::sync::atomic::Ordering;

        let resolve = |host: &'static str| {
            std::thread::spawn(move || {
                let dns = CachingDns {
                    lookup: stub_lookup,
                };
                crate::executor::block_on_with_executor(
                    || async move { dns.resolve_dns(host).await },
                )
            })
        };

        // Two concurrent misses: one lookup, both get its addresses.
        let before = STUB_CALLS.load(Ordering::SeqCst);
        let (a, b) = (resolve("seam.test"), resolve("seam.test"));
        let a = a.join().expect("no panic").expect("must resolve");
        let b = b.join().expect("no panic").expect("must resolve");
        let expected: Vec<IpAddr> = vec!["192.0.2.7".parse().unwrap()];
        assert_eq!(
            a, expected,
            "the resolver must return the lookup's addresses"
        );
        assert_eq!(b, expected);
        assert_eq!(
            STUB_CALLS.load(Ordering::SeqCst) - before,
            1,
            "concurrent misses for one host must coalesce into one lookup"
        );

        // A later hit serves the cache.
        let c = resolve("seam.test")
            .join()
            .expect("no panic")
            .expect("must resolve");
        assert_eq!(c, expected);
        assert_eq!(
            STUB_CALLS.load(Ordering::SeqCst) - before,
            1,
            "hit must not re-resolve"
        );

        // An expired entry re-resolves.
        CACHE
            .lock()
            .expect("mutex should be valid")
            .as_mut()
            .expect("populated above")
            .insert(
                "seam.test".to_string(),
                (
                    Instant::now() - DNS_TTL - Duration::from_secs(1),
                    Arc::new(expected.clone()),
                ),
            );
        resolve("seam.test")
            .join()
            .expect("no panic")
            .expect("must resolve");
        assert_eq!(
            STUB_CALLS.load(Ordering::SeqCst) - before,
            2,
            "expiry must re-resolve"
        );

        // Failures propagate and are not cached: a second attempt asks
        // the lookup again.
        resolve("fail.seam.test")
            .join()
            .expect("no panic")
            .expect_err("the lookup failure must propagate");
        resolve("fail.seam.test")
            .join()
            .expect("no panic")
            .expect_err("still failing");
        assert_eq!(
            STUB_CALLS.load(Ordering::SeqCst) - before,
            4,
            "failures must not be cached"
        );
        assert!(cached("fail.seam.test").is_none());

        // An empty-but-successful answer is an error too, and must not
        // be cached for the TTL.
        resolve("empty.seam.test")
            .join()
            .expect("no panic")
            .expect_err("an empty answer must not be served as success");
        assert!(cached("empty.seam.test").is_none());
    }

    /// The cache must serve what was stored, expire it after the TTL,
    /// and keep hosts separate.  (Resolution itself is `getaddrinfo`;
    /// testing that would test the host's resolver, not this module.)
    #[test]
    fn cache_round_trips_and_isolates_hosts() {
        let a: Arc<Vec<IpAddr>> = Arc::new(vec!["192.0.2.1".parse().unwrap()]);
        let b: Arc<Vec<IpAddr>> = Arc::new(vec!["192.0.2.2".parse().unwrap()]);

        store("a.test", a.clone());
        store("b.test", b.clone());
        assert_eq!(cached("a.test").as_deref(), Some(&*a));
        assert_eq!(cached("b.test").as_deref(), Some(&*b));
        assert!(cached("c.test").is_none());

        // Backdate the entry past the TTL: it must stop being served.
        CACHE
            .lock()
            .expect("mutex should be valid")
            .as_mut()
            .expect("populated above")
            .insert(
                "a.test".to_string(),
                (Instant::now() - DNS_TTL - Duration::from_secs(1), a),
            );
        assert!(cached("a.test").is_none());
        assert_eq!(cached("b.test").as_deref(), Some(&*b));
    }
}
