use std::sync::Mutex;

use s3::bucket::Bucket;

use crate::result::Result;

type VecBucketBuilder = dyn FnOnce() -> Result<Vec<Bucket>> + Send + 'static;

/// A `LazyVecBucket` is a vector of `Bucket`s that is constructed
/// when first needed.
///
/// This avoids the potential I/O overhead of creating a credential
/// object when no S3 call will be made.
pub(crate) struct LazyVecBucket {
    builder: Mutex<Option<Box<VecBucketBuilder>>>,
    buckets: quinine::MonoBox<Result<Vec<Bucket>>>,
}

/// Workaround for the fact that rust-s3 doesn't redact credentials in debug
/// impls.
impl std::fmt::Debug for LazyVecBucket {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)] // Because we disregard Debug in dead code analysis.
        struct RedactedBucket<'a> {
            name: &'a str,
            region: &'a awsregion::Region,
        }

        match self.buckets.as_ref() {
            None => write!(fmt, "{:?}", Option::<Vec<Bucket>>::None),
            Some(Err(e)) => write!(fmt, "{:?}", Err::<(), std::io::Error>(e.to_io())),
            Some(Ok(buckets)) => {
                let redacted = buckets
                    .iter()
                    .map(|x| RedactedBucket {
                        name: x.name.as_str(),
                        region: &x.region,
                    })
                    .collect::<Vec<_>>();

                write!(fmt, "{:?}", redacted)
            }
        }
    }
}

impl LazyVecBucket {
    pub fn new_from_buckets(buckets: Vec<Bucket>) -> Self {
        LazyVecBucket {
            builder: Default::default(),
            buckets: Box::new(Ok(buckets)).into(),
        }
    }

    #[inline(never)]
    fn make_buckets(&self) -> std::io::Result<&[Bucket]> {
        let mut builder = self.builder.lock().expect("poisoned lock");

        // Only do something if we don't have buckets yet.
        if self.buckets.is_none() {
            let buckets = builder.take().expect("should have a buider")();
            let _ = self.buckets.store_value(buckets);
        }

        self.buckets()
    }

    pub fn buckets(&self) -> std::io::Result<&[Bucket]> {
        if let Some(cache) = self.buckets.as_ref() {
            match cache {
                Ok(buckets) => return Ok(buckets),
                Err(e) => return Err(e.to_io()),
            }
        }

        self.make_buckets()
    }
}

#[test]
fn test_buckets_no_credential() {
    let bucket = Bucket::new_public("test-bucket", awsregion::Region::UsEast1).unwrap();
    let lazy_vec = LazyVecBucket::new_from_buckets(vec![bucket]);

    println!("Buckets: {:?}", lazy_vec);
    let debug_output = format!("{:?}", lazy_vec);
    assert!(!debug_output.to_lowercase().contains("credential"));
    assert!(!debug_output.to_lowercase().contains("key"));
    assert!(!debug_output.to_lowercase().contains("token"));
}
