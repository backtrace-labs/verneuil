use std::io::Error;
use std::io::ErrorKind;

/// A zstd frame starts with 0xFD2FB528 in 4 little-endian bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// How big to set the initial capacity by default.
///
/// We expect to mostly decompress chunks of this size.
fn bounded_vector_size_initial_capacity() -> usize {
    crate::tracker::DEFAULT_WRITE_SNAPSHOT_GRANULARITY
        .max(crate::tracker::write_snapshot_granularity()) as usize
}

/// A `Writer` that dumps bytes to `dst` and fails instead of writing
/// more than `max` bytes.
struct BoundedVectorSink {
    /// Initially populated with a destination vector; None when the
    /// `max` size would be exceeded.
    dst: Option<Vec<u8>>,

    /// Inclusive upper bound for valid input sizes.
    max: usize,
}

impl BoundedVectorSink {
    /// Returns a fresh `BoundedVectorSink` that will accept up to
    /// `max` bytes.
    fn new(max: usize) -> Self {
        Self {
            dst: Some(Vec::with_capacity(
                max.clamp(0, bounded_vector_size_initial_capacity()),
            )),
            max,
        }
    }

    /// Returns the contents written in this sink, or `None` if the
    /// `max` size was exceeded.
    fn take(self) -> Option<Vec<u8>> {
        match self.dst {
            None => None,
            Some(mut v) => {
                v.shrink_to_fit();
                Some(v)
            }
        }
    }
}

impl std::io::Write for BoundedVectorSink {
    fn write(&mut self, src: &[u8]) -> std::io::Result<usize> {
        match self.dst.as_mut() {
            None => Ok(0),
            Some(dst) if dst.len().saturating_add(src.len()) > self.max => {
                // That's too many, fuse to failed mode.
                self.dst = None;
                Ok(0)
            }
            Some(dst) => {
                dst.extend(src);
                Ok(src.len())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Attempts to decompress `payload` bytes that may or may not be
/// zstd-compressed.
///
/// Returns `None` if the payload is definitely not zstd-compressed,
/// and `Some(Result)` with the outcome of compression if the payload
/// bytes look like zstd.
pub(crate) fn try_to_unzstd(
    payload: &[u8],
    decompressed_size_limit: usize,
) -> Option<std::io::Result<Vec<u8>>> {
    fn decompress(payload: &[u8], decompressed_size_limit: usize) -> std::io::Result<Vec<u8>> {
        let mut decoder = zstd::Decoder::new(payload)?;
        let mut sink = BoundedVectorSink::new(decompressed_size_limit);

        std::io::copy(&mut decoder, &mut sink)?;
        match sink.take() {
            None => Err(Error::new(
                ErrorKind::Other,
                "decoded zstd data > decompressed_size_limit",
            )),
            Some(decompressed) => Ok(decompressed),
        }
    }

    if payload.starts_with(&ZSTD_MAGIC) {
        Some(decompress(payload, decompressed_size_limit))
    } else {
        None
    }
}
