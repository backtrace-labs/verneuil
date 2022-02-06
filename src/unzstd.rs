use std::io::Error;
use std::io::ErrorKind;

/// A zstd frame starts with 0xFD2FB528 in 4 little-endian bytes.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

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
        use std::io::Read;

        let mut decoder = zstd::Decoder::new(&*payload)?;
        let mut decompressed = vec![0; decompressed_size_limit];
        match decoder.read(&mut decompressed) {
            Ok(n) if n < decompressed_size_limit => {
                decompressed.resize(n, 0u8);
                decompressed.shrink_to_fit();
                Ok(decompressed)
            }
            Ok(_) => Err(Error::new(
                ErrorKind::Other,
                "decoded zstd data >= decompressed_size_limit",
            )),
            Err(e) => Err(e),
        }
    }

    if payload.starts_with(&ZSTD_MAGIC) {
        Some(decompress(payload, decompressed_size_limit))
    } else {
        None
    }
}
