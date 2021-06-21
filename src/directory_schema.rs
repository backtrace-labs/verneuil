//! Replicated sqlite DBs are represented as protobuf "directory"
//! metadata that refer to content-addressed chunks by fingerprint.

/// A umash fingerprint.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(crate) struct Fprint {
    #[prost(fixed64, tag = "1")]
    pub major: u64,
    #[prost(fixed64, tag = "2")]
    pub minor: u64,
}

impl From<umash::Fingerprint> for Fprint {
    fn from(fp: umash::Fingerprint) -> Fprint {
        (&fp).into()
    }
}

impl From<&umash::Fingerprint> for Fprint {
    fn from(fp: &umash::Fingerprint) -> Fprint {
        Fprint {
            major: fp.hash[0],
            minor: fp.hash[1],
        }
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(crate) struct DirectoryV1 {
    // The fingerprint for the file's 100-byte sqlite header.  There
    // may be some rare collisions over long time periods (> 4 billion
    // transactions), or when the file is deleted and re-created, but
    // it's fast to compute.
    #[prost(message, tag = "1")]
    pub header_fprint: Option<Fprint>,

    // The fingerprint for the file's list of chunk fingerprints, as
    // little-endian (major, minor) bytes.  Collisions are
    // astronomically unlikely.
    #[prost(message, tag = "2")]
    pub contents_fprint: Option<Fprint>,

    // The fingerprints for each chunk as pairs of u64.  The first
    // chunk has fingerprint `chunks[0], chunks[1]`, the second
    // `chunks[2], chunks[3]`, etc.
    #[prost(fixed64, repeated, tag = "3")]
    pub chunks: Vec<u64>,

    // The total length of the file, in bytes.
    #[prost(uint64, tag = "4")]
    pub len: u64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(crate) struct Directory {
    #[prost(message, tag = "1")]
    pub v1: Option<DirectoryV1>,
}
