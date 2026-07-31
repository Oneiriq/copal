//! Content digests.
//!
//! A blob's identity is the SHA-256 of its bytes. The digest doubles as
//! the blob record key and as the content-addressed storage path, so it
//! is validated strictly: exactly 64 lowercase hex characters.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::error::CopalError;

/// A validated lowercase-hex SHA-256 digest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentDigest(String);

impl ContentDigest {
    /// Wrap a digest string, validating shape.
    pub fn parse(value: impl Into<String>) -> crate::Result<Self> {
        let value = value.into();
        if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(CopalError::validation(
                "digest must be 64 hex characters (sha256)",
            ));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    /// Digest a complete in-memory buffer.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self(hex::encode(hasher.finalize()))
    }

    /// View as `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The content-addressed storage suffix: `ab/cd/<full digest>`.
    ///
    /// Two levels of fan-out keep directory listings bounded on
    /// filesystem backends and prefix-distributed on object stores.
    pub fn storage_key(&self) -> String {
        format!("{}/{}/{}", &self.0[..2], &self.0[2..4], self.0)
    }
}

impl std::fmt::Display for ContentDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Incremental digest for streamed uploads: feed chunks as they arrive,
/// finish once, never buffer the payload.
#[derive(Default)]
pub struct DigestBuilder {
    hasher: Sha256,
    bytes_seen: u64,
}

impl DigestBuilder {
    /// Start a fresh digest.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one chunk.
    pub fn update(&mut self, chunk: &[u8]) {
        self.hasher.update(chunk);
        self.bytes_seen += chunk.len() as u64;
    }

    /// Total bytes fed so far.
    pub fn bytes_seen(&self) -> u64 {
        self.bytes_seen
    }

    /// Finish, returning the digest and total size.
    pub fn finish(self) -> (ContentDigest, u64) {
        (
            ContentDigest(hex::encode(self.hasher.finalize())),
            self.bytes_seen,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SHA-256 of the empty string — the canonical test vector.
    const EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn known_vector() {
        assert_eq!(ContentDigest::of_bytes(b"").as_str(), EMPTY);
    }

    #[test]
    fn streaming_matches_oneshot() {
        let mut b = DigestBuilder::new();
        b.update(b"hello ");
        b.update(b"world");
        let (streamed, size) = b.finish();
        assert_eq!(streamed, ContentDigest::of_bytes(b"hello world"));
        assert_eq!(size, 11);
    }

    #[test]
    fn storage_key_fans_out() {
        let d = ContentDigest::parse(EMPTY).unwrap();
        assert_eq!(d.storage_key(), format!("e3/b0/{EMPTY}"));
    }

    #[test]
    fn parse_rejects_bad_shapes() {
        assert!(ContentDigest::parse("abc").is_err());
        assert!(ContentDigest::parse("g".repeat(64)).is_err());
        // Uppercase input normalises rather than failing.
        let upper = EMPTY.to_ascii_uppercase();
        assert_eq!(ContentDigest::parse(upper).unwrap().as_str(), EMPTY);
    }
}
