//! Grant tokens for signed URLs.
//!
//! A token is `cg1.<grant id>.<secret>`: a stateful capability backed
//! by a row, with no signature scheme involved. The grant row holds `sha256(secret)`; the
//! secret itself is never stored, so a database leak does not leak
//! usable URLs. Because verification is a row lookup, revocation is an
//! UPDATE, use-counts are enforceable atomically, and, by design,
//! there is no signing key to distribute, rotate, or lose.
//!
//! `cg1` versions the format: a future stateless HMAC mode (for
//! CDN-edge verification without a database hop) arrives as `cg2`
//! beside it, with `cg1` staying.

use copal_core::CopalError;
use rand::Rng as _;
use sha2::{Digest as _, Sha256};

const PREFIX: &str = "cg1";
const SECRET_BYTES: usize = 32;

/// A minted or parsed grant token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantToken {
    /// The grant record id (ULID, lowercase).
    pub grant_id: String,
    /// The bearer secret, lowercase hex. Never stored server-side.
    pub secret: String,
}

impl GrantToken {
    /// Mint a fresh token with a cryptographically random secret.
    pub fn mint() -> Self {
        let raw: [u8; SECRET_BYTES] = rand::rng().random();
        Self {
            grant_id: ulid::Ulid::new().to_string().to_ascii_lowercase(),
            secret: hex::encode(raw),
        }
    }

    /// The wire form embedded in URLs.
    pub fn encode(&self) -> String {
        format!("{PREFIX}.{}.{}", self.grant_id, self.secret)
    }

    /// Parse a wire token. Rejections are uniform; callers surface
    /// every failure identically so the token format is not an oracle.
    pub fn parse(raw: &str) -> copal_core::Result<Self> {
        let (grant_id, secret) = parse_token(raw, PREFIX, "malformed grant token")?;
        Ok(Self { grant_id, secret })
    }

    /// `sha256(secret)` in lowercase hex, the only form the store sees.
    pub fn secret_hash(&self) -> String {
        hash_secret(&self.secret)
    }
}

const KEY_PREFIX: &str = "ck1";

/// A minted or parsed tenant API key. Same stateful-capability design
/// as grant tokens: `ck1.<key id>.<secret>`, the store holds only
/// `sha256(secret)`, revocation is an UPDATE, no signing key exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyToken {
    /// The api_key record id (ULID, lowercase).
    pub key_id: String,
    /// The bearer secret, lowercase hex. Never stored server-side.
    pub secret: String,
}

impl ApiKeyToken {
    /// Mint a fresh key with a cryptographically random secret.
    pub fn mint() -> Self {
        let raw: [u8; SECRET_BYTES] = rand::rng().random();
        Self {
            key_id: ulid::Ulid::new().to_string().to_ascii_lowercase(),
            secret: hex::encode(raw),
        }
    }

    /// The wire form callers present as a bearer credential.
    pub fn encode(&self) -> String {
        format!("{KEY_PREFIX}.{}.{}", self.key_id, self.secret)
    }

    /// Parse a wire key; rejections are uniform.
    pub fn parse(raw: &str) -> copal_core::Result<Self> {
        let (key_id, secret) = parse_token(raw, KEY_PREFIX, "malformed api key")?;
        Ok(Self { key_id, secret })
    }

    /// `sha256(secret)` in lowercase hex, the only form the store sees.
    pub fn secret_hash(&self) -> String {
        hash_secret(&self.secret)
    }
}

/// Shared `<prefix>.<id>.<hex secret>` parsing for both token families.
fn parse_token(
    raw: &str,
    expected_prefix: &str,
    fault: &'static str,
) -> copal_core::Result<(String, String)> {
    let mut parts = raw.split('.');
    let (Some(prefix), Some(id), Some(secret), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(CopalError::validation(fault));
    };
    if prefix != expected_prefix
        || id.is_empty()
        || secret.len() != SECRET_BYTES * 2
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        || !secret.chars().all(|c| c.is_ascii_hexdigit())
    {
        return Err(CopalError::validation(fault));
    }
    Ok((id.to_owned(), secret.to_ascii_lowercase()))
}

/// Hash a bearer secret for storage or comparison.
pub fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

/// Constant-time equality over the two hash strings.
///
/// The compared values are already digests, so variable-time equality
/// would leak little, but a capability check is exactly where "little"
/// should be "nothing".
pub fn verify_secret(presented_secret: &str, stored_hash: &str) -> bool {
    let presented = hash_secret(presented_secret);
    let a = presented.as_bytes();
    let b = stored_hash.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_encode_parse_round_trips() {
        let token = GrantToken::mint();
        let parsed = GrantToken::parse(&token.encode()).unwrap();
        assert_eq!(parsed, token);
        assert_eq!(parsed.secret_hash(), token.secret_hash());
    }

    #[test]
    fn mints_are_unique() {
        let a = GrantToken::mint();
        let b = GrantToken::mint();
        assert_ne!(a.grant_id, b.grant_id);
        assert_ne!(a.secret, b.secret);
    }

    #[test]
    fn parse_rejects_malformed_tokens() {
        for bad in [
            "",
            "cg1",
            "cg1.only-id",
            "cg2.id.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "cg1.id.short",
            "cg1.id.zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "cg1.bad id.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "cg1.id.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.extra",
        ] {
            assert!(GrantToken::parse(bad).is_err(), "{bad:?} must not parse");
        }
    }

    #[test]
    fn api_keys_round_trip_and_families_do_not_cross() {
        let key = ApiKeyToken::mint();
        let parsed = ApiKeyToken::parse(&key.encode()).unwrap();
        assert_eq!(parsed, key);
        // A grant token is not an api key and vice versa.
        assert!(ApiKeyToken::parse(&GrantToken::mint().encode()).is_err());
        assert!(GrantToken::parse(&key.encode()).is_err());
    }

    #[test]
    fn verification_accepts_the_secret_and_only_the_secret() {
        let token = GrantToken::mint();
        let stored = token.secret_hash();
        assert!(verify_secret(&token.secret, &stored));
        let mut wrong = token.secret.clone();
        // Flip one hex digit.
        let flipped = if wrong.ends_with('0') { '1' } else { '0' };
        wrong.pop();
        wrong.push(flipped);
        assert!(!verify_secret(&wrong, &stored));
        assert!(!verify_secret("", &stored));
    }
}
