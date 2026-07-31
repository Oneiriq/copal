//! AWS Signature Version 4 verification for the S3 gateway.
//!
//! The gateway verifies what S3 tooling signs: the canonical request
//! is rebuilt from the incoming request, the string-to-sign from the
//! client's own credential scope, the signing key from the stored
//! secret, and the signatures compare in constant time. Any region is
//! accepted (the scope the client declared is the scope verified);
//! the clock-skew window on `x-amz-date` is fifteen minutes.

use axum::http::{HeaderMap, Method};
use hmac::{Hmac, Mac};
use sha2::{Digest as _, Sha256};

use copal_core::CopalError;

type HmacSha256 = Hmac<Sha256>;

/// The parsed pieces of an `Authorization: AWS4-HMAC-SHA256 ...` header.
#[derive(Debug, Clone)]
pub struct ParsedAuth {
    pub access_key_id: String,
    pub scope: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

/// Parse the SigV4 authorization header.
pub fn parse_authorization(headers: &HeaderMap) -> copal_core::Result<ParsedAuth> {
    let raw = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| CopalError::unauthorized("missing authorization"))?;
    let rest = raw
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(|| CopalError::unauthorized("unsupported authorization scheme"))?;

    let mut access_key_id = None;
    let mut scope = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(credential) = part.strip_prefix("Credential=") {
            let mut pieces = credential.splitn(2, '/');
            access_key_id = pieces.next().map(str::to_owned);
            scope = pieces.next().map(str::to_owned);
        } else if let Some(list) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(list.split(';').map(str::to_owned).collect());
        } else if let Some(sig) = part.strip_prefix("Signature=") {
            signature = Some(sig.to_owned());
        }
    }
    match (access_key_id, scope, signed_headers, signature) {
        (Some(access_key_id), Some(scope), Some(signed_headers), Some(signature)) => {
            Ok(ParsedAuth {
                access_key_id,
                scope,
                signed_headers,
                signature,
            })
        }
        _ => Err(CopalError::unauthorized("malformed authorization header")),
    }
}

/// Verify a request against the stored secret. `raw_path` is the
/// request path exactly as received (percent-encoding preserved) and
/// `raw_query` the query string without the leading question mark.
pub fn verify(
    auth: &ParsedAuth,
    secret: &str,
    method: &Method,
    raw_path: &str,
    raw_query: &str,
    headers: &HeaderMap,
) -> copal_core::Result<()> {
    let amz_date = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| CopalError::unauthorized("missing x-amz-date"))?;
    check_skew(amz_date)?;

    let payload_hash = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    let canonical_headers: String = auth
        .signed_headers
        .iter()
        .map(|name| {
            let value = headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .trim();
            format!("{name}:{value}\n")
        })
        .collect();

    let canonical_query = canonicalize_query(raw_query);
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        raw_path,
        canonical_query,
        canonical_headers,
        auth.signed_headers.join(";"),
        payload_hash,
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        auth.scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );

    let mut scope_parts = auth.scope.split('/');
    let date = scope_parts.next().unwrap_or_default();
    let region = scope_parts.next().unwrap_or_default();
    let service = scope_parts.next().unwrap_or_default();
    let key = derive_signing_key(secret, date, region, service);
    let computed = hex::encode(hmac(&key, string_to_sign.as_bytes()));

    if constant_time_eq(computed.as_bytes(), auth.signature.as_bytes()) {
        Ok(())
    } else {
        Err(CopalError::unauthorized("signature mismatch"))
    }
}

/// The SigV4 key derivation chain.
pub fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Sort query parameters by key (then value), keeping their encoding.
fn canonicalize_query(raw_query: &str) -> String {
    if raw_query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&str, &str)> = raw_query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').unwrap_or((p, "")))
        .collect();
    pairs.sort_unstable();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// `x-amz-date` (YYYYMMDDTHHMMSSZ) within fifteen minutes of now.
fn check_skew(amz_date: &str) -> copal_core::Result<()> {
    let parsed =
        parse_amz_date(amz_date).ok_or_else(|| CopalError::unauthorized("malformed x-amz-date"))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| CopalError::Store("clock before epoch".into()))?
        .as_secs() as i64;
    if (now - parsed).abs() > 15 * 60 {
        return Err(CopalError::unauthorized("request time too skewed"));
    }
    Ok(())
}

/// Parse YYYYMMDDTHHMMSSZ to a unix timestamp without a calendar crate.
fn parse_amz_date(raw: &str) -> Option<i64> {
    let bytes = raw.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| raw.get(range)?.parse::<i64>().ok();
    let year = digits(0..4)?;
    let month = digits(4..6)?;
    let day = digits(6..8)?;
    let hour = digits(9..11)?;
    let minute = digits(11..13)?;
    let second = digits(13..15)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Days since epoch via the civil-days algorithm.
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hour * 3_600 + minute * 60 + second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signing_key_matches_the_documented_aws_vector() {
        // The published AWS example: secret, 20150830, us-east-1, iam.
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9",
        );
    }

    #[test]
    fn amz_dates_parse_and_gate_skew() {
        // A fixed date parses to the expected epoch second.
        assert_eq!(parse_amz_date("20150830T123600Z"), Some(1_440_938_160));
        assert_eq!(parse_amz_date("19700101T000000Z"), Some(0));
        assert!(parse_amz_date("2015-08-30T12:36:00Z").is_none());
        // A decades-old date refuses.
        assert!(check_skew("20150830T123600Z").is_err());
    }

    #[test]
    fn query_canonicalization_sorts_pairs() {
        assert_eq!(
            canonicalize_query("b=2&a=1&list-type=2"),
            "a=1&b=2&list-type=2",
        );
        assert_eq!(canonicalize_query("flag&x=1"), "flag=&x=1");
        assert_eq!(canonicalize_query(""), "");
    }
}
