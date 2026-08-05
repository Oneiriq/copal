//! The external key-custody seam.
//!
//! The blob master key decides whether stored bytes can be read at
//! all, and it lived in the process environment, where it sits in
//! shell history, in a compose file, in whatever inspects a running
//! container. A key that guards retention and WORM deserves the same
//! custody the rest of a deployment's secrets get.
//!
//! Copal implements no key manager. It asks one for the key at boot
//! and holds it in memory for the process lifetime, which keeps the
//! secret out of the environment and puts issuance, revocation, and
//! access logging where an operator already runs them.
//!
//! The contract is one request, so a fifty-line adapter in front of
//! Vault, AWS KMS, or a sealed file can serve it:
//!
//! ```text
//! GET {addr}/keys/{key_id}
//! Authorization: Bearer {token}       (when configured)
//! -> 200 {"current": "<64 hex>", "previous": "<64 hex>" | null}
//! ```
//!
//! Both keys arrive together because a rotation is a state, and an
//! operator reading one key at a time can see a half of it that never
//! existed. `previous` carries the retiring key while the re-seal
//! sweep drains, exactly as `COPAL_BLOB_ENCRYPTION_KEY_PREVIOUS` does.

use copal_core::CopalError;
use serde::Deserialize;

/// What custody answered: the key that seals, and the retiring one
/// that still opens.
#[derive(Debug, Clone, Deserialize)]
pub struct KeyMaterial {
    pub current: String,
    #[serde(default)]
    pub previous: Option<String>,
}

fn hex_key(label: &str, raw: &str) -> copal_core::Result<()> {
    let trimmed = raw.trim();
    if trimmed.len() != 64 || !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(CopalError::Store(format!(
            "key custody returned a {label} key that is not 64 hex characters",
        )));
    }
    Ok(())
}

/// Ask custody for the master key.
///
/// Every failure is an error and the caller refuses to boot on it. A
/// deployment configured for external custody that cannot reach it
/// must not fall back to reading the environment, and must not come
/// up unable to open its own content.
pub async fn fetch(
    addr: &str,
    key_id: &str,
    token: Option<&str>,
) -> copal_core::Result<KeyMaterial> {
    let url = format!("{}/keys/{key_id}", addr.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| CopalError::Store(format!("key custody client: {e}")))?;
    let mut request = client.get(&url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| CopalError::Store(format!("key custody at {url}: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        let detail: String = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(200)
            .collect();
        return Err(CopalError::Store(format!(
            "key custody answered {status}: {detail}",
        )));
    }
    let material: KeyMaterial = response
        .json()
        .await
        .map_err(|e| CopalError::Store(format!("key custody answer: {e}")))?;
    hex_key("current", &material.current)?;
    if let Some(previous) = &material.previous {
        hex_key("previous", previous)?;
    }
    Ok(KeyMaterial {
        current: material.current.trim().to_owned(),
        previous: material.previous.map(|key| key.trim().to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn material(current: &str, previous: Option<&str>) -> copal_core::Result<KeyMaterial> {
        hex_key("current", current)?;
        if let Some(previous) = previous {
            hex_key("previous", previous)?;
        }
        Ok(KeyMaterial {
            current: current.trim().to_owned(),
            previous: previous.map(|key| key.trim().to_owned()),
        })
    }

    /// A key that is not a key must not reach the cipher, where it
    /// would fail later and further from its cause.
    #[test]
    fn custody_must_answer_with_a_key() {
        let good = "a".repeat(64);
        assert!(material(&good, None).is_ok());
        assert!(
            material(&format!(" {good} "), None).is_ok(),
            "padding trims"
        );
        assert!(material(&good, Some(&"b".repeat(64))).is_ok());

        assert!(material("short", None).is_err());
        assert!(material(&"z".repeat(64), None).is_err(), "z is not hex");
        assert!(
            material(&good, Some("nonsense")).is_err(),
            "the retiring key counts too"
        );
    }
}
