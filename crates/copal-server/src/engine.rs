//! Caller tokens for the engine's second enforcement layer.
//!
//! With `COPAL_ENGINE_ACCESS_KEY` set, the store defines a record
//! access method and this module mints the short-lived tokens that
//! authenticate against it. A caller session opened with such a token
//! is filtered by the schema's `PERMISSIONS`: rows outside the
//! token's tenant do not exist for it, and guarded columns come back
//! absent. The application layer keeps making the decisions callers
//! see; the engine layer exists so a request-path bug that drops a
//! tenant filter returns nothing instead of another tenant's rows.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use copal_core::TenantId;

/// Everything token minting needs, present only when the deployment
/// configured the access key.
#[derive(Clone)]
pub struct EngineAccess {
    pub key: String,
    pub namespace: String,
    pub database: String,
}

/// Derive the engine policy from the contract, so both enforcement
/// layers read one declaration set.
///
/// The derivation itself lives in janus beside the seven faces it
/// agrees with, and the default [`janus::ClaimVocabulary`] IS this
/// deployment's caller-token conventions - scopes ride as `sc`, the
/// admin claim as `adm`, the principal handle as `pr` - proven
/// byte-identical to the hand derivation this call replaced in
/// janus's own tests. A guard the contract names without an engine
/// clause still refuses the boot: shipping it would silently drop
/// the engine layer for that column while the application layer kept
/// enforcing, and the two layers exist to agree.
pub fn engine_policy() -> copal_core::Result<copal_store::schema::EnginePolicy> {
    let derived = janus::derive_policy(
        &crate::contract::contract(),
        &janus::ClaimVocabulary::default(),
    )
    .map_err(|e| copal_core::CopalError::Store(e.to_string()))?;
    let mut policy = copal_store::schema::EnginePolicy {
        field_guards: derived.field_guards,
        select_conjuncts: derived.select_conjuncts,
        delete_conjuncts: Vec::new(),
    };
    // Retention is enforceable policy the contract cannot declare
    // yet, so it is stated here explicitly rather than derived: a
    // caller session may delete a version row only when nothing
    // binds it. The service store bypasses this, and version pruning
    // runs there; the clause exists so a request-path bug that
    // reaches version deletion meets a second refusal.
    policy.delete_conjuncts.push((
        "file_version".to_owned(),
        "legal_hold != true AND (retain_until IS NONE OR retain_until < time::now())".to_owned(),
    ));
    Ok(policy)
}

/// Seconds a caller token stays valid. Sessions opened with it keep
/// their own engine-side duration; this bounds how long a stolen
/// token mints new ones.
const TOKEN_TTL_SECS: u64 = 300;

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Mint a caller token for one authenticated principal.
///
/// The `id` claim points into the `api_key` table, which makes the
/// engine bind a record session; the record does not need to exist
/// for verification, it names the caller for `$auth` and audit. The
/// admin claim mirrors the scope model, so engine field guards and
/// contract field guards read the same authority.
pub fn mint_caller_token(
    access: &EngineAccess,
    tenant: &TenantId,
    key_id: &str,
    scopes: &[String],
    principal: Option<&str>,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let claims = serde_json::json!({
        "iss": "copal",
        "iat": now,
        "exp": now + TOKEN_TTL_SECS,
        "ns": access.namespace,
        "db": access.database,
        "ac": "caller",
        "id": format!("api_key:⟨{key_id}⟩"),
        "tn": tenant.as_str(),
        "adm": scopes.iter().any(|s| s == "admin"),
        "sc": scopes,
        "pr": principal,
    });
    let header = b64(serde_json::json!({ "alg": "HS256", "typ": "JWT" })
        .to_string()
        .as_bytes());
    let payload = b64(claims.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha256>::new_from_slice(access.key.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(signing_input.as_bytes());
    let signature = b64(&mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_carry_the_governance_claims() {
        let access = EngineAccess {
            key: "k".into(),
            namespace: "ns".into(),
            database: "db".into(),
        };
        let tenant = TenantId::parse("acme").unwrap();
        let token = mint_caller_token(
            &access,
            &tenant,
            "01KEY",
            &["read".into(), "admin".into()],
            Some("alice"),
        );
        let payload = token.split('.').nth(1).unwrap();
        let claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claims["ac"], "caller");
        assert_eq!(claims["tn"], "acme");
        assert_eq!(claims["id"], "api_key:⟨01KEY⟩");
        assert_eq!(claims["adm"], true);
        assert_eq!(claims["ns"], "ns");
        assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());
    }
}
