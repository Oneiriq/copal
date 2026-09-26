//! The engine as a second enforcement layer under the repositories.
//!
//! `Store::caller` opens a session the engine filters by the schema's
//! `PERMISSIONS`, so a request-path bug that drops or confuses a
//! tenant filter returns nothing instead of another tenant's rows.
//! `get_file_any` is the honest stand-in for that bug: it queries by
//! id with no tenant filter at all, and through a caller session it
//! still cannot cross tenants.

use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::json;
use sha2::Sha256;

use copal_core::{FileSpec, TenantId};
use copal_store::repo::file;
use copal_store::{Store, StoreConfig};

const KEY: &str = "caller-store-key";

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// A hand-rolled HS256 caller token; the server-side minter is
/// copal-server's, this only speaks the same claims.
fn token(tenant: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = json!({
        "iss": "caller-store-test",
        "iat": now,
        "exp": now + 3600,
        "ns": "copal_test",
        "db": "copal",
        "ac": "caller",
        "id": "api_key:ck1",
        "tn": tenant,
        "adm": false,
    });
    let header = b64(json!({ "alg": "HS256", "typ": "JWT" })
        .to_string()
        .as_bytes());
    let payload = b64(claims.to_string().as_bytes());
    let signing_input = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(signing_input.as_bytes());
    let signature = b64(&mac.finalize().into_bytes());
    format!("{signing_input}.{signature}")
}

fn spec(path: &str) -> FileSpec {
    FileSpec {
        path: path.to_owned(),
        content_type: "text/plain".to_owned(),
        access: copal_core::AccessLevel::Private,
        metadata: json!({}),
        idempotency_key: None,
    }
}

#[tokio::test]
async fn caller_sessions_put_the_engine_under_the_repos() {
    let store = Store::connect(StoreConfig::memory_with_engine_access(KEY))
        .await
        .expect("store connects and defines caller access");
    let acme = TenantId::parse("acme").unwrap();
    let rival = TenantId::parse("rival").unwrap();
    let ours = file::create_file(&store, &acme, &spec("ours.txt"), "tester")
        .await
        .unwrap()
        .record;
    let theirs = file::create_file(&store, &rival, &spec("theirs.txt"), "tester")
        .await
        .unwrap()
        .record;

    let caller = store.caller(&token("acme")).await.expect("session binds");

    // Own-tenant reads keep working through the caller session.
    assert!(file::get_file(&caller, &acme, &ours.id)
        .await
        .unwrap()
        .is_some());

    // The buggy-repo shape: no tenant filter at all. The service
    // session serves it; the caller session comes back empty.
    assert!(file::get_file_any(&store, &theirs.id)
        .await
        .unwrap()
        .is_some());
    assert!(file::get_file_any(&caller, &theirs.id)
        .await
        .unwrap()
        .is_none());

    // A confused filter (the WRONG tenant named outright) finds
    // nothing either, because the engine's clause still holds.
    assert!(file::get_file(&caller, &rival, &theirs.id)
        .await
        .unwrap()
        .is_none());

    // The caller session ends with its store; the service session
    // stays untouched.
    drop(caller);
    assert!(file::get_file(&store, &rival, &theirs.id)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn wrong_key_tokens_never_open_sessions() {
    let store = Store::connect(StoreConfig::memory_with_engine_access(KEY))
        .await
        .unwrap();
    let mut forged = token("acme");
    let dot = forged.rfind('.').unwrap();
    forged.truncate(dot + 1);
    forged.push_str(&b64(b"not-a-signature"));
    assert!(store.caller(&forged).await.is_err());
}

#[tokio::test]
async fn stores_without_the_key_have_no_access_method() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    assert!(store.caller(&token("acme")).await.is_err());
}
