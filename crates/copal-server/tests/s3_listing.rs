//! Object listings in both of S3's dialects: V1 resumes from a
//! `marker` key and names the next one in `NextMarker`, and V2 honors
//! `start-after` on a first page.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt as _;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_server::app::AppState;
use copal_server::auth::AuthConfig;
use copal_server::s3::{s3_admin_router, s3_router};
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

struct Client {
    gateway: axum::Router,
    access_key_id: String,
    secret: String,
    _dir: tempfile::TempDir,
}

async fn client() -> Client {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            admin_token: Some("root".to_owned()),
            ..AuthConfig::default()
        })
        .with_cipher(Some(BlobCipher::from_hex(MASTER_KEY).unwrap()));
    let mint = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/s3-credentials")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = s3_admin_router(state.clone()).oneshot(mint).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let credential: Value = serde_json::from_slice(&bytes).unwrap();
    Client {
        gateway: s3_router(state),
        access_key_id: credential["access_key_id"].as_str().unwrap().to_owned(),
        secret: credential["secret_access_key"].as_str().unwrap().to_owned(),
        _dir: dir,
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compact UTC timestamp (YYYYMMDDTHHMMSSZ) for `x-amz-date`.
fn amz_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year_base = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year_base + 1 } else { year_base };
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        tod / 3_600,
        tod % 3_600 / 60,
        tod % 60,
    )
}

impl Client {
    /// Send one signed request; the query must already be sorted.
    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
    ) -> (StatusCode, String) {
        let payload_hash = hex::encode(Sha256::digest(body));
        let amz_date = amz_now();
        let date = &amz_date[..8];
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{method}\n{path}\n{query}\nhost:localhost\nx-amz-content-sha256:{payload_hash}\n\
             x-amz-date:{amz_date}\n\n{signed_headers}\n{payload_hash}",
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes())),
        );
        let k_date = hmac_sha256(format!("AWS4{}", self.secret).as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, b"us-east-1");
        let k_service = hmac_sha256(&k_region, b"s3");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        let uri = if query.is_empty() {
            path.to_owned()
        } else {
            format!("{path}?{query}")
        };
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("host", "localhost")
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("content-length", body.len().to_string())
            .header(
                "authorization",
                format!(
                    "AWS4-HMAC-SHA256 Credential={}/{scope}, \
                     SignedHeaders={signed_headers}, Signature={signature}",
                    self.access_key_id,
                ),
            )
            .body(Body::from(body.to_vec()))
            .unwrap();
        let response = self.gateway.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn put_keys(&self, keys: &[&str]) {
        for key in keys {
            let (status, body) = self
                .send("PUT", &format!("/acme/{key}"), "", key.as_bytes())
                .await;
            assert_eq!(status, StatusCode::OK, "{key}: {body}");
        }
    }
}

fn tag<'a>(body: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(&body[start..end])
}

fn keys(body: &str) -> Vec<String> {
    body.split("<Key>")
        .skip(1)
        .filter_map(|chunk| chunk.split("</Key>").next())
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn v1_listings_resume_from_the_marker() {
    let client = client().await;
    client.put_keys(&["a.txt", "b.txt", "c.txt"]).await;

    let mut collected = Vec::new();
    let mut marker: Option<String> = None;
    for _ in 0..10 {
        let query = match &marker {
            Some(marker) => format!("marker={marker}&max-keys=1"),
            None => "max-keys=1".to_owned(),
        };
        let (status, body) = client.send("GET", "/acme", &query, b"").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            tag(&body, "Marker").unwrap_or_default(),
            marker.as_deref().unwrap_or_default(),
            "the marker echoes: {body}",
        );
        collected.extend(keys(&body));
        if tag(&body, "IsTruncated") != Some("true") {
            break;
        }
        marker = Some(
            tag(&body, "NextMarker")
                .expect("a truncated page names the next")
                .to_owned(),
        );
    }
    assert_eq!(collected, vec!["a.txt", "b.txt", "c.txt"]);
}

#[tokio::test]
async fn v2_listings_start_after_the_named_key() {
    let client = client().await;
    client.put_keys(&["a.txt", "b.txt", "c.txt"]).await;

    let (status, body) = client
        .send("GET", "/acme", "list-type=2&start-after=a.txt", b"")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(keys(&body), vec!["b.txt", "c.txt"]);
    assert_eq!(tag(&body, "KeyCount"), Some("2"));
}
