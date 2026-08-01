//! The S3 gateway end to end: credential custody, SigV4 verification,
//! object round trips, listings, and aws-chunked decoding. The test
//! signer builds real SigV4 signatures independently of the server
//! code, so both sides must agree on the protocol.

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

const MASTER_KEY: &str = "6f2a1c9d8e7b64530f1e2d3c4b5a69788796a5b4c3d2e1f00112233445566778";

async fn stack() -> (axum::Router, axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let cipher = BlobCipher::from_hex(MASTER_KEY).unwrap();
    let state = AppState::new(store, blobs)
        .with_auth(AuthConfig {
            admin_token: Some("root".to_owned()),
            ..AuthConfig::default()
        })
        .with_cipher(Some(cipher));
    let gateway = s3_router(state.clone());
    let admin = s3_admin_router(state);
    (gateway, admin, dir)
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn text_body(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Mint a credential through the admin surface; returns (access key
/// id, secret).
async fn mint(admin: &axum::Router, tenant: &str) -> (String, String) {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/v1/admin/tenants/{tenant}/s3-credentials"))
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = admin.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    (
        body["access_key_id"].as_str().unwrap().to_owned(),
        body["secret_access_key"].as_str().unwrap().to_owned(),
    )
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
    // Civil-from-days: the inverse of the algorithm in the verifier.
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

/// A signed S3 request. The query string must already be sorted by
/// key, exactly as it will be sent.
#[allow(clippy::too_many_arguments)]
fn signed_request(
    method: &str,
    path: &str,
    query: &str,
    payload_hash: &str,
    access_key_id: &str,
    secret: &str,
    body: Body,
    extra_headers: &[(&str, &str)],
) -> Request<Body> {
    let amz_date = amz_now();
    let date = &amz_date[..8];
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let canonical_headers =
        format!("host:localhost\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n",);
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, b"us-east-1");
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
    );
    let uri = if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    };
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "localhost")
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization);
    for (name, value) in extra_headers {
        builder = builder.header(*name, *value);
    }
    builder.body(body).unwrap()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

#[tokio::test]
async fn credentials_gate_the_surface() {
    let (gateway, admin, _dir) = stack().await;
    let (access_key, secret) = mint(&admin, "acme").await;

    // A tampered signature refuses.
    let mut request = signed_request(
        "GET",
        "/acme",
        "list-type=2",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let forged = request.headers()["authorization"]
        .to_str()
        .unwrap()
        .replace("Signature=", "Signature=0");
    request
        .headers_mut()
        .insert("authorization", forged.parse().unwrap());
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(text_body(response).await.contains("SignatureDoesNotMatch"));

    // An unknown access key refuses without an oracle.
    let request = signed_request(
        "GET",
        "/acme",
        "list-type=2",
        "UNSIGNED-PAYLOAD",
        "01hunknownkey0000000000000",
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(text_body(response).await.contains("InvalidAccessKeyId"));

    // A foreign bucket refuses even with a valid signature.
    let request = signed_request(
        "GET",
        "/rivals",
        "list-type=2",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(text_body(response).await.contains("AccessDenied"));

    // Revocation ends the credential.
    let request = Request::builder()
        .method("DELETE")
        .uri(format!(
            "/v1/admin/tenants/acme/s3-credentials/{access_key}"
        ))
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = admin.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let request = signed_request(
        "GET",
        "/acme",
        "list-type=2",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // The listing shows the credential without its secret.
    let request = Request::builder()
        .method("GET")
        .uri("/v1/admin/tenants/acme/s3-credentials")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let body = json_body(admin.clone().oneshot(request).await.unwrap()).await;
    let listed = body["items"].as_array().unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].get("secret_sealed").is_none());
    assert!(listed[0]["revoked_at"].is_string());
}

#[tokio::test]
async fn objects_round_trip_with_integrity() {
    let (gateway, admin, _dir) = stack().await;
    let (access_key, secret) = mint(&admin, "acme").await;
    let payload = b"observability begins at ingest";
    let digest = sha256_hex(payload);

    // PUT with the payload hash signed doubles as a digest assertion.
    let request = signed_request(
        "PUT",
        "/acme/docs/readme.txt",
        "",
        &digest,
        &access_key,
        &secret,
        Body::from(payload.to_vec()),
        &[("content-type", "text/plain")],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["etag"].to_str().unwrap(),
        format!("\"{digest}\""),
    );

    // A lying payload hash fails the upload after hashing.
    let request = signed_request(
        "PUT",
        "/acme/docs/liar.txt",
        "",
        &sha256_hex(b"other bytes"),
        &access_key,
        &secret,
        Body::from(payload.to_vec()),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(text_body(response)
        .await
        .contains("XAmzContentSHA256Mismatch"));

    // GET returns the bytes with the digest ETag; HEAD the metadata.
    let request = signed_request(
        "GET",
        "/acme/docs/readme.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"].to_str().unwrap(),
        "text/plain",
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);

    let request = signed_request(
        "HEAD",
        "/acme/docs/readme.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-length"].to_str().unwrap(),
        payload.len().to_string(),
    );

    // Re-upload at the same key replaces the content.
    let second = b"second version";
    let request = signed_request(
        "PUT",
        "/acme/docs/readme.txt",
        "",
        &sha256_hex(second),
        &access_key,
        &secret,
        Body::from(second.to_vec()),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let request = signed_request(
        "GET",
        "/acme/docs/readme.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let bytes = gateway
        .clone()
        .oneshot(request)
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&bytes[..], second);

    // DELETE is idempotent; the key then reads as absent.
    for _ in 0..2 {
        let request = signed_request(
            "DELETE",
            "/acme/docs/readme.txt",
            "",
            "UNSIGNED-PAYLOAD",
            &access_key,
            &secret,
            Body::empty(),
            &[],
        );
        let response = gateway.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let request = signed_request(
        "GET",
        "/acme/docs/readme.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(text_body(response).await.contains("NoSuchKey"));
}

#[tokio::test]
async fn listing_collapses_prefixes_and_paginates() {
    let (gateway, admin, _dir) = stack().await;
    let (access_key, secret) = mint(&admin, "acme").await;
    for key in ["a/1.txt", "a/b/2.txt", "c.txt"] {
        let body = format!("content of {key}");
        let request = signed_request(
            "PUT",
            &format!("/acme/{key}"),
            "",
            &sha256_hex(body.as_bytes()),
            &access_key,
            &secret,
            Body::from(body.clone().into_bytes()),
            &[],
        );
        let response = gateway.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "put {key}");
    }

    // Root listing with a delimiter: one key, one collapsed prefix.
    let request = signed_request(
        "GET",
        "/acme",
        "delimiter=%2F&list-type=2",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let body = text_body(gateway.clone().oneshot(request).await.unwrap()).await;
    assert!(body.contains("<Key>c.txt</Key>"), "{body}");
    assert!(body.contains("<Prefix></Prefix>"), "{body}");
    assert!(
        body.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"),
        "{body}",
    );
    assert!(!body.contains("<Key>a/1.txt</Key>"), "{body}");

    // A deeper prefix narrows the walk one level.
    let request = signed_request(
        "GET",
        "/acme",
        "delimiter=%2F&list-type=2&prefix=a%2F",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let body = text_body(gateway.clone().oneshot(request).await.unwrap()).await;
    assert!(body.contains("<Key>a/1.txt</Key>"), "{body}");
    assert!(
        body.contains("<CommonPrefixes><Prefix>a/b/</Prefix></CommonPrefixes>"),
        "{body}",
    );

    // Continuation tokens walk the full set one key at a time.
    let mut collected = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let query = match &token {
            Some(token) => format!("continuation-token={token}&list-type=2&max-keys=1"),
            None => "list-type=2&max-keys=1".to_owned(),
        };
        let request = signed_request(
            "GET",
            "/acme",
            &query,
            "UNSIGNED-PAYLOAD",
            &access_key,
            &secret,
            Body::empty(),
            &[],
        );
        let body = text_body(gateway.clone().oneshot(request).await.unwrap()).await;
        if let Some(start) = body.find("<Key>") {
            let end = body[start..].find("</Key>").unwrap() + start;
            collected.push(body[start + 5..end].to_owned());
        }
        match body.find("<NextContinuationToken>") {
            Some(start) => {
                let end = body[start..].find("</NextContinuationToken>").unwrap() + start;
                token = Some(body[start + 23..end].to_owned());
            }
            None => break,
        }
    }
    assert_eq!(collected, vec!["a/1.txt", "a/b/2.txt", "c.txt"]);

    // ListBuckets names exactly the credential's tenant; HeadBucket
    // answers with authorization alone.
    let request = signed_request(
        "GET",
        "/",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let body = text_body(gateway.clone().oneshot(request).await.unwrap()).await;
    assert!(body.contains("<Name>acme</Name>"), "{body}");
    let request = signed_request(
        "HEAD",
        "/acme",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn aws_chunked_bodies_land_decoded() {
    let (gateway, admin, _dir) = stack().await;
    let (access_key, secret) = mint(&admin, "acme").await;

    let framed = b"6;chunk-signature=aaaa\r\nstream\r\n4;chunk-signature=bbbb\r\ning!\r\n0;chunk-signature=cccc\r\n\r\n";
    let request = signed_request(
        "PUT",
        "/acme/chunked.bin",
        "",
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        &access_key,
        &secret,
        Body::from(framed.to_vec()),
        &[("content-encoding", "aws-chunked")],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let request = signed_request(
        "GET",
        "/acme/chunked.bin",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"streaming!");
}

#[tokio::test]
async fn copy_object_moves_no_bytes_and_serves() {
    let (gateway, admin, _dir) = stack().await;
    let (access_key, secret) = mint(&admin, "acme").await;

    let payload = b"copied once, stored once";
    let put = signed_request(
        "PUT",
        "/acme/originals/a.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::from(payload.to_vec()),
        &[("content-type", "text/plain")],
    );
    let response = gateway.clone().oneshot(put).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response.headers()[axum::http::header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();

    // The copy carries no body; the source rides the header.
    let copy = signed_request(
        "PUT",
        "/acme/copies/b.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[("x-amz-copy-source", "/acme/originals/a.txt")],
    );
    let response = gateway.clone().oneshot(copy).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("<CopyObjectResult>"), "{body}");
    let digest = etag.trim_matches('"');
    assert!(
        body.contains(digest),
        "the copy shares the source digest: {body}"
    );

    // The destination serves the source bytes under the same ETag.
    let get = signed_request(
        "GET",
        "/acme/copies/b.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(get).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[axum::http::header::ETAG]
            .to_str()
            .unwrap(),
        etag,
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], payload);

    // A self-copy refuses; a missing source is NoSuchKey; a foreign
    // bucket refuses before any lookup.
    for (source, code) in [
        ("/acme/originals/a.txt", "InvalidRequest"),
        ("/acme/never/was.txt", "NoSuchKey"),
        ("/rivals/theirs.txt", "AccessDenied"),
    ] {
        let target = if source.ends_with("a.txt") && source.starts_with("/acme/originals") {
            "/acme/originals/a.txt"
        } else {
            "/acme/copies/c.txt"
        };
        let copy = signed_request(
            "PUT",
            target,
            "",
            "UNSIGNED-PAYLOAD",
            &access_key,
            &secret,
            Body::empty(),
            &[("x-amz-copy-source", source)],
        );
        let response = gateway.clone().oneshot(copy).await.unwrap();
        let body = text_body(response).await;
        assert!(body.contains(code), "{source}: {body}");
    }
}

#[tokio::test]
async fn batch_delete_removes_and_reports() {
    let (gateway, admin, _dir) = stack().await;
    let (access_key, secret) = mint(&admin, "acme").await;

    for key in ["batch/one.txt", "batch/two.txt"] {
        let put = signed_request(
            "PUT",
            &format!("/acme/{key}"),
            "",
            "UNSIGNED-PAYLOAD",
            &access_key,
            &secret,
            Body::from(b"doomed".to_vec()),
            &[],
        );
        let response = gateway.clone().oneshot(put).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Two real keys and one that never existed: all three report
    // Deleted, because a delete of an absent key is already true.
    let document = "<Delete>        <Object><Key>batch/one.txt</Key></Object>        <Object><Key>batch/two.txt</Key></Object>        <Object><Key>batch/never.txt</Key></Object>        </Delete>";
    let request = signed_request(
        "POST",
        "/acme",
        "delete=",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::from(document.as_bytes().to_vec()),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = text_body(response).await;
    assert_eq!(body.matches("<Deleted>").count(), 3, "{body}");
    assert!(!body.contains("<Error>"), "{body}");

    // The keys are gone from the read path.
    let get = signed_request(
        "GET",
        "/acme/batch/one.txt",
        "",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::empty(),
        &[],
    );
    let response = gateway.clone().oneshot(get).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Quiet mode reports errors only, and an empty document refuses.
    let quiet =
        "<Delete><Quiet>true</Quiet>        <Object><Key>batch/two.txt</Key></Object></Delete>";
    let request = signed_request(
        "POST",
        "/acme",
        "delete=",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::from(quiet.as_bytes().to_vec()),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    let body = text_body(response).await;
    assert!(!body.contains("<Deleted>"), "quiet omits successes: {body}");

    let request = signed_request(
        "POST",
        "/acme",
        "delete=",
        "UNSIGNED-PAYLOAD",
        &access_key,
        &secret,
        Body::from(b"<Delete></Delete>".to_vec()),
        &[],
    );
    let response = gateway.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(text_body(response).await.contains("MalformedXML"));
}
