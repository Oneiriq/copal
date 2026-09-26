//! S3 multipart upload end to end: create, upload parts out of order,
//! re-send a part, complete, verify the assembled object, and abort.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::BodyExt as _;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::ObjectStore;
use copal_server::app::{AppState, Residencies};
use copal_server::auth::AuthConfig;
use copal_server::s3::{s3_admin_router, s3_router};
use copal_server::sweeps::{run_pass, SweepConfig};
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "1122334455667788990a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";

struct Stack {
    gateway: axum::Router,
    admin: axum::Router,
    store: Store,
    residencies: Residencies<ObjectStore>,
    dir: tempfile::TempDir,
}

async fn stack() -> Stack {
    stack_with_floor(1).await
}

async fn stack_with_floor(floor: i64) -> Stack {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open(dir.path().to_str().unwrap()).unwrap();
    let mut state = AppState::new(store.clone(), blobs.clone())
        .with_auth(AuthConfig {
            admin_token: Some("root".to_owned()),
            ..AuthConfig::default()
        })
        .with_cipher(Some(BlobCipher::from_hex(MASTER_KEY).unwrap()));
    // These tests exercise assembly and lifecycle, not part sizing;
    // pushing 5 MiB per part to satisfy the default floor would buy
    // nothing. The floor has its own test below.
    state.limits.min_multipart_part_bytes = floor;
    Stack {
        gateway: s3_router(state.clone()),
        admin: s3_admin_router(state),
        store,
        residencies: Residencies::local_only(blobs),
        dir,
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn text_body(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn mint(admin: &axum::Router) -> (String, String) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/acme/s3-credentials")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let body = json_body(admin.clone().oneshot(request).await.unwrap()).await;
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

/// A signed request; the query must already be sorted.
fn signed(
    method: &str,
    path: &str,
    query: &str,
    access_key_id: &str,
    secret: &str,
    body: Vec<u8>,
) -> Request<Body> {
    let payload_hash = hex::encode(Sha256::digest(&body));
    let amz_date = amz_now();
    let date = &amz_date[..8];
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let canonical_headers =
        format!("host:localhost\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
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
    Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "localhost")
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .header("content-length", body.len().to_string())
        .body(Body::from(body))
        .unwrap()
}

/// Staged part FILES under the multipart prefix; the per-session
/// directories outlive their contents on a filesystem backend and
/// carry no bytes.
fn staged_part_files(dir: &tempfile::TempDir) -> usize {
    fn walk(path: &std::path::Path, found: &mut usize) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, found);
                } else {
                    *found += 1;
                }
            }
        }
    }
    let mut found = 0;
    walk(&dir.path().join("mpu"), &mut found);
    found
}

fn between(haystack: &str, open: &str, close: &str) -> String {
    let start = haystack.find(open).expect("open tag") + open.len();
    let end = haystack[start..].find(close).expect("close tag") + start;
    haystack[start..end].to_owned()
}

#[tokio::test]
async fn multipart_assembles_parts_in_order() {
    let stack = stack().await;
    let (key_id, secret) = mint(&stack.admin).await;
    let gateway = &stack.gateway;

    // Create.
    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/big/archive.bin",
            "uploads=",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = text_body(response).await;
    let upload_id = between(&body, "<UploadId>", "</UploadId>");
    assert!(!upload_id.is_empty());

    // Parts, sent out of order: 2 then 1, and part 1 re-sent with the
    // content that must win.
    let part1_wrong = vec![b'w'; 40];
    let part1 = vec![b'a'; 40];
    let part2 = vec![b'b'; 25];

    for (number, bytes) in [(2, &part2), (1, &part1_wrong), (1, &part1)] {
        let response = gateway
            .clone()
            .oneshot(signed(
                "PUT",
                "/acme/big/archive.bin",
                &format!("partNumber={number}&uploadId={upload_id}"),
                &key_id,
                &secret,
                bytes.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "part {number}");
        let etag = response.headers()["etag"].to_str().unwrap().to_owned();
        assert_eq!(
            etag.trim_matches('"'),
            hex::encode(Sha256::digest(bytes)),
            "part etag is its digest",
        );
    }

    // ListParts sees exactly two parts, the re-sent one replacing.
    let response = gateway
        .clone()
        .oneshot(signed(
            "GET",
            "/acme/big/archive.bin",
            &format!("uploadId={upload_id}"),
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    let body = text_body(response).await;
    assert_eq!(body.matches("<Part>").count(), 2, "{body}");
    assert!(body.contains("<Size>40</Size>"), "{body}");

    // Complete with a manifest naming both parts.
    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{}\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>",
        hex::encode(Sha256::digest(&part1)),
        hex::encode(Sha256::digest(&part2)),
    );
    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/big/archive.bin",
            &format!("uploadId={upload_id}"),
            &key_id,
            &secret,
            manifest.into_bytes(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The assembled object is part1 then part2, byte for byte.
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    let response = gateway
        .clone()
        .oneshot(signed(
            "GET",
            "/acme/big/archive.bin",
            "",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], &expected[..]);

    // The session is gone, and so are its staged parts.
    let response = gateway
        .clone()
        .oneshot(signed(
            "GET",
            "/acme/big/archive.bin",
            &format!("uploadId={upload_id}"),
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(staged_part_files(&stack.dir), 0, "staged parts cleaned");
}

#[tokio::test]
async fn aborted_and_abandoned_sessions_release_their_parts() {
    let stack = stack().await;
    let (key_id, secret) = mint(&stack.admin).await;
    let gateway = &stack.gateway;

    let start = |key: &'static str| {
        let (key_id, secret) = (key_id.clone(), secret.clone());
        let gateway = gateway.clone();
        async move {
            let response = gateway
                .clone()
                .oneshot(signed(
                    "POST",
                    key,
                    "uploads=",
                    &key_id,
                    &secret,
                    Vec::new(),
                ))
                .await
                .unwrap();
            let body = text_body(response).await;
            let upload_id = between(&body, "<UploadId>", "</UploadId>");
            let response = gateway
                .oneshot(signed(
                    "PUT",
                    key,
                    &format!("partNumber=1&uploadId={upload_id}"),
                    &key_id,
                    &secret,
                    vec![b'p'; 10],
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            upload_id
        }
    };

    // One session is aborted explicitly.
    let aborted = start("/acme/aborted.bin").await;
    let response = gateway
        .clone()
        .oneshot(signed(
            "DELETE",
            "/acme/aborted.bin",
            &format!("uploadId={aborted}"),
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Another is simply abandoned; a zero-TTL sweep collects it.
    let _abandoned = start("/acme/abandoned.bin").await;
    let config = SweepConfig {
        tus_session_ttl_secs: 0,
        ..SweepConfig::default()
    };
    let report = run_pass(&stack.store, &stack.residencies, &config).await;
    assert_eq!(report.multipart_swept, 1);

    assert_eq!(
        staged_part_files(&stack.dir),
        0,
        "both sessions released their parts",
    );
}

#[tokio::test]
async fn undersized_parts_and_open_sessions_behave_like_s3() {
    let stack = stack_with_floor(5 * 1024 * 1024).await;
    let (key_id, secret) = mint(&stack.admin).await;
    let gateway = &stack.gateway;

    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/small/parts.bin",
            "uploads=",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    let body = text_body(response).await;
    let upload_id = between(&body, "<UploadId>", "</UploadId>");

    // An open session is listable, which is how clients find work to
    // resume or abort.
    let response = gateway
        .clone()
        .oneshot(signed(
            "GET",
            "/acme",
            "uploads=",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    let listing = text_body(response).await;
    assert!(listing.contains("<Key>small/parts.bin</Key>"), "{listing}");
    assert!(listing.contains(&upload_id), "{listing}");

    // Two small parts: the first is under the 5 MiB floor every S3
    // implementation enforces, so completion must refuse rather than
    // assemble a file no other implementation would have accepted.
    for number in [1, 2] {
        let response = gateway
            .clone()
            .oneshot(signed(
                "PUT",
                "/acme/small/parts.bin",
                &format!("partNumber={number}&uploadId={upload_id}"),
                &key_id,
                &secret,
                vec![b'x'; 1_000],
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/small/parts.bin",
            &format!("uploadId={upload_id}"),
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let refusal = text_body(response).await;
    assert!(refusal.contains("EntityTooSmall"), "{refusal}");

    // A single small part is fine: the last part carries no minimum,
    // and a one-part upload is all last part.
    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/small/single.bin",
            "uploads=",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    let body = text_body(response).await;
    let single = between(&body, "<UploadId>", "</UploadId>");
    let response = gateway
        .clone()
        .oneshot(signed(
            "PUT",
            "/acme/small/single.bin",
            &format!("partNumber=1&uploadId={single}"),
            &key_id,
            &secret,
            vec![b'y'; 10],
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/small/single.bin",
            &format!("uploadId={single}"),
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "one part is the last part"
    );
}

/// Completion assembles the parts the manifest lists and nothing
/// else, and refuses a list out of order the way S3 does. Parts left
/// out go with the session.
#[tokio::test]
async fn completion_assembles_only_the_listed_parts() {
    let stack = stack().await;
    let (key_id, secret) = mint(&stack.admin).await;
    let gateway = &stack.gateway;

    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/acme/picked/parts.bin",
            "uploads=",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    let upload_id = between(&text_body(response).await, "<UploadId>", "</UploadId>");

    let parts = [vec![b'x'; 12], vec![b'y'; 9], vec![b'z'; 7]];
    for (index, bytes) in parts.iter().enumerate() {
        let response = gateway
            .clone()
            .oneshot(signed(
                "PUT",
                "/acme/picked/parts.bin",
                &format!("partNumber={}&uploadId={upload_id}", index + 1),
                &key_id,
                &secret,
                bytes.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let entry = |number: usize| {
        format!(
            "<Part><PartNumber>{number}</PartNumber><ETag>\"{}\"</ETag></Part>",
            hex::encode(Sha256::digest(&parts[number - 1])),
        )
    };
    let complete = |manifest: String| {
        signed(
            "POST",
            "/acme/picked/parts.bin",
            &format!("uploadId={upload_id}"),
            &key_id,
            &secret,
            format!("<CompleteMultipartUpload>{manifest}</CompleteMultipartUpload>").into_bytes(),
        )
    };

    let response = gateway
        .clone()
        .oneshot(complete(format!("{}{}", entry(3), entry(1))))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(text_body(response).await.contains("InvalidPartOrder"));

    let response = gateway
        .clone()
        .oneshot(complete(format!("{}{}", entry(1), entry(3))))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = gateway
        .clone()
        .oneshot(signed(
            "GET",
            "/acme/picked/parts.bin",
            "",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let mut expected = parts[0].clone();
    expected.extend_from_slice(&parts[2]);
    assert_eq!(&bytes[..], &expected[..], "part 2 was left out");
    assert_eq!(
        staged_part_files(&stack.dir),
        0,
        "every staged part cleaned"
    );
}
