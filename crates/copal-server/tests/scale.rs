//! Scale measurements for the large-object path: streamed single PUT,
//! multipart assembly, ranged reads, and the scan pass, at 512 MiB and
//! above.
//!
//! Everything here is a measurement rather than a check. The
//! assertions only guard that the measured path did what it claims
//! (the bytes round-tripped, the verdict came back clean); none of
//! them compare a timing against a number, because a timing gate on
//! shared hardware is a flaky test with extra steps. The numbers land
//! in docs/operations.md with their conditions.
//!
//! Every test is `#[ignore]`d: these runs take minutes and gigabytes
//! of disk, which the ordinary suite must never pay. `bench/scale.sh`
//! runs them ONE PER PROCESS in release mode, which is what makes the
//! peak-working-set figures attributable: the OS peak only ever
//! ratchets upward, so a process that runs one scenario has that
//! scenario's peak, and reading the ratchet after each phase says
//! which phase owned the growth.
//!
//! Test bodies are generated, never stored: the byte at every offset
//! is a pure function of the offset, so the client side holds one
//! 1 MiB chunk at a time and a ranged read verifies exactly without a
//! reference file. Blob roots live in `tempfile` temp dirs, which
//! delete on drop, failure paths included.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt as _;
use hmac::{Hmac, KeyInit as _, Mac as _};
use http_body_util::BodyExt as _;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tower::ServiceExt as _;

use copal_blob::crypto::BlobCipher;
use copal_blob::{BlobStore as _, ObjectStore};
use copal_server::app::AppState;
use copal_server::s3::{s3_admin_router, s3_router};
use copal_server::{build_router, clamav};
use copal_store::{Store, StoreConfig};

const MASTER_KEY: &str = "1122334455667788990a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";

/// The generator's chunk: 1 MiB, the shape a well-behaved HTTP client
/// streams in.
const CHUNK: usize = 1 << 20;

/// The part size the aws CLI uses when it switches to multipart,
/// which it does above 8 MiB without asking; measuring any other size
/// would describe a client nobody runs.
const PART: u64 = 8 << 20;

/// Process memory as the OS accounts it.
///
/// The peak working set only ratchets upward, which is the property
/// the runner leans on: one scenario per process makes the peak that
/// scenario's peak, and reading it between phases attributes growth
/// to the phase that caused it.
#[cfg(windows)]
mod rss {
    /// PROCESS_MEMORY_COUNTERS as the API fills it on x64. Declared
    /// here rather than through a bindings crate because ten fields
    /// do not justify a dependency in a test binary.
    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn K32GetProcessMemoryInfo(
            process: isize,
            counters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
    }

    /// (peak working set, current working set), both in MiB.
    pub fn peak_and_current_mib() -> (u64, u64) {
        let mut counters = ProcessMemoryCounters {
            cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
            page_fault_count: 0,
            peak_working_set_size: 0,
            working_set_size: 0,
            quota_peak_paged_pool_usage: 0,
            quota_paged_pool_usage: 0,
            quota_peak_non_paged_pool_usage: 0,
            quota_non_paged_pool_usage: 0,
            pagefile_usage: 0,
            peak_pagefile_usage: 0,
        };
        let ok =
            unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) };
        assert!(ok != 0, "GetProcessMemoryInfo failed");
        (
            (counters.peak_working_set_size as u64) >> 20,
            (counters.working_set_size as u64) >> 20,
        )
    }
}

#[cfg(not(windows))]
mod rss {
    /// VmHWM (the peak) and VmRSS from /proc/self/status, in MiB.
    pub fn peak_and_current_mib() -> (u64, u64) {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        let read = |key: &str| -> u64 {
            status
                .lines()
                .find(|line| line.starts_with(key))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
                .unwrap_or(0)
                >> 10
        };
        (read("VmHWM:"), read("VmRSS:"))
    }
}

/// One `name value` line, the shape the container bench records, so
/// the runner can grep measurements out of test-harness chatter.
fn record(name: &str, value: impl std::fmt::Display) {
    println!("{name} {value}");
}

/// Record the memory ratchet after a phase: the peak so far and the
/// working set right now.
fn record_memory(prefix: &str) {
    let (peak, current) = rss::peak_and_current_mib();
    record(&format!("{prefix}_peak_rss_mib"), peak);
    record(&format!("{prefix}_current_rss_mib"), current);
}

/// The byte at any offset. 251 is prime, so the pattern never aligns
/// with the 64 KiB encryption frame or the 1 MiB chunk, and a
/// frame-boundary bug cannot hide inside a repeat.
fn pattern_byte(offset: u64) -> u8 {
    (offset % 251) as u8
}

fn pattern_chunk(offset: u64, len: usize) -> bytes::Bytes {
    let mut buffer = vec![0u8; len];
    for (i, byte) in buffer.iter_mut().enumerate() {
        *byte = pattern_byte(offset + i as u64);
    }
    bytes::Bytes::from(buffer)
}

/// A streaming body of `total` patterned bytes: the generator holds
/// one chunk, so the client side of the measurement can never be the
/// thing holding object-scale memory.
fn pattern_stream(
    total: u64,
) -> impl futures::Stream<Item = Result<bytes::Bytes, std::convert::Infallible>> + Send + Unpin {
    Box::pin(futures::stream::unfold(0u64, move |offset| async move {
        if offset >= total {
            return None;
        }
        let len = std::cmp::min(CHUNK as u64, total - offset) as usize;
        Some((Ok(pattern_chunk(offset, len)), offset + len as u64))
    }))
}

/// The shipping configuration in one process: mem:// metadata, a
/// filesystem blob root, encryption at rest on (the CPE1 sealed
/// format) unless a plaintext delta is being measured.
async fn rest_stack(encrypted: bool, max_upload: usize) -> (axum::Router, tempfile::TempDir) {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = if encrypted {
        ObjectStore::open_encrypted(dir.path().to_str().unwrap(), MASTER_KEY).unwrap()
    } else {
        ObjectStore::open(dir.path().to_str().unwrap()).unwrap()
    };
    let mut state = AppState::new(store, blobs);
    state.limits.max_upload_bytes = max_upload;
    (build_router(state), dir)
}

fn rest_request(method: &str, uri: &str, body: Body) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-copal-tenant", "bench");
    if method == "POST" {
        builder = builder.header("content-type", "application/json");
    }
    builder.body(body).unwrap()
}

async fn create_file(router: &axum::Router, path: &str) -> String {
    let request = rest_request(
        "POST",
        "/v1/files",
        Body::from(json!({ "path": path }).to_string()),
    );
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Drain a response body chunk by chunk, verifying the pattern at
/// every chunk edge (cheap, and enough to catch misalignment) and
/// returning how many bytes arrived.
async fn drain_verified(response: axum::response::Response, start_offset: u64) -> u64 {
    let mut stream = response.into_body().into_data_stream();
    let mut offset = start_offset;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        if !chunk.is_empty() {
            assert_eq!(chunk[0], pattern_byte(offset), "chunk head at {offset}");
            let last = offset + chunk.len() as u64 - 1;
            assert_eq!(
                chunk[chunk.len() - 1],
                pattern_byte(last),
                "chunk tail at {last}"
            );
        }
        offset += chunk.len() as u64;
    }
    offset - start_offset
}

/// Streamed single PUT through the REST face, then ranged reads at
/// three depths and one full sequential read, all against the same
/// object. Ranged timings are best of three with the spread beside
/// them, the way the container bench reports.
async fn single_put_scenario(mib: u64, encrypted: bool, label: &str) {
    let total = mib << 20;
    let (router, _dir) = rest_stack(encrypted, (total as usize) + CHUNK).await;
    let id = create_file(&router, "scale.bin").await;
    record_memory(&format!("{label}_baseline"));

    let started = std::time::Instant::now();
    let upload = rest_request(
        "PUT",
        &format!("/v1/files/{id}/content"),
        Body::from_stream(pattern_stream(total)),
    );
    let response = router.clone().oneshot(upload).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let put_ms = started.elapsed().as_millis();
    record(&format!("{label}_put_ms"), put_ms);
    record(
        &format!("{label}_put_mib_s"),
        mib.saturating_mul(1000) / (put_ms as u64).max(1),
    );
    record_memory(&format!("{label}_after_put"));

    // A 1 MiB window at the front, the middle, and the far end: the
    // far end is the read that would betray a whole-object decrypt,
    // because it can only be fast if the frames covering the window
    // are the only frames touched.
    let window = 1u64 << 20;
    for (name, offset) in [
        ("start", 0),
        ("middle", (total / 2) - (total / 2) % window),
        ("end", total - window),
    ] {
        let mut samples = Vec::new();
        for _ in 0..3 {
            let mut request =
                rest_request("GET", &format!("/v1/files/{id}/content"), Body::empty());
            request.headers_mut().insert(
                "range",
                format!("bytes={offset}-{}", offset + window - 1)
                    .parse()
                    .unwrap(),
            );
            let started = std::time::Instant::now();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            let got = drain_verified(response, offset).await;
            samples.push(started.elapsed().as_millis());
            assert_eq!(got, window);
        }
        samples.sort_unstable();
        record(&format!("{label}_range_{name}_ms"), samples[0]);
        record(
            &format!("{label}_range_{name}_spread_ms"),
            samples[2] - samples[0],
        );
    }
    record_memory(&format!("{label}_after_ranges"));

    let started = std::time::Instant::now();
    let response = router
        .clone()
        .oneshot(rest_request(
            "GET",
            &format!("/v1/files/{id}/content"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let got = drain_verified(response, 0).await;
    let read_ms = started.elapsed().as_millis();
    assert_eq!(got, total);
    record(&format!("{label}_full_read_ms"), read_ms);
    record(
        &format!("{label}_full_read_mib_s"),
        mib.saturating_mul(1000) / (read_ms as u64).max(1),
    );
    record_memory(&format!("{label}_after_full_read"));
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

/// A SigV4-signed request, the same shape the gateway's own tests
/// sign; the query must already be sorted.
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

fn between(haystack: &str, open: &str, close: &str) -> String {
    let start = haystack.find(open).expect("open tag") + open.len();
    let end = haystack[start..].find(close).expect("close tag") + start;
    haystack[start..end].to_owned()
}

/// Multipart through the S3 gateway at the aws CLI's part size:
/// create, upload every part, complete (which is where assembly,
/// sealing, and hashing happen in one streaming pass), then read the
/// assembled object back.
async fn multipart_scenario(mib: u64) {
    let total = mib << 20;
    let parts = total / PART;
    assert_eq!(total % PART, 0, "sizes here are multiples of the part size");

    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open_encrypted(dir.path().to_str().unwrap(), MASTER_KEY).unwrap();
    let mut state = AppState::new(store, blobs)
        .with_auth(copal_server::auth::AuthConfig {
            admin_token: Some("root".to_owned()),
            ..copal_server::auth::AuthConfig::default()
        })
        .with_cipher(Some(BlobCipher::from_hex(MASTER_KEY).unwrap()));
    state.limits.max_upload_bytes = (total as usize) + CHUNK;
    let gateway = s3_router(state.clone());
    let admin = s3_admin_router(state);

    let mint = Request::builder()
        .method("POST")
        .uri("/v1/admin/tenants/bench/s3-credentials")
        .header("x-copal-admin-token", "root")
        .body(Body::empty())
        .unwrap();
    let response = admin.oneshot(mint).await.unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let credential: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let key_id = credential["access_key_id"].as_str().unwrap().to_owned();
    let secret = credential["secret_access_key"].as_str().unwrap().to_owned();
    record_memory("multipart_baseline");

    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/bench/assembled.bin",
            "uploads=",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let upload_id = between(&String::from_utf8_lossy(&body), "<UploadId>", "</UploadId>");

    // Parts go up sequentially: one client, the aws CLI's part size.
    // The client-side SHA256 each signature needs is part of what a
    // real client pays, so it stays inside the timed window.
    let started = std::time::Instant::now();
    let mut etags = Vec::new();
    for part in 0..parts {
        let offset = part * PART;
        let body = pattern_chunk(offset, PART as usize).to_vec();
        let response = gateway
            .clone()
            .oneshot(signed(
                "PUT",
                "/bench/assembled.bin",
                &format!("partNumber={}&uploadId={upload_id}", part + 1),
                &key_id,
                &secret,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "part {}", part + 1);
        etags.push(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"')
                .to_owned(),
        );
    }
    let parts_ms = started.elapsed().as_millis();
    record("multipart_parts_ms", parts_ms);
    record(
        "multipart_parts_mib_s",
        mib.saturating_mul(1000) / (parts_ms as u64).max(1),
    );
    record_memory("multipart_after_parts");

    let manifest: String = etags
        .iter()
        .enumerate()
        .map(|(i, etag)| {
            format!(
                "<Part><PartNumber>{}</PartNumber><ETag>\"{etag}\"</ETag></Part>",
                i + 1,
            )
        })
        .collect();
    let manifest = format!("<CompleteMultipartUpload>{manifest}</CompleteMultipartUpload>");
    let started = std::time::Instant::now();
    let response = gateway
        .clone()
        .oneshot(signed(
            "POST",
            "/bench/assembled.bin",
            &format!("uploadId={upload_id}"),
            &key_id,
            &secret,
            manifest.into_bytes(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let complete_ms = started.elapsed().as_millis();
    record("multipart_complete_ms", complete_ms);
    record(
        "multipart_complete_mib_s",
        mib.saturating_mul(1000) / (complete_ms as u64).max(1),
    );
    record_memory("multipart_after_complete");

    let started = std::time::Instant::now();
    let response = gateway
        .clone()
        .oneshot(signed(
            "GET",
            "/bench/assembled.bin",
            "",
            &key_id,
            &secret,
            Vec::new(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let got = drain_verified(response, 0).await;
    assert_eq!(got, total);
    record("multipart_read_back_ms", started.elapsed().as_millis());
    record_memory("multipart_after_read_back");
}

/// A protocol-faithful clamd stand-in: accept one INSTREAM session,
/// drain the frames into a fixed buffer, answer clean. The point is
/// to run the REAL scan client over a socket without shipping bytes
/// to an actual scanner, so the measurement covers exactly what the
/// scan activity allocates and sends.
async fn fake_clamd() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut command = [0u8; 10];
        socket.read_exact(&mut command).await.unwrap();
        assert_eq!(&command, b"zINSTREAM\0");
        let mut sink = vec![0u8; CHUNK];
        loop {
            let mut len = [0u8; 4];
            socket.read_exact(&mut len).await.unwrap();
            let mut remaining = u32::from_be_bytes(len) as usize;
            if remaining == 0 {
                break;
            }
            while remaining > 0 {
                let take = remaining.min(sink.len());
                socket.read_exact(&mut sink[..take]).await.unwrap();
                remaining -= take;
            }
        }
        socket.write_all(b"stream: OK\0").await.unwrap();
        socket.shutdown().await.unwrap();
    });
    addr
}

/// What the scan pass holds. The pipeline's scan activity buffers the
/// WHOLE object (`blobs.read`) before streaming it to clamd, and on a
/// sealed object that read materializes the ciphertext and the
/// plaintext at once. This scenario seeds a sealed object through the
/// streaming writer (which stays flat), then runs the same read and
/// the same clamd client the activity runs, so the recorded peak is
/// the answer to the roadmap's stated unknown.
async fn scan_scenario(mib: u64) {
    let total = mib << 20;
    let dir = tempfile::tempdir().unwrap();
    let blobs = ObjectStore::open_encrypted(dir.path().to_str().unwrap(), MASTER_KEY).unwrap();
    let stored = blobs.put_streamed(pattern_stream(total)).await.unwrap();
    assert_eq!(stored.size_bytes, total);
    record_memory("scan_baseline");

    let addr = fake_clamd().await;
    let started = std::time::Instant::now();
    let content = blobs.read(&stored.digest).await.unwrap();
    let read_ms = started.elapsed().as_millis();
    assert_eq!(content.len() as u64, total);
    record("scan_buffered_read_ms", read_ms);
    record_memory("scan_after_buffered_read");

    let started = std::time::Instant::now();
    let verdict = clamav::scan(&addr.to_string(), &content).await.unwrap();
    assert_eq!(verdict, clamav::Verdict::Clean);
    record("scan_instream_ms", started.elapsed().as_millis());
    record_memory("scan_after_instream");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_single_put_512mib() {
    single_put_scenario(512, true, "put_512mib").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_single_put_1gib() {
    single_put_scenario(1024, true, "put_1gib").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_single_put_2gib() {
    single_put_scenario(2048, true, "put_2gib").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_single_put_1gib_plaintext() {
    single_put_scenario(1024, false, "put_1gib_plain").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_multipart_512mib() {
    multipart_scenario(512).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_multipart_1gib() {
    multipart_scenario(1024).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_multipart_2gib() {
    multipart_scenario(2048).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_scan_pass_512mib() {
    scan_scenario(512).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_scan_pass_1gib() {
    scan_scenario(1024).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale bench: minutes and gigabytes; run via bench/scale.sh"]
async fn scale_scan_pass_2gib() {
    scan_scenario(2048).await;
}
