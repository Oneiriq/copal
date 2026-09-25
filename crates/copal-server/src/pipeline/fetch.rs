//! The URL-ingestion activity: content the server pulls itself.
//!
//! Split out of the pipeline module by size alone; the fetch worker
//! is the same kind of activity body its siblings are. A fetch is an
//! upload the server performs on the caller's behalf, so everything
//! here mirrors the upload path deliberately: the outbound policy
//! gates the tenant-supplied URL, the upload ceiling rides the
//! stream, completion walks the same claim-and-complete path, and
//! the fetched bytes are handed to the standard post-upload run so a
//! fetched file is indistinguishable from an uploaded one by the
//! time it serves.

use futures::StreamExt as _;
use serde_json::{json, Value};

use copal_blob::BlobStore;
use copal_core::{CopalError, FileId, FileState, TenantId};
use copal_store::repo::{blob as blob_repo, completion as completion_repo, file as file_repo};
use copal_store::Store;

use super::{refuse_derived, upload_run_key, UPLOAD_WORKFLOW};

/// Deterministic idempotency key for a fetch run: one ingestion per
/// record per source URL.
pub fn fetch_run_key(file: &FileId, url_digest: &str) -> String {
    format!("fetch:{file}:{url_digest}")
}

/// What the fetch activity is allowed to pull.
#[derive(Debug, Clone)]
pub struct FetchPolicy {
    /// Whether tenant-supplied URLs may point at private address
    /// space. The same question webhooks answer, asked of ingestion.
    pub allow_private_targets: bool,
    /// Byte ceiling on fetched bodies; the upload ceiling, since a
    /// fetch is an upload the server performs on the caller's behalf.
    pub max_bytes: u64,
}

impl Default for FetchPolicy {
    fn default() -> Self {
        Self {
            allow_private_targets: false,
            max_bytes: 1 << 30,
        }
    }
}

/// Redirects a fetch follows before it gives up on the source.
const MAX_REDIRECTS: usize = 5;

/// Where opening a source ended: its response, or a reason the record
/// fails without a retry.
enum Opened {
    Response(reqwest::Response),
    Refused(String),
}

/// Request `url`, following redirects by hand. The client never
/// follows one itself: a redirect names a new destination the source
/// chose, so every hop passes `vet` first and connects only to the
/// addresses `vet` checked, never to a fresh DNS answer.
async fn open_source<F>(url: &str, vet: F) -> copal_core::Result<Opened>
where
    F: Fn(&str) -> copal_core::Result<crate::netguard::Vetted> + Sync,
{
    let mut current = url.to_owned();
    let mut redirects = 0;
    loop {
        let vetted = match vet(&current) {
            Ok(vetted) => vetted,
            Err(refusal) => return Ok(Opened::Refused(format!("outbound policy: {refusal}"))),
        };
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none());
        if let Some((host, addrs)) = &vetted.pin {
            builder = builder.resolve_to_addrs(host, addrs);
        }
        let client = builder
            .build()
            .map_err(|e| CopalError::Blob(format!("fetch client: {e}")))?;
        let response = crate::trace::inject(client.get(vetted.url.clone()))
            .send()
            .await
            .map_err(|e| CopalError::Blob(format!("fetch {current}: {e}")))?;
        if !response.status().is_redirection() {
            return Ok(Opened::Response(response));
        }
        // A 3xx with nowhere to go (a 304, a bare 300) is the source's
        // answer, and the status checks downstream judge it.
        let Some(location) = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
        else {
            return Ok(Opened::Response(response));
        };
        let Ok(next) = vetted.url.join(location) else {
            return Ok(Opened::Refused(format!(
                "source redirected to an invalid location {location:?}",
            )));
        };
        redirects += 1;
        if redirects > MAX_REDIRECTS {
            return Ok(Opened::Refused(format!(
                "source redirected more than {MAX_REDIRECTS} times",
            )));
        }
        current = next.into();
    }
}

/// The URL-ingestion activity body. The primary record was created as
/// a draft by the API; this pulls the remote bytes under the outbound
/// policy and the upload ceiling, completes the upload into scanning,
/// and enqueues the standard post-upload run, so fetched content
/// walks the same scan and finalize path an uploaded body does.
pub(super) async fn fetch_source<B: BlobStore>(
    store: &Store,
    residencies: &crate::app::Residencies<B>,
    policy: &FetchPolicy,
    input: Value,
) -> copal_core::Result<Value> {
    let tenant = TenantId::parse(input["tenant"].as_str().unwrap_or_default())?;
    let file = FileId::parse(input["file"].as_str().unwrap_or_default())?;
    let url = input["url"].as_str().unwrap_or_default().to_owned();

    // Replay after success: content already landed.
    if let Some(record) = file_repo::get_file(store, &tenant, &file).await? {
        if record.digest.is_some()
            && record.state != FileState::Draft
            && record.state != FileState::Failed
        {
            return Ok(json!({
                "file": file.as_str(),
                "outcome": "already-fetched",
            }));
        }
    }

    // The URL is tenant-supplied, so the outbound policy applies at
    // execution too: configuration may have tightened since enqueue.
    // It applies to every redirect hop as well, since the source picks
    // those.
    let allow_private = policy.allow_private_targets;
    let vet = move |candidate: &str| {
        if allow_private {
            crate::netguard::accept_outbound_url(candidate)
        } else {
            crate::netguard::vet_outbound_url(candidate)
        }
    };
    let response = match open_source(&url, vet).await? {
        Opened::Response(response) => response,
        Opened::Refused(reason) => return refuse_derived(store, &tenant, &file, reason).await,
    };
    let status = response.status();
    if status.is_client_error() {
        let reason = format!("source answered {status}");
        return refuse_derived(store, &tenant, &file, reason).await;
    }
    if !status.is_success() {
        return Err(CopalError::Blob(format!("source answered {status}")));
    }
    let served_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_owned());
    if let Some(length) = response.content_length() {
        if length > policy.max_bytes {
            let reason = format!(
                "source is {length} bytes; the upload ceiling is {}",
                policy.max_bytes,
            );
            return refuse_derived(store, &tenant, &file, reason).await;
        }
    }

    // Claim before the first byte, the same order uploads use; the
    // ceiling rides the stream so a lying Content-Length still stops
    // at the limit.
    file_repo::claim_upload(store, &tenant, &file, "fetch-worker", 900).await?;
    let ceiling = policy.max_bytes;
    let mut total: u64 = 0;
    let counted = response.bytes_stream().map(move |chunk| match chunk {
        Ok(bytes) => {
            total += bytes.len() as u64;
            if total > ceiling {
                Err(format!(
                    "source exceeds the upload ceiling at {total} bytes"
                ))
            } else {
                Ok(bytes)
            }
        }
        Err(e) => Err(format!("fetch stream: {e}")),
    });
    let counted = std::pin::pin!(counted);
    let residency = copal_store::repo::tenant::get_residency(store, &tenant).await?;
    let target = residencies.get(&residency)?;
    let stored = match target.put_streamed(counted).await {
        Ok(stored) => stored,
        Err(CopalError::Blob(reason)) if reason.contains("exceeds the upload ceiling") => {
            return refuse_derived(store, &tenant, &file, reason).await;
        }
        Err(other) => return Err(other),
    };
    blob_repo::record_sighting(
        store,
        &stored.digest,
        stored.size_bytes,
        &residency,
        &stored.storage_path,
    )
    .await?;
    // The declared type wins; otherwise the source's served type
    // lands before completion.
    let declared = input["declared"].as_bool().unwrap_or(false);
    if !declared {
        if let Some(served) = served_type.as_deref() {
            file_repo::set_content_type(store, &tenant, &file, served).await?;
        }
    }
    // The declaration the fetch request carried, validated at the
    // API before the record existed; it lands on the version row
    // like any uploaded content's would, so extraction resolves it
    // against whatever the source actually served.
    let markers = match input.get("markers") {
        None | Some(Value::Null) => None,
        Some(declaration) => Some(declaration.clone()),
    };
    let record = completion_repo::complete_upload(
        store,
        &tenant,
        &file,
        &residency,
        &stored.digest,
        stored.size_bytes,
        "fetch",
        FileState::Scanning,
        markers.as_ref(),
    )
    .await?;
    let post_input = json!({
        "tenant": tenant.as_str(),
        "file": file.as_str(),
        "residency": residency,
        "digest": stored.digest.as_str(),
        "declared_type": record.content_type,
        "path": record.path,
    });
    copal_store::repo::flow::enqueue(
        store,
        &tenant,
        UPLOAD_WORKFLOW,
        post_input,
        Some(&file),
        Some(&upload_run_key(&file, &stored.digest)),
    )
    .await?;
    crate::metrics::incr("copal_fetches_total");
    Ok(json!({
        "file": file.as_str(),
        "outcome": "fetched",
        "digest": stored.digest.as_str(),
        "bytes": stored.size_bytes,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::http::{header::LOCATION, StatusCode};
    use axum::routing::get;

    use super::*;
    use crate::netguard::{accept_outbound_url, vet_outbound_url, Vetted};

    /// A loopback origin: `/start` redirects to `/secret` on the same
    /// listener, `/metadata` to the cloud metadata address, and `/loop`
    /// to itself. `hits` counts requests that reach `/secret`.
    async fn origin(hits: Arc<AtomicUsize>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let secret = format!("{base}/secret");
        let app = axum::Router::new()
            .route(
                "/start",
                get(move || async move { (StatusCode::FOUND, [(LOCATION, secret)]) }),
            )
            .route(
                "/metadata",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(LOCATION, "http://169.254.169.254/latest/meta-data/")],
                    )
                }),
            )
            .route(
                "/loop",
                get(|| async { (StatusCode::FOUND, [(LOCATION, "/loop")]) }),
            )
            .route(
                "/secret",
                get(move || {
                    hits.fetch_add(1, Ordering::SeqCst);
                    async { "internal" }
                }),
            );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        base
    }

    /// The outbound policy as a deployment runs it, except that the
    /// test's own start URL stands in for a public origin.
    fn policy_except(start: String) -> impl Fn(&str) -> copal_core::Result<Vetted> + Sync {
        move |candidate| {
            if candidate == start {
                accept_outbound_url(candidate)
            } else {
                vet_outbound_url(candidate)
            }
        }
    }

    #[tokio::test]
    async fn every_redirect_hop_meets_the_outbound_policy() {
        let hits = Arc::new(AtomicUsize::new(0));
        let base = origin(hits.clone()).await;
        for path in ["/start", "/metadata"] {
            let start = format!("{base}{path}");
            match open_source(&start, policy_except(start.clone()))
                .await
                .unwrap()
            {
                Opened::Refused(reason) => {
                    assert!(reason.contains("non-public"), "{path}: {reason}");
                }
                Opened::Response(response) => {
                    panic!("{path} was followed to {}", response.url());
                }
            }
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "the private hop was requested"
        );
    }

    #[tokio::test]
    async fn redirects_are_followed_when_the_policy_allows_the_target() {
        let hits = Arc::new(AtomicUsize::new(0));
        let base = origin(hits.clone()).await;
        let Opened::Response(response) = open_source(&format!("{base}/start"), accept_outbound_url)
            .await
            .unwrap()
        else {
            panic!("an allowed redirect was refused");
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "internal");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_redirect_loop_stops_at_the_hop_limit() {
        let base = origin(Arc::new(AtomicUsize::new(0))).await;
        match open_source(&format!("{base}/loop"), accept_outbound_url)
            .await
            .unwrap()
        {
            Opened::Refused(reason) => assert!(reason.contains("more than"), "{reason}"),
            Opened::Response(response) => panic!("the loop ended at {}", response.status()),
        }
    }
}
