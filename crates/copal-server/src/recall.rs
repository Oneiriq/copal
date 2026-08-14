//! Recall: the asynchronous path from archive-cold bytes back to a
//! serving copy.
//!
//! An archive-class tier cannot answer a GET until a restore, so
//! recall is explicit and journaled: a flow run issues the backend's
//! restore, polls until the object's bytes answer, then runs the
//! promote motions -- raw copy home, digest verify, CAS flip --
//! leaving the cold copy for the mover's grace-erase. The request
//! that finds bytes archive-cold enqueues the run idempotently and
//! answers 202 with the run to poll; retrying the GET is harmless
//! because the work is durable.
//!
//! Readability is probed, never assumed from the class: an object
//! still readable in an archive tier (written before the bucket's
//! lifecycle transitioned it, or temporarily restored) serves
//! directly, which is exactly how S3 itself behaves.

use std::collections::HashMap;

use serde_json::{json, Value};

use copal_blob::tier::RestoreSpec;
use copal_blob::BlobStore;
use copal_core::{ContentDigest, CopalError, TenantId};
use copal_store::repo::{flow as flow_repo, tier as tier_repo};
use copal_store::Store;

use crate::app::Residencies;

/// The recall workflow key, registered by the standard registry.
pub const RECALL_WORKFLOW: &str = "tier.recall";

/// Per-step attempt ceiling. The await step burns one attempt per
/// probe interval, so this is also the polling budget of one run; a
/// restore outlasting it fails the run, and the next hour's GET
/// starts a fresh one against the by-then-warmer object.
pub const RECALL_MAX_ATTEMPTS: i64 = 600;

/// Seconds the await step sleeps before answering "not yet".
const PROBE_INTERVAL_SECS: u64 = 15;

/// Restore drivers by residency then tier, built at boot from the
/// same configuration the topology reads.
pub type RestoreDrivers = HashMap<String, HashMap<String, RestoreDriver>>;

/// One tier's restore dialect, executable.
#[derive(Debug, Clone)]
pub enum RestoreDriver {
    /// Nothing to issue; the probe decides.
    Instant,
    /// S3 `RestoreObject`, signed with the tier's own credentials.
    S3 {
        bucket: String,
        root: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        days: u32,
    },
}

impl RestoreDriver {
    /// Build from the configuration's spec. `Undriven` cannot reach
    /// here: validation refuses archive classes without a driver,
    /// and online classes never issue restores.
    pub fn from_spec(spec: &RestoreSpec) -> Option<Self> {
        match spec {
            RestoreSpec::Instant => Some(Self::Instant),
            RestoreSpec::S3Restore {
                bucket,
                root,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                days,
            } => Some(Self::S3 {
                bucket: bucket.clone(),
                root: root.clone(),
                endpoint: endpoint.clone(),
                region: region.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                days: *days,
            }),
            RestoreSpec::Undriven(_) => None,
        }
    }

    /// Issue the restore for one object. Idempotent: a restore
    /// already in progress answers as success.
    pub async fn issue(&self, digest: &ContentDigest) -> copal_core::Result<()> {
        match self {
            Self::Instant => Ok(()),
            Self::S3 {
                bucket,
                root,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                days,
            } => {
                let key = match root.as_deref().map(|r| r.trim_matches('/')) {
                    Some(prefix) if !prefix.is_empty() => {
                        format!("{prefix}/objects/{}", digest.storage_key())
                    }
                    _ => format!("objects/{}", digest.storage_key()),
                };
                let region = region.as_deref().unwrap_or("us-east-1");
                let base = match endpoint.as_deref() {
                    Some(endpoint) => endpoint.trim_end_matches('/').to_owned(),
                    None => format!("https://s3.{region}.amazonaws.com"),
                };
                let host = base
                    .strip_prefix("https://")
                    .or_else(|| base.strip_prefix("http://"))
                    .unwrap_or(&base)
                    .to_owned();
                let path = format!("/{bucket}/{key}");
                let body = format!(
                    "<RestoreRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Days>{days}</Days></RestoreRequest>"
                );
                let signed = crate::s3::sigv4::sign_outbound(
                    &axum::http::Method::POST,
                    &host,
                    &path,
                    "restore=",
                    body.as_bytes(),
                    access_key_id,
                    secret_access_key,
                    region,
                );
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .map_err(|e| CopalError::Blob(format!("restore client: {e}")))?;
                let mut request = client.post(format!("{base}{path}?restore")).body(body);
                for (name, value) in signed {
                    request = request.header(name, value);
                }
                let response = request
                    .send()
                    .await
                    .map_err(|e| CopalError::Blob(format!("restore request: {e}")))?;
                match response.status().as_u16() {
                    // 200: already restored or in progress; 202:
                    // restore initiated. 409 is AWS's
                    // RestoreAlreadyInProgress, which is the goal
                    // state by another name.
                    200 | 202 | 409 => Ok(()),
                    404 => Err(CopalError::not_found(format!("archived object {digest}"))),
                    status => {
                        let text = response.text().await.unwrap_or_default();
                        Err(CopalError::Blob(format!(
                            "restore refused with {status}: {text}"
                        )))
                    }
                }
            }
        }
    }
}

/// The idempotency key one recall shares across every request that
/// wants the same bytes: hour-bucketed so a terminally failed run
/// stalls at most the rest of its hour before a fresh GET starts a
/// new one.
pub fn recall_key(residency: &str, digest: &ContentDigest, unix_hour: u64) -> String {
    format!("recall-{residency}-{digest}-{unix_hour}")
}

fn current_hour() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() / 3_600)
        .unwrap_or(0)
}

/// Enqueue the recall for one blob, idempotently. Returns the run id
/// and whether this call created it.
pub async fn enqueue(
    store: &Store,
    tenant: &TenantId,
    residency: &str,
    tier: &str,
    digest: &ContentDigest,
) -> copal_core::Result<(String, bool)> {
    let input = json!({
        "tenant": tenant.as_str(),
        "residency": residency,
        "tier": tier,
        "digest": digest.as_str(),
    });
    flow_repo::enqueue(
        store,
        tenant,
        RECALL_WORKFLOW,
        input,
        None,
        Some(&recall_key(residency, digest, current_hour())),
    )
    .await
}

/// Whether a recall run for this blob is pending or running under
/// the current or previous hour's key: the S3 HEAD's
/// `x-amz-restore` answer.
pub async fn in_flight(
    store: &Store,
    tenant: &TenantId,
    residency: &str,
    digest: &ContentDigest,
) -> bool {
    let hour = current_hour();
    for candidate in [hour, hour.saturating_sub(1)] {
        let key = recall_key(residency, digest, candidate);
        if let Ok(Some(run)) = flow_repo::find_by_idempotency_key(store, tenant, &key).await {
            if matches!(run.status.as_str(), "pending" | "running") {
                return true;
            }
        }
    }
    false
}

/// The REST dialect for archive-cold content: 202 because the
/// request started durable work -- a run exists, it is pollable at
/// `/v1/runs/{id}`, and retrying the GET is harmless. 503 would say
/// try later and mean nothing started.
pub fn accepted_response(run: &str) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    (
        axum::http::StatusCode::ACCEPTED,
        [(axum::http::header::RETRY_AFTER, "60")],
        axum::Json(json!({
            "run": run,
            "detail": "content is archive-cold; a recall is in flight -- \
                       poll the run, then retry this request",
        })),
    )
        .into_response()
}

/// Resolve a pipeline source, enqueueing the recall when it is
/// archive-cold: the step fails retryably, the run's own retry
/// budget carries it, and once the recall promotes the bytes home
/// the retried step finds them.
pub async fn pipeline_source<B: BlobStore>(
    store: &Store,
    residencies: &Residencies<B>,
    topology: &crate::tiering::Topology,
    tenant: &TenantId,
    residency: &str,
    digest: &ContentDigest,
) -> copal_core::Result<B> {
    match residencies
        .resolve_content(store, topology, residency, digest)
        .await?
    {
        crate::app::ContentResolution::Ready(backend) => Ok(backend),
        crate::app::ContentResolution::ArchiveCold { tier } => {
            let (run, _) = enqueue(store, tenant, residency, &tier, digest).await?;
            Err(CopalError::conflict(format!(
                "source is archive-cold; recall run {run} is in flight"
            )))
        }
    }
}

fn parse_input(input: &Value) -> copal_core::Result<(String, String, ContentDigest)> {
    let residency = input["residency"].as_str().unwrap_or("local").to_owned();
    let tier = input["tier"]
        .as_str()
        .ok_or_else(|| CopalError::validation("recall input names no tier"))?
        .to_owned();
    let digest = ContentDigest::parse(input["digest"].as_str().unwrap_or_default())?;
    Ok((residency, tier, digest))
}

/// Register the recall workflow and its activities. Called by the
/// standard registry with the same residencies and drivers the
/// server serves from.
pub fn register<B: BlobStore>(
    registry: copal_flow::FlowRegistry,
    store: Store,
    residencies: Residencies<B>,
    drivers: RestoreDrivers,
) -> copal_flow::FlowRegistry {
    let request_residencies = residencies.clone();
    let await_residencies = residencies.clone();
    let promote_store = store;
    registry
        .activity("tier_restore_request", move |input: Value| {
            let drivers = drivers.clone();
            let residencies = request_residencies.clone();
            async move {
                let (residency, tier, digest) = parse_input(&input)?;
                // Already readable (temporarily restored, or written
                // before the bucket archived it): nothing to issue.
                if residencies
                    .tier_backend(&residency, &tier)?
                    .read_probe(&digest)
                    .await?
                {
                    return Ok(input);
                }
                let driver = drivers
                    .get(&residency)
                    .and_then(|tiers| tiers.get(&tier))
                    .ok_or_else(|| {
                        CopalError::Store(format!(
                            "no restore driver for tier {tier} of residency {residency}"
                        ))
                    })?;
                driver.issue(&digest).await?;
                Ok(input)
            }
        })
        .activity("tier_await_readable", move |input: Value| {
            let residencies = await_residencies.clone();
            async move {
                let (residency, tier, digest) = parse_input(&input)?;
                let backend = residencies.tier_backend(&residency, &tier)?.clone();
                if backend.read_probe(&digest).await? {
                    return Ok(input);
                }
                // One interval per attempt: the workflow's attempt
                // ceiling is the polling budget, and each attempt
                // stays far inside the run claim's lease.
                tokio::time::sleep(std::time::Duration::from_secs(PROBE_INTERVAL_SECS)).await;
                if backend.read_probe(&digest).await? {
                    return Ok(input);
                }
                Err(CopalError::conflict("archived object is not readable yet"))
            }
        })
        .activity("tier_recall_promote", move |input: Value| {
            let store = promote_store.clone();
            let residencies = residencies.clone();
            async move {
                let (residency, tier, digest) = parse_input(&input)?;
                // Someone else -- a rival run, the mover -- already
                // brought it home: the goal state, not a conflict.
                let placed = copal_store::repo::blob::get_location(&store, &residency, &digest)
                    .await?
                    .and_then(|location| location.tier);
                if placed.as_deref() != Some(tier.as_str()) {
                    return Ok(input);
                }
                let cold = residencies.tier_backend(&residency, &tier)?.clone();
                let hot = residencies.get(&residency)?.clone();
                let (_, raw) = cold.open_raw(&digest).await?;
                hot.put_raw(&digest, raw).await?;
                if !crate::mover::verified(&hot, &digest).await? {
                    hot.delete(&digest).await?;
                    return Err(CopalError::Blob(format!(
                        "recalled copy of {digest} failed verification"
                    )));
                }
                tier_repo::promote_flip(&store, &residency, &digest, &tier).await?;
                crate::metrics::incr("copal_tiering_recalled_total");
                Ok(input)
            }
        })
        .workflow(
            RECALL_WORKFLOW,
            &[
                "tier_restore_request",
                "tier_await_readable",
                "tier_recall_promote",
            ],
            RECALL_MAX_ATTEMPTS,
        )
}
