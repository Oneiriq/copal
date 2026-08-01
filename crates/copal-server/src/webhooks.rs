//! Webhooks: registration surface and the delivery dispatcher.
//!
//! Events come from the engine outbox (a DEFINE EVENT on `file` writes
//! `file_event` rows in the transaction that changes state), so
//! delivery is at-least-once across restarts. The dispatcher fans
//! events out to per-endpoint delivery rows, claims each attempt with
//! the shared CAS discipline, signs the body with the endpoint's
//! secret, and backs off on failure. LIVE SELECT on the outbox is the
//! wake signal; a slow tick covers a dropped subscription.
//!
//! Endpoint secrets follow the S3 credential custody rule: signing
//! needs the secret back, so it is stored sealed under the blob master
//! key, and the whole surface exists only when that key is configured.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use futures::StreamExt as _;
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;

use copal_blob::crypto::BlobCipher;
use copal_blob::BlobStore;
use copal_core::CopalError;
use copal_store::repo::eventing;
use copal_store::Store;

use crate::app::{forwarded_origin, AppState};
use crate::error::ApiError;

/// The outbox table the dispatcher watches.
const EVENT_TABLE: &str = "file_event";
/// Per-attempt claim lease.
const CLAIM_LEASE_SECS: u32 = 60;
/// Outbound request deadline.
const HTTP_TIMEOUT_SECS: u64 = 10;
/// Rows per pass, both fan-out and delivery.
const PASS_BATCH: i64 = 100;

/// Webhook management state: the application plus the sealing cipher.
#[derive(Clone)]
pub struct WebhookState<B: BlobStore> {
    pub app: AppState<B>,
}

/// Tenant-facing webhook routes. Built only when the blob master key
/// exists, because registration seals the signing secret under it.
pub fn webhook_router<B: BlobStore + 'static>(app: AppState<B>) -> Router {
    let state = WebhookState { app };
    Router::new()
        .route(
            "/v1/webhooks",
            axum::routing::post(register_endpoint::<B>).get(list_endpoints::<B>),
        )
        .route("/v1/webhooks/deliveries", get(list_deliveries::<B>))
        .route(
            "/v1/webhooks/{id}",
            axum::routing::delete(remove_endpoint::<B>),
        )
        .route("/v1/events", get(list_events::<B>))
        .with_state(state)
}

/// Registration body. Events filter by dotted action; empty means all.
#[derive(Debug, Deserialize)]
struct RegisterRequest {
    url: String,
    #[serde(default)]
    events: Vec<String>,
}

/// Register an endpoint. The signing secret appears exactly once, in
/// this response; the store keeps it sealed.
async fn register_endpoint<B: BlobStore>(
    State(state): State<WebhookState<B>>,
    headers: HeaderMap,
    Json(request): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let tenant = crate::auth::authenticate(&state.app, &headers).await?;
    let body = register_core(
        &state.app,
        &tenant,
        &request.url,
        &request.events,
        forwarded_origin(&headers).as_deref(),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(body)))
}

/// Register an endpoint, the shared core behind the REST handler and
/// the GraphQL action resolver.
pub(crate) async fn register_core<B: BlobStore>(
    app: &AppState<B>,
    tenant: &copal_core::TenantId,
    url: &str,
    events: &[String],
    origin: Option<&str>,
) -> Result<serde_json::Value, ApiError> {
    // Refuse destinations inside the deployment before a row exists.
    if !app.limits.allow_private_webhook_targets {
        crate::netguard::check_outbound_url(url)?;
    }
    let secret = copal_sign::ApiKeyToken::mint().secret;
    let sealed = app.require_cipher()?.seal(secret.as_bytes())?;
    let sealed_b64 = base64::engine::general_purpose::STANDARD.encode(sealed);
    let events = events.join(",");
    let row = eventing::create_endpoint(&app.store, tenant, url, &events, &sealed_b64).await?;
    copal_store::repo::auth::record_audit(
        &app.store,
        tenant,
        tenant.as_str(),
        "webhook.registered",
        &row.endpoint_id(),
        origin,
        Some(json!({ "url": row.target_url })),
    )
    .await?;
    Ok(json!({
        "id": row.endpoint_id(),
        "url": row.target_url,
        "events": row.events,
        "secret": secret,
        "created_at": row.created_at,
    }))
}

/// List a tenant's endpoints (never their secrets).
async fn list_endpoints<B: BlobStore>(
    State(state): State<WebhookState<B>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = crate::auth::authenticate(&state.app, &headers).await?;
    let items = eventing::list_endpoints(&state.app.store, &tenant).await?;
    Ok(Json(json!({ "items": items })))
}

/// Deactivate an endpoint; unknown and already-inactive both read 404.
async fn remove_endpoint<B: BlobStore>(
    State(state): State<WebhookState<B>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let tenant = crate::auth::authenticate(&state.app, &headers).await?;
    remove_core(
        &state.app,
        &tenant,
        &id,
        forwarded_origin(&headers).as_deref(),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Deactivate an endpoint, shared by both faces.
pub(crate) async fn remove_core<B: BlobStore>(
    app: &AppState<B>,
    tenant: &copal_core::TenantId,
    id: &str,
    origin: Option<&str>,
) -> Result<(), ApiError> {
    if !eventing::deactivate_endpoint(&app.store, tenant, id).await? {
        return Err(CopalError::not_found(format!("webhook {id}")).into());
    }
    copal_store::repo::auth::record_audit(
        &app.store,
        tenant,
        tenant.as_str(),
        "webhook.removed",
        id,
        origin,
        None,
    )
    .await?;
    Ok(())
}

/// Bounded-listing query.
#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    limit: Option<i64>,
    /// Narrow the feed to one dotted verb, the same filter the
    /// subscription takes.
    #[serde(default)]
    action: Option<String>,
}

/// A tenant's recent file events, newest first.
async fn list_events<B: BlobStore>(
    State(state): State<WebhookState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = crate::auth::authenticate(&state.app, &headers).await?;
    let limit = params.limit.unwrap_or(100).clamp(1, 1_000);
    let rows =
        eventing::list_events(&state.app.store, &tenant, params.action.as_deref(), limit).await?;
    let items: Vec<_> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.event_id(),
                "event": row.action,
                "file": row.file_id(),
                "payload": row.payload,
                "created_at": row.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "items": items })))
}

/// A tenant's recent delivery attempts, newest first.
async fn list_deliveries<B: BlobStore>(
    State(state): State<WebhookState<B>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<ListQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let tenant = crate::auth::authenticate(&state.app, &headers).await?;
    let limit = params.limit.unwrap_or(100).clamp(1, 1_000);
    let rows = eventing::list_deliveries(&state.app.store, &tenant, limit).await?;
    let items: Vec<_> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.delivery_id(),
                "event": row.event_id(),
                "endpoint": row.endpoint_id(),
                "state": row.state,
                "attempts": row.attempts,
                "next_attempt_at": row.next_attempt_at,
                "last_status": row.last_status,
                "created_at": row.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "items": items })))
}

/// What one dispatcher pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DispatchReport {
    pub events_dispatched: u64,
    pub delivered: u64,
    pub retried: u64,
    pub failed: u64,
}

impl DispatchReport {
    /// Whether the pass found nothing to do.
    pub fn idle(&self) -> bool {
        *self == Self::default()
    }
}

/// One dispatcher pass: fan out fresh events, then attempt every due
/// delivery. Every step is CAS-guarded, so concurrent replicas split
/// the work instead of duplicating it.
pub async fn run_pass(
    store: &Store,
    cipher: &BlobCipher,
    http: &reqwest::Client,
    instance_id: &str,
    allow_private: bool,
) -> DispatchReport {
    let mut report = DispatchReport::default();

    // Fan-out: deliveries first, dispatched mark second, so a crash
    // between them retries into idempotent conflicts.
    match eventing::undispatched_events(store, PASS_BATCH).await {
        Ok(events) => {
            for event in events {
                let endpoints = match eventing::active_endpoints(store, &event.tenant_id).await {
                    Ok(endpoints) => endpoints,
                    Err(err) => {
                        tracing::warn!(error = %err, "webhook fan-out: endpoints");
                        continue;
                    }
                };
                let mut fanned = true;
                for endpoint in endpoints
                    .iter()
                    .filter(|endpoint| endpoint.wants(&event.action))
                {
                    match eventing::create_delivery(
                        store,
                        &event.tenant_id,
                        &event.event_id(),
                        &endpoint.endpoint_id(),
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "webhook fan-out: delivery");
                            fanned = false;
                        }
                    }
                }
                if fanned {
                    match eventing::mark_dispatched(store, &event.event_id()).await {
                        Ok(true) => report.events_dispatched += 1,
                        Ok(false) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "webhook fan-out: mark");
                        }
                    }
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "webhook fan-out: list"),
    }

    // Delivery: claim, sign, post, settle.
    match eventing::due_deliveries(store, PASS_BATCH).await {
        Ok(deliveries) => {
            for delivery in deliveries {
                match eventing::claim_delivery(
                    store,
                    &delivery.delivery_id(),
                    instance_id,
                    CLAIM_LEASE_SECS,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(err) => {
                        tracing::warn!(error = %err, "webhook claim");
                        continue;
                    }
                }
                match attempt_delivery(store, cipher, http, &delivery, allow_private).await {
                    Ok(status) => {
                        report.delivered += 1;
                        crate::metrics::incr(
                            "copal_webhook_deliveries_total{outcome=\"delivered\"}",
                        );
                        tracing::debug!(status, delivery = %delivery.delivery_id(), "delivered");
                    }
                    Err(AttemptOutcome::Retry(status)) => {
                        report.retried += 1;
                        crate::metrics::incr("copal_webhook_deliveries_total{outcome=\"retry\"}");
                        let _ = eventing::record_attempt_failure(
                            store,
                            &delivery.delivery_id(),
                            delivery.attempts,
                            status,
                        )
                        .await;
                    }
                    Err(AttemptOutcome::Terminal(reason)) => {
                        report.failed += 1;
                        crate::metrics::incr("copal_webhook_deliveries_total{outcome=\"failed\"}");
                        tracing::warn!(reason, delivery = %delivery.delivery_id(), "terminal");
                        // Jump straight past the cap: the endpoint or
                        // event is gone, retrying cannot help.
                        let _ = eventing::record_attempt_failure(
                            store,
                            &delivery.delivery_id(),
                            eventing::MAX_ATTEMPTS - 1,
                            None,
                        )
                        .await;
                    }
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "webhook due list"),
    }

    report
}

/// Why an attempt did not settle as delivered.
enum AttemptOutcome {
    /// Transport or HTTP failure; backoff applies.
    Retry(Option<i64>),
    /// The delivery can never succeed.
    Terminal(&'static str),
}

/// Sign and post one claimed delivery.
async fn attempt_delivery(
    store: &Store,
    cipher: &BlobCipher,
    http: &reqwest::Client,
    delivery: &eventing::DeliveryRow,
    allow_private: bool,
) -> Result<i64, AttemptOutcome> {
    let Some(event_id) = delivery.event_id() else {
        return Err(AttemptOutcome::Terminal("delivery has no event link"));
    };
    let Some(endpoint_id) = delivery.endpoint_id() else {
        return Err(AttemptOutcome::Terminal("delivery has no endpoint link"));
    };
    let event = match eventing::fetch_event(store, &event_id).await {
        Ok(Some(event)) => event,
        Ok(None) => return Err(AttemptOutcome::Terminal("event row is gone")),
        Err(_) => return Err(AttemptOutcome::Retry(None)),
    };
    let endpoint = match eventing::fetch_endpoint(store, &endpoint_id).await {
        Ok(Some(endpoint)) if endpoint.active => endpoint,
        Ok(_) => return Err(AttemptOutcome::Terminal("endpoint gone or inactive")),
        Err(_) => return Err(AttemptOutcome::Retry(None)),
    };
    let secret = open_secret(cipher, &endpoint.secret_sealed)
        .ok_or(AttemptOutcome::Terminal("secret does not open"))?;
    // Re-check at delivery: DNS answers change between registration
    // and now, and a rebinding answer would otherwise be followed.
    if !allow_private && crate::netguard::check_outbound_url(&endpoint.target_url).is_err() {
        return Err(AttemptOutcome::Terminal(
            "endpoint resolves to a non-public address",
        ));
    }

    let body = json!({
        "id": event.event_id(),
        "event": event.action,
        "tenant": event.tenant_id,
        "file": event.file_id(),
        "payload": event.payload,
        "created_at": event.created_at,
    });
    let bytes = serde_json::to_vec(&body).map_err(|_| AttemptOutcome::Retry(None))?;
    let signature = sign_body(&secret, &bytes);

    let response = http
        .post(&endpoint.target_url)
        .header("content-type", "application/json")
        .header("x-copal-event", event.action.as_str())
        .header("x-copal-delivery", delivery.delivery_id())
        .header("x-copal-signature", signature)
        .body(bytes)
        .send()
        .await;
    match response {
        Ok(response) => {
            let status = response.status().as_u16() as i64;
            if response.status().is_success() {
                let _ = eventing::complete_delivery(store, &delivery.delivery_id(), status).await;
                Ok(status)
            } else {
                Err(AttemptOutcome::Retry(Some(status)))
            }
        }
        Err(_) => Err(AttemptOutcome::Retry(None)),
    }
}

fn open_secret(cipher: &BlobCipher, sealed_b64: &str) -> Option<String> {
    let sealed = base64::engine::general_purpose::STANDARD
        .decode(sealed_b64)
        .ok()?;
    let bytes = cipher.open(&sealed).ok()?;
    String::from_utf8(bytes).ok()
}

/// The delivery signature: `sha256=` plus the hex HMAC of the exact
/// body bytes under the endpoint secret.
pub fn sign_body(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// The dispatcher loop: drain passes, then sleep on the outbox live
/// query with a slow fallback tick. The watch stream ending (a dropped
/// connection) rebuilds the subscription.
pub async fn run_forever(
    store: Store,
    cipher: BlobCipher,
    instance_id: String,
    allow_private: bool,
) {
    let http = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
        // A redirect would reach an address the guard never checked.
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(http) => http,
        Err(err) => {
            tracing::error!(error = %err, "webhook dispatcher: http client");
            return;
        }
    };
    loop {
        let mut wake = match store.watch(EVENT_TABLE).await {
            Ok(stream) => Some(stream),
            Err(err) => {
                tracing::warn!(error = %err, "outbox watch unavailable; ticking instead");
                None
            }
        };
        loop {
            loop {
                let report = run_pass(&store, &cipher, &http, &instance_id, allow_private).await;
                if report.idle() {
                    break;
                }
                tracing::debug!(?report, "webhook pass");
            }
            match &mut wake {
                Some(stream) => {
                    let tick = tokio::time::sleep(std::time::Duration::from_secs(30));
                    tokio::select! {
                        item = stream.next() => {
                            if item.is_none() {
                                break;
                            }
                        }
                        () = tick => {}
                    }
                }
                None => {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                    break;
                }
            }
        }
    }
}
