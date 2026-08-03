//! The contract-first REST face, mounted under `/v1c`.
//!
//! Janus's `RestRouter` derives its route table from the contract
//! and answers through the same dispatcher the GraphQL and MCP faces
//! use, so everything served here exists because the declaration
//! says so. The hand-written `/v1` routes stay canonical (they carry
//! REST-specific semantics like 201/202 and the byte faces); `/v1c`
//! is the generated twin, and the parity test holds the two to the
//! same answers.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;

use copal_blob::BlobStore;

use crate::app::AppState;

/// Serve one `/v1c/*` request through the contract router.
pub async fn serve<B: BlobStore>(
    State(state): State<AppState<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // The same identity path every face uses; the dispatcher then
    // enforces scopes, budgets, and guards.
    let (tenant, identity) = match crate::auth::authenticate_with_identity(&state, &headers).await {
        Ok(pair) => pair,
        Err(e) => return e.into_response(),
    };
    let ctx = match crate::mcp::seeded_context(&state, &headers, &tenant, identity).await {
        Ok(ctx) => ctx,
        Err((_, message)) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response();
        }
    };
    let dispatcher = match crate::graphql::dispatcher(state.clone()) {
        Ok(dispatcher) => dispatcher,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };
    let router = janus::runtime::RestRouter::new(dispatcher);

    // The contract's paths carry the canonical /v1 prefix; this face
    // serves them under /v1c, so the prefix swaps before matching.
    let path = uri.path().replacen("/v1c", "/v1", 1);
    let query = uri.query().unwrap_or_default();
    let parsed = if body.is_empty() {
        None
    } else {
        match serde_json::from_slice(&body) {
            Ok(value) => Some(value),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "body is not JSON" })),
                )
                    .into_response();
            }
        }
    };

    let answer = router
        .handle(method.as_str(), &path, query, parsed, ctx)
        .await;
    let status = StatusCode::from_u16(answer.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(answer.body)).into_response()
}
