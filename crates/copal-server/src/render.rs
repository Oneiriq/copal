//! The render listener: the one place Copal displays content instead
//! of handing it over as a download.
//!
//! Script-capable types (HTML, SVG, XML) always download on the
//! service's own origin, so an upload can never run where tenant
//! credentials live. A render link is a grant whose operation is
//! `render`, redeemable only here, on a listener of its own
//! (`COPAL_RENDER_BIND`) that serves nothing else. Every response is
//! sandboxed by [`crate::serve::render_sandbox`]: the page runs in an
//! opaque origin with no network access of its own.
//!
//! Give the listener its own registrable domain in production, the
//! way hosted-content services keep user pages off their main domain.
//! Separate ports on one host are separate origins, which suffices for
//! development, but cookies are scoped by host, not port.

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use axum::Router;

use copal_blob::BlobStore;

use crate::app::AppState;
use crate::error::ApiError;

/// The whole render surface: render-link and short-link redemption and
/// nothing else. Every other path on this listener is a 404.
pub fn render_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    Router::new()
        .route("/v1/render/{grant_ref}", get(redeem_render::<B>))
        .route("/a/{link}", get(redeem_short::<B>))
        .with_state(state)
}

/// Redeem a short link, `/a/<name>-<code>`: the typeable address of a
/// render grant, answered exactly as the grant's own link would be.
async fn redeem_short<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(link): Path<String>,
) -> Result<Response, ApiError> {
    let response = crate::app::redeem_short_link(&state, &headers, &link).await?;
    Ok(crate::serve::render_sandbox(response))
}

/// Redeem a render link. Refusals are the uniform 404 every grant
/// family answers; success is the file displayed inside the sandbox.
async fn redeem_render<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Path(grant_ref): Path<String>,
) -> Result<Response, ApiError> {
    let response = crate::app::redeem_read_grant(&state, &headers, &grant_ref, "render").await?;
    Ok(crate::serve::render_sandbox(response))
}
