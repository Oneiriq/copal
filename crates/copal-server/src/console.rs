//! The operator console: kayak's contract-driven pages mounted on
//! the admin surface, plus the deployment panels only copal can
//! know.
//!
//! Browsers cannot send `x-copal-admin-token`, so the console
//! authenticates with HTTP Basic: any username, the admin token as
//! the password, compared the same constant-time way the header is,
//! previous token honored. The console therefore exists only when
//! an admin token is configured, and it rides the admin router, so
//! a split `COPAL_ADMIN_BIND` keeps it off the tenant network.
//!
//! The deployment home lists tenants and tails the audit trail.
//! Everything under `/t/{tenant}/` is the kayak console for that
//! tenant: the operator acts as the tenant with full scopes through
//! the same dispatcher every API face uses, so what the console
//! shows and refuses is what the API shows and refuses.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Redirect, Response};
use maud::{html, Markup, PreEscaped};

use copal_blob::BlobStore;
use copal_core::TenantId;

use crate::app::AppState;

/// The console's look belongs to kayak, which generates every other
/// page here. A second copy is a second console: this page kept one,
/// so it went on printing raw byte counts and nanosecond timestamps
/// after the generated pages stopped.
use kayak::runtime::{cell, document, rail_section, Page};
use serde_json::Value;

/// Constant-time Basic check against the admin token (previous
/// honored), answering with whoever got through. `Err` is the
/// challenge or refusal to return.
#[allow(clippy::result_large_err)]
fn gate<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
) -> Result<crate::auth::Operator, Response> {
    // The realm reaches a browser's credential dialog, and the
    // username never does anything, so it says so where it will be
    // read.
    let challenge = || {
        Err((
            StatusCode::UNAUTHORIZED,
            [(
                "www-authenticate",
                "Basic realm=\"copal console: any username, admin token as password\"",
            )],
            "the console takes the admin token as the Basic password",
        )
            .into_response())
    };
    let Some(configured) = state.auth.admin_token.as_deref() else {
        return Err((
            StatusCode::NOT_FOUND,
            "the console exists only when an admin token is configured",
        )
            .into_response());
    };
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|b64| {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.decode(b64).ok()
        })
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .and_then(|pair| pair.split_once(':').map(|(_, pass)| pass.to_owned()));
    let Some(presented) = presented else {
        return challenge();
    };
    let current = copal_sign::verify_secret(&presented, &copal_sign::hash_secret(configured));
    let previous = match state.auth.admin_token_previous.as_deref() {
        Some(prior) => copal_sign::verify_secret(&presented, &copal_sign::hash_secret(prior)),
        None => false,
    };
    if !(current || previous) {
        return challenge();
    }
    crate::auth::operator(state, headers).map_err(IntoResponse::into_response)
}

/// The operator acts as the tenant with every scope and the root
/// store: the dispatcher still enforces guards and validation, so
/// the console sees what an all-scoped caller of that tenant sees.
fn operator_context(
    tenant: &TenantId,
    operator: &crate::auth::Operator,
) -> kayak::runtime::KayakContext {
    let mut ctx = kayak::runtime::KayakContext::new();
    ctx.insert(crate::graphql::Tenant(tenant.clone()));
    ctx.insert(kayak::runtime::Principal::new(
        operator.as_str().to_owned(),
        vec![
            "read".to_owned(),
            "write".to_owned(),
            "admin".to_owned(),
            "search".to_owned(),
        ],
    ));
    ctx
}

fn console_router<B: BlobStore>(
    state: &AppState<B>,
    tenant: &TenantId,
) -> Result<kayak::runtime::ConsoleRouter, crate::error::ApiError> {
    let dispatcher = crate::graphql::dispatcher(state.clone())
        .map_err(|e| crate::error::ApiError::from(copal_core::CopalError::Store(e.to_string())))?;
    Ok(kayak::runtime::ConsoleRouter::new(
        dispatcher,
        kayak::runtime::ConsoleConfig {
            base: format!("/admin/console/t/{}", tenant.as_str()),
            title: format!("copal · {}", tenant.as_str()),
        },
    )
    .with_schema(copal_store::schema::tables()))
}

/// GET `/admin/console`: the deployment home.
pub async fn home<B: BlobStore>(State(state): State<AppState<B>>, headers: HeaderMap) -> Response {
    let operator = match gate(&state, &headers) {
        Ok(operator) => operator,
        Err(refused) => return refused,
    };
    let tenants = copal_store::repo::tenant::known_tenants(&state.store)
        .await
        .unwrap_or_default();
    let audit = copal_store::repo::auth::export_audit_page(&state.store, None, 25, None)
        .await
        .unwrap_or_default();
    let fleet = match &state.fleet {
        Some(cfg) => Some(
            copal_store::fleet::overview(cfg, 12)
                .await
                .map_err(|e| e.to_string()),
        ),
        None => None,
    };
    let body = html! {
        h1 { "Deployment" }
        p.dim { "signed in as " (operator) }
        h2 { "Tenants" }
        @if tenants.is_empty() { p.dim { "no tenant has stored anything yet" } }
        table {
            thead { tr { th { "Tenant" } th { "Files" } th { "Bytes" } } }
            tbody {
                @for row in &tenants {
                    tr {
                        td {
                            @if let Some(id) = row.get("tenant_id").and_then(|v| v.as_str()) {
                                a href=(format!("/admin/console/t/{id}")) { (id) }
                            }
                        }
                        td { (cell("files", row.get("files"))) }
                        td { (cell("bytes", row.get("bytes"))) }
                    }
                }
            }
        }
        @if let Some(fleet) = &fleet {
            h2 { "Fleet" }
            @match fleet {
                Ok(namespaces) => {
                    @for ns in namespaces {
                        h2.dim { "ns " (ns.name) }
                        @for db in &ns.databases {
                            table {
                                thead {
                                    tr { th { (ns.name) "/" (db.name) } th { "Rows" } }
                                }
                                tbody {
                                    @for table in &db.tables {
                                        tr {
                                            td { (table.name) }
                                            td {
                                                @match table.rows {
                                                    Some(rows) => (rows),
                                                    None => span.dim { "·" },
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(reason) => { p.dim { (reason) } }
            }
        }
        h2 { "Audit tail" }
        table {
            thead { tr { th { "At" } th { "Tenant" } th { "Actor" } th { "Action" } th { "Subject" } } }
            tbody {
                @for row in audit.iter().rev() {
                    tr {
                        td.dim { (cell("created_at", row.get("created_at"))) }
                        td { (row.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("")) }
                        td { (row.get("actor").and_then(|v| v.as_str()).unwrap_or("")) }
                        td { (row.get("action").and_then(|v| v.as_str()).unwrap_or("")) }
                        td.dim { (row.get("subject").and_then(|v| v.as_str()).unwrap_or("")) }
                    }
                }
            }
        }
    };
    page("Deployment", &tenants, body)
}

/// GET `/admin/console/t/{tenant}` and everything under it: the
/// kayak console for that tenant.
pub async fn tenant_pages<B: BlobStore>(
    State(state): State<AppState<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path((tenant, rest)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let operator = match gate(&state, &headers) {
        Ok(operator) => operator,
        Err(refused) => return refused,
    };
    let tenant = match TenantId::parse(&tenant) {
        Ok(tenant) => tenant,
        Err(e) => return crate::error::ApiError::from(e).into_response(),
    };
    let router = match console_router(&state, &tenant) {
        Ok(router) => router,
        Err(e) => return e.into_response(),
    };
    let ctx = operator_context(&tenant, &operator);
    if method == Method::POST {
        let pairs: Vec<(String, String)> = serde_urlencoded::from_bytes(&body).unwrap_or_default();
        match router.submit(&rest, &pairs, ctx).await {
            kayak::runtime::FormOutcome::Redirect(target) => Redirect::to(&target).into_response(),
            kayak::runtime::FormOutcome::Page(answer) => {
                let status = StatusCode::from_u16(answer.status)
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                (status, Html(answer.html)).into_response()
            }
        }
    } else {
        let answer = router
            .page(&rest, uri.query().unwrap_or_default(), ctx)
            .await;
        let status =
            StatusCode::from_u16(answer.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Html(answer.html)).into_response()
    }
}

/// GET `/admin/console/t/{tenant}` without a trailing segment.
pub async fn tenant_home<B: BlobStore>(
    state: State<AppState<B>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(tenant): Path<String>,
    body: Bytes,
) -> Response {
    tenant_pages(
        state,
        method,
        uri,
        headers,
        Path((tenant, String::new())),
        body,
    )
    .await
}

fn page(title: &str, tenants: &[Value], content: Markup) -> Response {
    let rail = html! {
        (rail_section("Deployment", &[(
            "Overview".to_owned(),
            "/admin/console".to_owned(),
            true,
        )]))
        @if !tenants.is_empty() {
            (rail_section(
                "Tenants",
                &tenants
                    .iter()
                    .filter_map(|row| row.get("tenant_id").and_then(Value::as_str))
                    .map(|id| (
                        id.to_owned(),
                        format!("/admin/console/t/{id}"),
                        false,
                    ))
                    .collect::<Vec<_>>(),
            ))
        }
    };
    let footer = html! {
        span { "copal" }
        span.dim { "Admin surface" }
    };
    let document = PreEscaped(document(
        Page {
            brand: "copal",
            home: "/admin/console",
            title,
        },
        rail,
        content,
        footer,
    ));
    Html(document.into_string()).into_response()
}
