//! The MCP face: the contract, served to agents.
//!
//! `tools/list` is the manifest Kayak generates from the contract,
//! and `tools/call` dispatches through the same chain every other
//! face uses, so scopes, budgets, guards, and caller-bound engine
//! sessions enforce identically whether the caller is a person's
//! script or an agent's tool call. Nothing here is a wrapper anyone
//! maintains: a contract change regenerates the manifest, the drift
//! gate refuses until it is blessed, and the differ names what
//! changed.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Map, Value};

use copal_core::TenantId;
use kayak::runtime::{ActionArgs, GetArgs, ListArgs, QueryArgs, SortDirection};

use copal_blob::BlobStore;

use crate::app::AppState;

/// What one tool name means, resolved once from the contract with
/// exactly the naming the manifest generator uses.
#[derive(Debug, Clone)]
enum Route {
    List { resource: String },
    Get { resource: String },
    Action { resource: String, action: String },
    Query { name: String },
}

fn routes() -> &'static BTreeMap<String, Route> {
    static ROUTES: OnceLock<BTreeMap<String, Route>> = OnceLock::new();
    ROUTES.get_or_init(|| {
        let contract = crate::contract::contract();
        let mut map = BTreeMap::new();
        for resource in &contract.resources {
            let singular = kayak_singular(&resource.name);
            map.insert(
                format!("{}_list", resource.name),
                Route::List {
                    resource: resource.name.clone(),
                },
            );
            map.insert(
                format!("{singular}_get"),
                Route::Get {
                    resource: resource.name.clone(),
                },
            );
            for action in &resource.actions {
                map.insert(
                    format!("{singular}_{}", action.name),
                    Route::Action {
                        resource: resource.name.clone(),
                        action: action.name.clone(),
                    },
                );
            }
        }
        for query in &contract.queries {
            map.insert(
                query.name.clone(),
                Route::Query {
                    name: query.name.clone(),
                },
            );
        }
        map
    })
}

fn kayak_singular(name: &str) -> String {
    // The manifest generator uses kayak::naming::singular; the
    // contract's resource names are regular plurals, and the tool
    // router must agree with the manifest byte for byte, which the
    // parity test holds.
    name.strip_suffix('s').unwrap_or(name).to_owned()
}

fn manifest() -> &'static Value {
    static MANIFEST: OnceLock<Value> = OnceLock::new();
    MANIFEST.get_or_init(|| {
        // Generation refuses on an invalid contract. The drift gate in
        // tests/contract.rs fails first on any contract this would refuse,
        // so reaching the panic means serving a build the gate never passed.
        kayak::generate_mcp_tools(&crate::contract::contract())
            .expect("the contract validates; the drift gate enforces it")
    })
}

/// The version of the protocol this face speaks.
const PROTOCOL: &str = "2025-06-18";

/// `POST /mcp`: JSON-RPC 2.0 over HTTP, the MCP streamable shape
/// without the optional stream.
pub async fn mcp_endpoint<B: BlobStore>(
    State(state): State<AppState<B>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> axum::response::Response {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();

    // Notifications carry no id and expect no body.
    if id.is_null() && method.starts_with("notifications/") {
        return axum::http::StatusCode::ACCEPTED.into_response();
    }

    crate::metrics::incr("copal_mcp_calls_total");
    let result = handle(&state, &headers, &method, request.get("params")).await;
    if result.is_err() {
        crate::metrics::incr("copal_mcp_errors_total");
    }
    let body = match result {
        Ok(value) => json!({ "jsonrpc": "2.0", "id": id, "result": value }),
        Err((code, message)) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message },
        }),
    };
    Json(body).into_response()
}

async fn handle<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    method: &str,
    params: Option<&Value>,
) -> Result<Value, (i64, String)> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "copal", "version": env!("CARGO_PKG_VERSION") },
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(manifest().clone()),
        "tools/call" => {
            let params = params.ok_or((-32602, "tools/call takes params".to_owned()))?;
            let name = params
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or((-32602, "tools/call needs a tool name".to_owned()))?;
            let arguments = params
                .get("arguments")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            call_tool(state, headers, name, arguments).await
        }
        other => Err((-32601, format!("method {other:?} is not served"))),
    }
}

async fn call_tool<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    name: &str,
    mut arguments: Map<String, Value>,
) -> Result<Value, (i64, String)> {
    let route = routes()
        .get(name)
        .ok_or((-32602, format!("no tool named {name:?}")))?
        .clone();

    // The same identity path every face uses; the dispatcher then
    // enforces scopes, budgets, and guards, and resolvers pick the
    // caller-bound engine session out of the context.
    let (tenant, identity) = crate::auth::authenticate_with_identity(state, headers)
        .await
        .map_err(|e| (-32000, e.0.to_string()))?;
    let ctx = seeded_context(state, headers, &tenant, identity).await?;

    let dispatcher =
        crate::graphql::dispatcher(state.clone()).map_err(|e| (-32000, e.to_string()))?;

    let value = match route {
        Route::List { resource } => {
            let limit = arguments
                .remove("limit")
                .and_then(|v| v.as_i64())
                .unwrap_or(50)
                .clamp(1, 1_000) as u32;
            let cursor = arguments
                .remove("cursor")
                .and_then(|v| v.as_str().map(str::to_owned));
            let sort = arguments
                .remove("sort")
                .and_then(|v| v.as_str().map(str::to_owned))
                .map(|raw| match raw.split_once(":desc") {
                    Some((column, _)) => (column.to_owned(), SortDirection::Desc),
                    None => (raw, SortDirection::Asc),
                });
            let filters: BTreeMap<String, Value> = arguments.into_iter().collect();
            let output = dispatcher
                .list(
                    &resource,
                    ctx,
                    ListArgs {
                        limit,
                        cursor,
                        filters,
                        sort,
                    },
                )
                .await
                .map_err(kayak_to_rpc)?;
            json!({ "items": output.items, "next_cursor": output.next_cursor })
        }
        Route::Get { resource } => {
            let id = arguments
                .remove("id")
                .and_then(|v| v.as_str().map(str::to_owned))
                .ok_or((-32602, "id is required".to_owned()))?;
            let row = dispatcher
                .get(&resource, ctx, GetArgs { id })
                .await
                .map_err(kayak_to_rpc)?;
            row.unwrap_or(Value::Null)
        }
        Route::Action { resource, action } => {
            let id = arguments
                .remove("id")
                .and_then(|v| v.as_str().map(str::to_owned));
            let args = ActionArgs {
                id,
                input: arguments.into_iter().collect(),
            };
            let value = dispatcher
                .action(&resource, &action, ctx, args)
                .await
                .map_err(kayak_to_rpc)?;
            value.unwrap_or(json!({ "ok": true }))
        }
        Route::Query { name } => {
            let args = QueryArgs {
                input: arguments.into_iter().collect(),
            };
            dispatcher
                .query(&name, ctx, args)
                .await
                .map_err(kayak_to_rpc)?
        }
    };

    Ok(json!({
        "content": [ { "type": "text", "text": value.to_string() } ],
        "isError": false,
    }))
}

pub(crate) async fn seeded_context<B: BlobStore>(
    state: &AppState<B>,
    headers: &HeaderMap,
    tenant: &TenantId,
    identity: Option<crate::auth::KeyIdentity>,
) -> Result<kayak::runtime::KayakContext, (i64, String)> {
    let mut ctx = kayak::runtime::KayakContext::new();
    ctx.insert(crate::graphql::Tenant(tenant.clone()));
    if state.engine_sessions {
        let store = crate::auth::request_store(state, tenant, identity.as_ref())
            .await
            .map_err(|e| (-32000, e.0.to_string()))?;
        ctx.insert(store);
    }
    if let Some(key) = identity {
        let subject = key.principal.clone().unwrap_or_else(|| key.key_id.clone());
        ctx.insert(kayak::runtime::Principal::new(subject, key.scopes));
    }
    if let Some(origin) = crate::app::forwarded_origin(headers) {
        ctx.insert(crate::graphql::RequestOrigin(origin));
    }
    Ok(ctx)
}

fn kayak_to_rpc(err: kayak::runtime::KayakError) -> (i64, String) {
    (-32000, err.to_string())
}

/// The tool router and the generated manifest must agree byte for
/// byte on names; this is the parity the module doc promises.
#[cfg(test)]
fn assert_route_parity() {
    let manifest = manifest();
    let names: Vec<&str> = manifest["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for name in names {
        assert!(routes().contains_key(name), "unrouted tool {name}");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_manifest_tool_routes() {
        super::assert_route_parity();
        let route_count = super::routes().len();
        let manifest_count = super::manifest()["tools"].as_array().unwrap().len();
        assert_eq!(route_count, manifest_count, "no stray routes either");
    }
}
