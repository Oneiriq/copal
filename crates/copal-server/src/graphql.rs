//! The GraphQL face, served by Janus from THE contract.
//!
//! Nothing here restates the API: the schema is built dynamically from
//! [`crate::contract::contract`] and every field dispatches through the
//! Janus runtime; resolvers below call the same repositories the REST
//! handlers call, and the same wire mapper renders rows, so the two
//! protocols cannot diverge. Tenancy is a Janus middleware: the HTTP
//! layer seeds the per-request context through the SAME authenticator
//! REST uses (trusted header or `ck1` bearer key, per configuration),
//! and [`RequireTenant`] fails closed when identity is missing.

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};

use copal_blob::BlobStore;
use copal_core::{CopalError, FileId, FileState, TenantId};
use copal_store::repo::{file as file_repo, flow as flow_repo};

use futures::StreamExt as _;
use janus::runtime::{
    BoxFuture, Dispatcher, JanusContext, JanusError, ListOutput, Middleware, Next, Operation,
    Outcome, Payload, Resolvers, RowStream, SortDirection,
};

use crate::app::{
    decode_cursor, decode_run_cursor, encode_cursor, encode_run_cursor, issue_grant_core,
    retry_run_core, start_run_core, AppState, StartRunRequest,
};
use crate::wire::{wire_event, wire_file, wire_run};

/// The per-request tenant, seeded from the transport by the HTTP layer.
#[derive(Debug, Clone)]
pub struct Tenant(pub TenantId);

/// The proxy-forwarded client origin, seeded for audit forensics.
#[derive(Debug, Clone)]
pub struct RequestOrigin(pub String);

/// Every contract operation requires a tenant; fail closed without one.
struct RequireTenant;

impl Middleware for RequireTenant {
    fn handle<'a>(
        &'a self,
        operation: Operation,
        ctx: JanusContext,
        payload: Payload,
        next: Next,
    ) -> BoxFuture<'a, Result<Outcome, JanusError>> {
        Box::pin(async move {
            if ctx.get::<Tenant>().is_none() {
                return Err(JanusError::Unauthorized(
                    "no tenant identity on the request (missing or invalid credentials)".into(),
                ));
            }
            next.run(operation, ctx, payload).await
        })
    }
}

fn tenant_of(ctx: &JanusContext) -> Result<TenantId, JanusError> {
    ctx.get::<Tenant>().map(|t| t.0.clone()).ok_or_else(|| {
        JanusError::Unauthorized(
            "no tenant identity on the request (missing or invalid credentials)".into(),
        )
    })
}

/// Domain errors in runtime vocabulary; infrastructure detail stays in
/// the log, mirroring the REST error mapper.
fn to_janus_error(err: CopalError) -> JanusError {
    match err {
        CopalError::Validation(m) => JanusError::BadRequest(m),
        CopalError::Unauthorized(m) => JanusError::Unauthorized(m),
        CopalError::Forbidden(m) => JanusError::Forbidden(m),
        CopalError::NotFound(_) => JanusError::NotFound,
        CopalError::Conflict(m) => JanusError::Conflict(m),
        CopalError::PayloadTooLarge(m) => JanusError::BadRequest(m),
        CopalError::Store(_) | CopalError::Blob(_) => {
            tracing::error!(error = %err, "internal failure");
            JanusError::Internal("internal error".into())
        }
    }
}

/// Parse a wire state name through the enum's own serde names.
fn parse_state(raw: &str) -> Result<FileState, JanusError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned()))
        .map_err(|_| JanusError::BadRequest(format!("unknown state {raw:?}")))
}

fn parse_file_id(raw: &str) -> Result<FileId, JanusError> {
    FileId::parse(raw).map_err(|e| JanusError::BadRequest(e.to_string()))
}

/// Build the Janus dispatcher over this state: the resolvers are thin
/// closures over the same repositories the REST handlers use.
fn dispatcher<B: BlobStore + 'static>(
    state: AppState<B>,
) -> Result<Arc<Dispatcher>, janus::runtime::RuntimeBuildError> {
    let list_state = state.clone();
    let get_state = state.clone();
    let url_state = state.clone();
    let upload_url_state = state.clone();
    let rendition_state = state.clone();
    let edge_url_state = state.clone();
    let hooks_list_state = state.clone();
    let hooks_get_state = state.clone();
    let hooks_register_state = state.clone();
    let hooks_remove_state = state.clone();
    let remove_state = state.clone();
    let events_list_state = state.clone();
    let events_get_state = state.clone();
    let events_watch_state = state.clone();
    let versions_state = state.clone();
    let deliveries_state = state.clone();
    let runs_list_state = state.clone();
    let runs_get_state = state.clone();
    let runs_start_state = state.clone();
    let runs_retry_state = state;

    let resolvers = Resolvers::new()
        .list("files", move |ctx, args| {
            let state = list_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let ascending = matches!(&args.sort, Some((_, SortDirection::Asc)));
                let after = args
                    .cursor
                    .as_deref()
                    .map(|raw| decode_cursor(raw, ascending))
                    .transpose()
                    .map_err(|e| JanusError::BadRequest(e.0.to_string()))?;
                let state_filter = args
                    .filters
                    .get("state")
                    .map(|v| parse_state(v.as_str().unwrap_or_default()))
                    .transpose()?;
                let limit = i64::from(args.limit);
                let records = file_repo::list_files(
                    &state.store,
                    &tenant,
                    limit,
                    after.as_ref(),
                    ascending,
                    state_filter,
                )
                .await
                .map_err(to_janus_error)?;
                let next_cursor = if records.len() as i64 == limit {
                    records.last().map(|last| {
                        encode_cursor(
                            &file_repo::ListPosition {
                                created_at: last.created_at.clone(),
                                id: last.id.clone(),
                            },
                            ascending,
                        )
                    })
                } else {
                    None
                };
                Ok(ListOutput {
                    items: records.iter().map(wire_file).collect(),
                    next_cursor,
                })
            }
        })
        .get("files", move |ctx, args| {
            let state = get_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(&args.id)?;
                let record = file_repo::get_file(&state.store, &tenant, &id)
                    .await
                    .map_err(to_janus_error)?;
                Ok(record.as_ref().map(wire_file))
            }
        })
        .action("files", "issue_url", move |ctx, args| {
            let state = url_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(args.id.as_deref().unwrap_or_default())?;
                let ttl_secs = args
                    .input
                    .get("ttl_secs")
                    .and_then(|v| v.as_u64())
                    .and_then(|v| u32::try_from(v).ok())
                    .unwrap_or(900);
                let max_uses = args
                    .input
                    .get("max_uses")
                    .and_then(|v| v.as_u64())
                    .and_then(|v| u32::try_from(v).ok());
                let origin = ctx.get::<RequestOrigin>().map(|o| o.0.clone());
                issue_grant_core(&state, &tenant, &id, ttl_secs, max_uses, origin.as_deref())
                    .await
                    .map(Some)
                    .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("files", "issue_upload_url", move |ctx, args| {
            let state = upload_url_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(args.id.as_deref().unwrap_or_default())?;
                let ttl_secs = args
                    .input
                    .get("ttl_secs")
                    .and_then(|v| v.as_u64())
                    .and_then(|v| u32::try_from(v).ok())
                    .unwrap_or(900);
                let origin = ctx.get::<RequestOrigin>().map(|o| o.0.clone());
                crate::app::issue_upload_grant_core(
                    &state,
                    &tenant,
                    &id,
                    ttl_secs,
                    origin.as_deref(),
                )
                .await
                .map(Some)
                .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("files", "issue_edge_url", move |ctx, args| {
            let state = edge_url_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(args.id.as_deref().unwrap_or_default())?;
                let ttl_secs = args
                    .input
                    .get("ttl_secs")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(900);
                let origin = ctx.get::<RequestOrigin>().map(|o| o.0.clone());
                crate::edge::issue_edge_url_core(&state, &tenant, &id, ttl_secs, origin.as_deref())
                    .await
                    .map(Some)
                    .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("files", "request_rendition", move |ctx, args| {
            let state = rendition_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(args.id.as_deref().unwrap_or_default())?;
                let spec = crate::app::RenditionSpec {
                    kind: args
                        .input
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .unwrap_or("thumb")
                        .to_owned(),
                    width: args
                        .input
                        .get("width")
                        .and_then(|v| v.as_u64())
                        .and_then(|v| u32::try_from(v).ok())
                        .unwrap_or(256),
                    height: args
                        .input
                        .get("height")
                        .and_then(|v| v.as_u64())
                        .and_then(|v| u32::try_from(v).ok())
                        .unwrap_or(256),
                    format: args
                        .input
                        .get("format")
                        .and_then(|v| v.as_str())
                        .unwrap_or("jpeg")
                        .to_owned(),
                };
                crate::app::request_rendition_core(&state, &tenant, &id, &spec)
                    .await
                    .map(|(_, body)| Some(body))
                    .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("files", "remove", move |ctx, args| {
            let state = remove_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(args.id.as_deref().unwrap_or_default())?;
                let origin = ctx.get::<RequestOrigin>().map(|o| o.0.clone());
                crate::app::remove_file_core(&state, &tenant, &id, origin.as_deref())
                    .await
                    .map_err(|e| to_janus_error(e.0))?;
                Ok(None)
            }
        })
        .list("webhooks", move |ctx, _args| {
            let state = hooks_list_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let items = copal_store::repo::eventing::list_endpoints(&state.store, &tenant)
                    .await
                    .map_err(to_janus_error)?;
                Ok(ListOutput {
                    items,
                    // Endpoints are few by nature; the page is the set.
                    next_cursor: None,
                })
            }
        })
        .get("webhooks", move |ctx, args| {
            let state = hooks_get_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let found = copal_store::repo::eventing::list_endpoints(&state.store, &tenant)
                    .await
                    .map_err(to_janus_error)?
                    .into_iter()
                    .find(|row| row.get("id").and_then(|v| v.as_str()) == Some(args.id.as_str()));
                Ok(found)
            }
        })
        .action("webhooks", "register", move |ctx, args| {
            let state = hooks_register_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let url = args
                    .input
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| JanusError::BadRequest("url is required".into()))?
                    .to_owned();
                let events = args
                    .input
                    .get("events")
                    .and_then(|v| v.as_array())
                    .map(|list| {
                        list.iter()
                            .filter_map(|v| v.as_str().map(str::to_owned))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let origin = ctx.get::<RequestOrigin>().map(|o| o.0.clone());
                crate::webhooks::register_core(&state, &tenant, &url, &events, origin.as_deref())
                    .await
                    .map(Some)
                    .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("webhooks", "remove", move |ctx, args| {
            let state = hooks_remove_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = args.id.clone().unwrap_or_default();
                let origin = ctx.get::<RequestOrigin>().map(|o| o.0.clone());
                crate::webhooks::remove_core(&state, &tenant, &id, origin.as_deref())
                    .await
                    .map_err(|e| to_janus_error(e.0))?;
                Ok(None)
            }
        })
        .sub_list("files", "versions", move |ctx, args| {
            let state = versions_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(&args.parent_id)?;
                // Tenancy and tombstone filtering ride the file fetch,
                // exactly as the REST handler does it.
                file_repo::get_file(&state.store, &tenant, &id)
                    .await
                    .map_err(to_janus_error)?
                    .ok_or_else(|| to_janus_error(CopalError::not_found(format!("file {id}"))))?;
                let (items, next_cursor) = crate::app::list_versions_page(
                    &state,
                    &tenant,
                    &id,
                    i64::from(args.limit),
                    args.cursor.as_deref(),
                )
                .await
                .map_err(|e| to_janus_error(e.0))?;
                Ok(ListOutput { items, next_cursor })
            }
        })
        .sub_list("webhooks", "deliveries", move |ctx, args| {
            let state = deliveries_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let delivery_state = args
                    .filters
                    .get("state")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);
                let items = crate::webhooks::endpoint_deliveries_page(
                    &state,
                    &tenant,
                    &args.parent_id,
                    delivery_state.as_deref(),
                    i64::from(args.limit),
                )
                .await
                .map_err(|e| to_janus_error(e.0))?;
                Ok(ListOutput {
                    items,
                    next_cursor: None,
                })
            }
        })
        .list("events", move |ctx, args| {
            let state = events_list_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let limit = i64::from(args.limit);
                let action = args.filters.get("action").and_then(|v| v.as_str());
                let rows =
                    copal_store::repo::eventing::list_events(&state.store, &tenant, action, limit)
                        .await
                        .map_err(to_janus_error)?;
                Ok(ListOutput {
                    items: rows.iter().map(wire_event).collect(),
                    // The outbox listing is newest-first and bounded;
                    // cursoring it waits for a keyset over created_at
                    // the way files and runs have.
                    next_cursor: None,
                })
            }
        })
        .watch("events", move |ctx, args| {
            let state = events_watch_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let action = args
                    .filters
                    .get("action")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);
                let rows = copal_store::repo::eventing::watch_events(
                    &state.store,
                    &tenant,
                    action.as_deref(),
                )
                .await
                .map_err(to_janus_error)?;
                // The same mapper the list face uses, so a row looks
                // identical whether it was polled or pushed.
                Ok(
                    Box::pin(
                        rows.map(|row| row.map(|row| wire_event(&row)).map_err(to_janus_error)),
                    ) as RowStream,
                )
            }
        })
        .get("events", move |ctx, args| {
            let state = events_get_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let row = copal_store::repo::eventing::fetch_event(&state.store, &args.id)
                    .await
                    .map_err(to_janus_error)?
                    .filter(|row| row.tenant_id == tenant.as_str());
                Ok(row.as_ref().map(wire_event))
            }
        })
        .list("runs", move |ctx, args| {
            let state = runs_list_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let ascending = matches!(&args.sort, Some((_, SortDirection::Asc)));
                let after = args
                    .cursor
                    .as_deref()
                    .map(|raw| decode_run_cursor(raw, ascending))
                    .transpose()
                    .map_err(|e| JanusError::BadRequest(e.0.to_string()))?;
                let status = args
                    .filters
                    .get("status")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned);
                let limit = i64::from(args.limit);
                let runs = flow_repo::list_runs(
                    &state.store,
                    &tenant,
                    limit,
                    after.as_ref(),
                    ascending,
                    status.as_deref(),
                )
                .await
                .map_err(to_janus_error)?;
                let next_cursor = if runs.len() as i64 == limit {
                    runs.last().map(|last| {
                        encode_run_cursor(
                            &flow_repo::RunListPosition {
                                created_at: last.created_at.clone(),
                                id: last.run_id(),
                            },
                            ascending,
                        )
                    })
                } else {
                    None
                };
                Ok(ListOutput {
                    items: runs.iter().map(wire_run).collect(),
                    next_cursor,
                })
            }
        })
        .get("runs", move |ctx, args| {
            let state = runs_get_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let run = flow_repo::get_run(&state.store, &tenant, &args.id)
                    .await
                    .map_err(to_janus_error)?;
                Ok(run.as_ref().map(wire_run))
            }
        })
        .action("runs", "start", move |ctx, args| {
            let state = runs_start_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let request = StartRunRequest {
                    workflow: args
                        .input
                        .get("workflow")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_owned(),
                    input: args.input.get("input").cloned().unwrap_or_default(),
                    file: args
                        .input
                        .get("file")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                    idempotency_key: args
                        .input
                        .get("idempotency_key")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                    mode: args
                        .input
                        .get("mode")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                };
                start_run_core(&state, &tenant, request)
                    .await
                    .map(|(_, body)| Some(body))
                    .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("runs", "retry", move |ctx, args| {
            let state = runs_retry_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let run_id = args.id.as_deref().unwrap_or_default();
                retry_run_core(&state, &tenant, run_id)
                    .await
                    .map(Some)
                    .map_err(|e| to_janus_error(e.0))
            }
        });

    Ok(Arc::new(Dispatcher::new(
        Arc::new(crate::contract::contract()),
        resolvers,
        vec![Arc::new(RequireTenant) as Arc<dyn Middleware>],
    )?))
}

/// Shared state for the GraphQL routes: the executable schema plus the
/// generated SDL document served for discovery, plus the app state the
/// authenticator needs.
#[derive(Clone)]
struct GraphqlState<B: BlobStore> {
    schema: async_graphql::dynamic::Schema,
    sdl: Arc<String>,
    app: AppState<B>,
}

/// Build the `/graphql` router: POST executes, GET serves the SDL.
///
/// Panics only on a contract/schema mismatch or missing resolver,
/// construction-time bugs the contract tests catch first.
pub fn graphql_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    let tables = copal_store::schema::tables();
    // Depth and complexity ceilings close the alias-amplification hole
    // (N aliases of files(limit: 100) multiplying into the store). The
    // schema has no cycles, so honest queries sit far below both.
    // Introspection stays on by choice: GET /graphql serves the SDL
    // openly, so introspection reveals nothing the contract does not.
    let schema = janus::runtime::graphql::schema_builder(
        &tables,
        dispatcher(state.clone()).expect("resolver completeness"),
    )
    .expect("contract builds a schema")
    .limit_depth(10)
    .limit_complexity(500)
    .finish()
    .expect("schema finishes");
    let sdl =
        janus::generate_sdl(&crate::contract::contract(), &tables).expect("contract generates SDL");
    let gql = GraphqlState {
        schema,
        sdl: Arc::new(sdl),
        app: state,
    };
    Router::new()
        .route("/graphql", post(execute::<B>).get(sdl_document::<B>))
        .with_state(gql)
}

/// The GraphQL-over-HTTP request body.
#[derive(serde::Deserialize)]
struct GraphqlRequest {
    query: String,
    #[serde(default)]
    variables: Option<serde_json::Value>,
    #[serde(default, rename = "operationName")]
    operation_name: Option<String>,
}

async fn execute<B: BlobStore>(
    State(gql): State<GraphqlState<B>>,
    headers: HeaderMap,
    Json(body): Json<GraphqlRequest>,
) -> axum::response::Response {
    let mut request = async_graphql::Request::new(body.query);
    if let Some(variables) = body.variables {
        request = request.variables(async_graphql::Variables::from_json(variables));
    }
    if let Some(operation) = body.operation_name {
        request = request.operation_name(operation);
    }

    // Seed the per-request context through the SAME authenticator the
    // REST face uses. A failed authentication seeds nothing, and the
    // RequireTenant middleware rejects each operation with the coded
    // error; GraphQL convention keeps auth failures in the body.
    let mut ctx = JanusContext::new();
    if let Ok(tenant) = crate::auth::authenticate(&gql.app, &headers).await {
        ctx.insert(Tenant(tenant));
    }
    if let Some(origin) = crate::app::forwarded_origin(&headers) {
        ctx.insert(RequestOrigin(origin));
    }
    let request = request.data(ctx);

    if wants_event_stream(&headers) {
        return stream_response(gql.schema.execute_stream(request));
    }
    Json(serde_json::to_value(gql.schema.execute(request).await).expect("graphql serializes"))
        .into_response()
}

/// Whether the caller asked for the streaming transport.
fn wants_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("text/event-stream"))
}

/// Render a GraphQL response stream as graphql-sse in distinct
/// connections mode: one `next` event per payload, then `complete`.
///
/// Subscriptions ride the SAME route and the SAME authenticator as
/// every other operation, which is why this is server-sent events
/// instead of a WebSocket. A browser cannot set headers on a WebSocket
/// handshake, so graphql-ws carries credentials in its own init
/// payload; that would be a second authentication path to keep correct.
fn stream_response(
    stream: impl futures::Stream<Item = async_graphql::Response> + Send + 'static,
) -> axum::response::Response {
    let events = stream
        .map(|response| {
            let payload = serde_json::to_string(&response).expect("graphql response serializes");
            Ok::<_, std::convert::Infallible>(
                axum::response::sse::Event::default()
                    .event("next")
                    .data(payload),
            )
        })
        .chain(futures::stream::once(async {
            Ok(axum::response::sse::Event::default()
                .event("complete")
                .data(""))
        }));
    axum::response::Sse::new(events)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}

async fn sdl_document<B: BlobStore>(State(gql): State<GraphqlState<B>>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/graphql")],
        gql.sdl.as_str().to_owned(),
    )
}
