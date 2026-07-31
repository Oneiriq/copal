//! The GraphQL face, served by Janus from THE contract.
//!
//! Nothing here restates the API: the schema is built dynamically from
//! [`crate::contract::contract`] and every field dispatches through the
//! Janus runtime — resolvers below call the same repositories the REST
//! handlers call, and the same wire mapper renders rows, so the two
//! protocols cannot diverge. Tenancy is a Janus middleware: the HTTP
//! layer seeds the per-request context from `x-copal-tenant`, and
//! [`RequireTenant`] fails closed when it is missing.

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};

use copal_blob::BlobStore;
use copal_core::{CopalError, FileId, FileState, TenantId};
use copal_store::repo::file as file_repo;
use janus::runtime::graphql::build_schema;
use janus::runtime::{
    BoxFuture, Dispatcher, JanusContext, JanusError, ListOutput, Middleware, Next, Operation,
    Outcome, Payload, Resolvers, SortDirection,
};

use crate::app::{decode_cursor, encode_cursor, issue_grant_core, AppState};
use crate::wire::wire_file;

/// The per-request tenant, seeded from the transport by the HTTP layer.
#[derive(Debug, Clone)]
pub struct Tenant(pub TenantId);

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
                    "missing or invalid x-copal-tenant header".into(),
                ));
            }
            next.run(operation, ctx, payload).await
        })
    }
}

fn tenant_of(ctx: &JanusContext) -> Result<TenantId, JanusError> {
    ctx.get::<Tenant>()
        .map(|t| t.0.clone())
        .ok_or_else(|| JanusError::Unauthorized("missing or invalid x-copal-tenant header".into()))
}

/// Domain errors in runtime vocabulary; infrastructure detail stays in
/// the log, mirroring the REST error mapper.
fn to_janus_error(err: CopalError) -> JanusError {
    match err {
        CopalError::Validation(m) => JanusError::BadRequest(m),
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
    let remove_state = state;

    let resolvers = Resolvers::new()
        .list("files", move |ctx, args| {
            let state = list_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let after = args
                    .cursor
                    .as_deref()
                    .map(decode_cursor)
                    .transpose()
                    .map_err(|e| JanusError::BadRequest(e.0.to_string()))?;
                let ascending = matches!(&args.sort, Some((_, SortDirection::Asc)));
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
                        encode_cursor(&file_repo::ListPosition {
                            created_at: last.created_at.clone(),
                            id: last.id.clone(),
                        })
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
                issue_grant_core(&state, &tenant, &id, ttl_secs, max_uses)
                    .await
                    .map(Some)
                    .map_err(|e| to_janus_error(e.0))
            }
        })
        .action("files", "remove", move |ctx, args| {
            let state = remove_state.clone();
            async move {
                let tenant = tenant_of(&ctx)?;
                let id = parse_file_id(args.id.as_deref().unwrap_or_default())?;
                file_repo::soft_delete(&state.store, &tenant, &id)
                    .await
                    .map_err(to_janus_error)?;
                Ok(None)
            }
        });

    Ok(Arc::new(Dispatcher::new(
        Arc::new(crate::contract::contract()),
        resolvers,
        vec![Arc::new(RequireTenant) as Arc<dyn Middleware>],
    )?))
}

/// Shared state for the GraphQL routes: the executable schema plus the
/// generated SDL document served for discovery.
#[derive(Clone)]
struct GraphqlState {
    schema: async_graphql::dynamic::Schema,
    sdl: Arc<String>,
}

/// Build the `/graphql` router: POST executes, GET serves the SDL.
///
/// Panics only on a contract/schema mismatch or missing resolver —
/// construction-time bugs the contract tests catch first.
pub fn graphql_router<B: BlobStore + 'static>(state: AppState<B>) -> Router {
    let tables = copal_store::schema::tables();
    let schema = build_schema(&tables, dispatcher(state).expect("resolver completeness"))
        .expect("contract builds a schema");
    let sdl =
        janus::generate_sdl(&crate::contract::contract(), &tables).expect("contract generates SDL");
    let gql = GraphqlState {
        schema,
        sdl: Arc::new(sdl),
    };
    Router::new()
        .route("/graphql", post(execute).get(sdl_document))
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

async fn execute(
    State(gql): State<GraphqlState>,
    headers: HeaderMap,
    Json(body): Json<GraphqlRequest>,
) -> impl IntoResponse {
    let mut request = async_graphql::Request::new(body.query);
    if let Some(variables) = body.variables {
        request = request.variables(async_graphql::Variables::from_json(variables));
    }
    if let Some(operation) = body.operation_name {
        request = request.operation_name(operation);
    }

    // Seed the per-request context. An absent or unparsable tenant
    // seeds nothing; RequireTenant rejects any contract operation.
    let mut ctx = JanusContext::new();
    if let Some(tenant) = headers
        .get("x-copal-tenant")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| TenantId::parse(raw).ok())
    {
        ctx.insert(Tenant(tenant));
    }
    let response = gql.schema.execute(request.data(ctx)).await;
    Json(serde_json::to_value(response).expect("graphql response serializes"))
}

async fn sdl_document(State(gql): State<GraphqlState>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/graphql")],
        gql.sdl.as_str().to_owned(),
    )
}
