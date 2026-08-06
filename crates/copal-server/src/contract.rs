//! THE Copal contract: one object, every face.
//!
//! This single declaration drives the checked-in artifacts
//! (`docs/openapi.json`, `docs/schema.graphql`, the four generated
//! clients), the drift gate in `tests/contract.rs`, AND the live
//! GraphQL endpoint; the served schema and the published documents
//! cannot disagree because they are the same object.

use janus::{
    Action, ActionField, ActionOutput, Contract, ContractLimits, FieldExposure, Query, Resource,
    SubResource, TypeRef,
};

/// The guard registry the contract's declarations name. Both faces
/// evaluate these same closures: the dispatcher projects GraphQL rows
/// through them, and the REST handlers strip through the shared
/// projection API.
pub fn guards() -> janus::runtime::Guards {
    janus::runtime::Guards::new().guard("owner_or_admin", |ctx, row| {
        let Some(principal) = ctx.get::<janus::runtime::Principal>() else {
            return false;
        };
        if principal.has("admin") {
            return true;
        }
        // Per row: the author sees their own attribution. Rows from
        // before principals existed carry values that match no
        // handle, so they read as nobody's, which is the safe
        // default: treating unknown authorship as ownership would
        // widen access on upgrade. Without a row (the filter and
        // sort narrowing moment) a partial viewer answers false.
        row.and_then(|r| r.get("created_by"))
            .and_then(|v| v.as_str())
            .is_some_and(|owner| owner == principal.subject)
    })
}

/// The wire contract for the files resource.
pub fn contract() -> Contract {
    Contract {
        name: "copal".into(),
        version: "0.1.0".into(),
        ir_revision: 1,
        // The ceilings the served schema enforces. Declared here so
        // they appear in the artifacts and tightening them is a
        // breaking change the differ names. The schema has no cycles,
        // so honest queries sit far below both.
        limits: Some(ContractLimits {
            max_depth: Some(10),
            max_complexity: Some(500),
            // Eight concurrent subscriptions covers a dashboard with
            // headroom; one caller cannot hold every live query the
            // deployment will serve.
            max_watches_per_principal: Some(8),
        }),
        // Consumption budgets, charged per caller per minute on BOTH
        // faces against one ledger. A listing costs its row limit;
        // everything else costs one. Reads run generous because
        // retrieval is the product; mutations run an order tighter.
        rate_classes: vec![
            janus::RateClass {
                name: "reads".into(),
                units_per_minute: 6_000,
            },
            janus::RateClass {
                name: "mutations".into(),
                units_per_minute: 600,
            },
        ],
        resources: vec![
            Resource {
                name: "files".into(),
                table: "file".into(),
                fields: vec![
                    FieldExposure::column("path"),
                    FieldExposure::column("state"),
                    FieldExposure::column("access"),
                    FieldExposure::column("content_type"),
                    FieldExposure::renamed("size_bytes", "size"),
                    FieldExposure::column("digest"),
                    FieldExposure::column("metadata"),
                    FieldExposure::column("version_count"),
                    FieldExposure::column("created_at"),
                    FieldExposure::column("updated_at"),
                ],
                // tenant_id is server-bound on every query; it is what
                // lets the prefix rule credit idx_file_listing for the
                // created_at sort.
                pinned: vec!["tenant_id".into()],
                filterable: vec!["state".into()],
                // The set the schema asserts, so a caller narrowing by
                // state picks from what exists rather than guessing at
                // its spelling. Every face gets it: an enum in the
                // OpenAPI document and the MCP manifest, and a menu in
                // the console.
                filter_options: [(
                    "state".to_owned(),
                    ["draft", "uploading", "scanning", "ready", "failed", "quarantined"]
                        .iter()
                        .map(|s| (*s).to_owned())
                        .collect(),
                )]
                .into_iter()
                .collect(),
                sortable: vec!["created_at".into()],
                max_page_size: 100,
                graphql: None,
                watchable: false,
                reads_require: vec!["read".into()],
                rate_class: Some("reads".into()),
                sub_resources: vec![SubResource {
                    name: "versions".into(),
                    table: "file_version".into(),
                    parent_key: "file".into(),
                    fields: vec![
                        FieldExposure::column("number"),
                        FieldExposure::column("content_type"),
                        FieldExposure::renamed("size_bytes", "size"),
                        FieldExposure::column("digest"),
                        FieldExposure::column("metadata_snapshot"),
                        // Version attribution is audit data: who
                        // uploaded each revision is for operators, so
                        // distribution keys list history without it.
                        FieldExposure::column("created_by").with_guard("owner_or_admin"),
                        FieldExposure::column("created_at"),
                    ],
                    pinned: vec!["tenant_id".into()],
                    filterable: vec![],
                    // No sort is declared: the keyset walks the version
                    // number downward and the listing serves no other
                    // order, so claiming one would be a lie the index
                    // rules cannot catch.
                    sortable: vec![],
                    max_page_size: 100,
                    description: Some("Every stored version of this file, newest first.".into()),
                    graphql: None,
                }],
                content: Some(janus::ContentFaces {
                    upload: true,
                    download: true,
                }),
                actions: vec![
                    Action {
                        name: "create".into(),
                        method: "POST".into(),
                        path: "".into(),
                        input: vec![
                            ActionField {
                                name: "path".into(),
                                kind: TypeRef::String,
                                required: true,
                                options: Vec::new(),
                                description: Some(
                                    "The file's path, unique among the tenant's live files."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "content_type".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Declared type; defaults to application/octet-stream.".into(),
                                ),
                            },
                            ActionField {
                                name: "access".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: ["public", "private", "tenant", "grant"]
                                    .iter()
                                    .map(|s| (*s).to_owned())
                                    .collect(),
                                description: Some(
                                    "public, private, tenant, or grant; defaults to private."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "metadata".into(),
                                kind: TypeRef::Json,
                                required: false,
                                options: Vec::new(),
                                description: Some("Caller metadata, stored verbatim.".into()),
                            },
                            ActionField {
                                name: "idempotency_key".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Replays return the original record instead of a duplicate."
                                        .into(),
                                ),
                            },
                        ],
                        output: ActionOutput::Resource,
                        description: Some(
                            "Create the file record; bytes follow through the content upload \
                             or an upload grant."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "issue_url".into(),
                        method: "POST".into(),
                        path: "/{id}/url".into(),
                        input: vec![
                        ActionField {
                            name: "ttl_secs".into(),
                            kind: TypeRef::Int,
                            required: false,
                            options: Vec::new(),
                            description: Some(
                                "Seconds until the URL stops working (default 900, max one year)."
                                    .into(),
                            ),
                        },
                        ActionField {
                            name: "max_uses".into(),
                            kind: TypeRef::Int,
                            required: false,
                            options: Vec::new(),
                            description: Some(
                                "Cap on redemptions; one-time links use 1. Unset = unlimited \
                                 within the TTL."
                                    .into(),
                            ),
                        },
                    ],
                        output: ActionOutput::Json,
                        description: Some("Issue a signed URL for a servable file.".into()),
                        graphql_field: None,
                        requires: vec!["read".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "issue_upload_url".into(),
                        method: "POST".into(),
                        path: "/{id}/upload-url".into(),
                        input: vec![ActionField {
                            name: "ttl_secs".into(),
                            kind: TypeRef::Int,
                            required: false,
                            options: Vec::new(),
                            description: Some(
                                "Seconds until the upload URL stops working (default 900, max \
                                 one day)."
                                    .into(),
                            ),
                        }],
                        output: ActionOutput::Json,
                        description: Some(
                            "Issue a single-use write capability, so a browser can upload \
                             without holding a tenant key."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "issue_edge_url".into(),
                        method: "POST".into(),
                        path: "/{id}/edge-url".into(),
                        input: vec![ActionField {
                            name: "ttl_secs".into(),
                            kind: TypeRef::Int,
                            required: false,
                            options: Vec::new(),
                            description: Some(
                                "Seconds until the token expires (default 900, max one day). \
                                 Edge tokens cannot be revoked individually."
                                    .into(),
                            ),
                        }],
                        output: ActionOutput::Json,
                        description: Some(
                            "Issue a cg2 token a CDN can verify itself, without a database \
                             hop."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["read".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "request_rendition".into(),
                        method: "POST".into(),
                        path: "/{id}/renditions".into(),
                        input: vec![
                            ActionField {
                                name: "kind".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Rendition label, part of the derived path (default \
                                     \"thumb\")."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "width".into(),
                                kind: TypeRef::Int,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Bounding width, 16..=4096 (default 256).".into(),
                                ),
                            },
                            ActionField {
                                name: "height".into(),
                                kind: TypeRef::Int,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Bounding height, 16..=4096 (default 256).".into(),
                                ),
                            },
                            ActionField {
                                name: "format".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some("\"jpeg\" (default) or \"png\".".into()),
                            },
                        ],
                        output: ActionOutput::Json,
                        description: Some(
                            "Derive an image rendition; repeating a request returns the \
                             existing one."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "transform".into(),
                        method: "POST".into(),
                        path: "/{id}/transform".into(),
                        input: vec![
                            ActionField {
                                name: "transformer".into(),
                                kind: TypeRef::String,
                                required: true,
                                options: Vec::new(),
                                description: Some(
                                    "Name of a transformer the deployment configures."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "params".into(),
                                kind: TypeRef::Json,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Free-form parameters forwarded to the service.".into(),
                                ),
                            },
                            ActionField {
                                name: "content_type".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Declared media type of the derived output (default \
                                     application/octet-stream)."
                                        .into(),
                                ),
                            },
                        ],
                        output: ActionOutput::Json,
                        description: Some(
                            "Derive new content through an operator-configured external \
                             transformer; repeating a request returns the existing \
                             derivation."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "fetch".into(),
                        method: "POST".into(),
                        path: "/fetch".into(),
                        input: vec![
                            ActionField {
                                name: "url".into(),
                                kind: TypeRef::String,
                                required: true,
                                options: Vec::new(),
                                description: Some(
                                    "http or https source the server pulls; the outbound \
                                     policy applies."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "path".into(),
                                kind: TypeRef::String,
                                required: true,
                                options: Vec::new(),
                                description: Some(
                                    "The file's path, unique among the tenant's live files."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "content_type".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Declared type; unset lets the source's answer stand."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "access".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: ["public", "private", "tenant", "grant"]
                                    .iter()
                                    .map(|s| (*s).to_owned())
                                    .collect(),
                                description: Some(
                                    "public, private, tenant, or grant; defaults to private."
                                        .into(),
                                ),
                            },
                            ActionField {
                                name: "metadata".into(),
                                kind: TypeRef::Json,
                                required: false,
                                options: Vec::new(),
                                description: Some("Caller metadata, stored verbatim.".into()),
                            },
                            ActionField {
                                name: "idempotency_key".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Replays return the original record instead of a duplicate."
                                        .into(),
                                ),
                            },
                        ],
                        output: ActionOutput::Json,
                        description: Some(
                            "Ingest content from a URL the server fetches itself; the \
                             fetched bytes walk the standard scan and finalize pipeline."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "remove".into(),
                        method: "DELETE".into(),
                        path: "/{id}".into(),
                        input: vec![],
                        output: ActionOutput::None,
                        description: Some(
                            "Soft-delete: tombstone the record and free its live path; bytes are \
                         reclaimed by garbage collection once nothing references them."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                ],
            },
            Resource {
                name: "webhooks".into(),
                table: "webhook_endpoint".into(),
                fields: vec![
                    FieldExposure::column("target_url"),
                    FieldExposure::column("events"),
                    FieldExposure::column("active"),
                    FieldExposure::column("created_at"),
                ],
                pinned: vec!["tenant_id".into()],
                filterable: vec![],
                sortable: vec!["created_at".into()],
                max_page_size: 100,
                graphql: None,
                watchable: false,
                reads_require: vec!["read".into()],
                rate_class: Some("reads".into()),
                sub_resources: vec![SubResource {
                    name: "deliveries".into(),
                    table: "webhook_delivery".into(),
                    parent_key: "endpoint".into(),
                    fields: vec![
                        FieldExposure::column("state"),
                        FieldExposure::column("attempts"),
                        FieldExposure::column("last_status"),
                        FieldExposure::column("next_attempt_at"),
                        FieldExposure::column("created_at"),
                    ],
                    pinned: vec!["tenant_id".into()],
                    filterable: vec!["state".into()],
                    sortable: vec![],
                    max_page_size: 100,
                    description: Some("Delivery attempts to this endpoint, newest first.".into()),
                    graphql: None,
                }],
                content: None,
                actions: vec![
                    Action {
                        name: "register".into(),
                        method: "POST".into(),
                        path: String::new(),
                        input: vec![
                            ActionField {
                                name: "url".into(),
                                kind: TypeRef::String,
                                required: true,
                                options: Vec::new(),
                                description: Some(
                                    "Destination, which must resolve to a public address.".into(),
                                ),
                            },
                            ActionField {
                                name: "events".into(),
                                kind: TypeRef::Json,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Dotted actions to deliver; empty means every event.".into(),
                                ),
                            },
                        ],
                        output: ActionOutput::Json,
                        description: Some(
                            "Register an endpoint. The signing secret appears once, here.".into(),
                        ),
                        graphql_field: None,
                        requires: vec!["admin".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "remove".into(),
                        method: "DELETE".into(),
                        path: "/{id}".into(),
                        input: vec![],
                        output: ActionOutput::None,
                        description: Some(
                            "Deactivate an endpoint; pending deliveries settle as failed.".into(),
                        ),
                        graphql_field: None,
                        requires: vec!["admin".into()],
                        rate_class: Some("mutations".into()),
                    },
                ],
            filter_options: Default::default(),
            },
            Resource {
                name: "events".into(),
                table: "file_event".into(),
                fields: vec![
                    // `action` keeps its column name on the wire:
                    // `event` is reserved in SurrealDB v3, and the
                    // contract's name gate refuses overrides that
                    // would collide there.
                    FieldExposure::column("action"),
                    FieldExposure::column("payload"),
                    FieldExposure::column("created_at"),
                ],
                pinned: vec!["tenant_id".into()],
                filterable: vec!["action".into()],
                sortable: vec!["created_at".into()],
                max_page_size: 100,
                graphql: None,
                // The outbox is the one resource worth watching: it is
                // where the engine records everything that happened to
                // a file, so a client that subscribes here stops
                // polling for processing to finish.
                watchable: true,
                reads_require: vec!["read".into()],
                rate_class: Some("reads".into()),
                sub_resources: vec![],
                content: None,
                actions: vec![],
            filter_options: Default::default(),
            },
            Resource {
                name: "runs".into(),
                table: "workflow_run".into(),
                fields: vec![
                    FieldExposure::renamed("workflow_key", "workflow"),
                    FieldExposure::column("status"),
                    FieldExposure::column("output"),
                    FieldExposure::renamed("run_error", "error"),
                    FieldExposure::column("created_at"),
                    FieldExposure::column("ended_at"),
                ],
                pinned: vec!["tenant_id".into()],
                // status rides idx_run_ops (tenant_id, status, created_at),
                // which is also what makes the created_at sort reachable.
                filterable: vec!["status".into()],
                sortable: vec!["created_at".into()],
                max_page_size: 100,
                graphql: None,
                watchable: false,
                reads_require: vec!["read".into()],
                rate_class: Some("reads".into()),
                sub_resources: vec![],
                content: None,
                actions: vec![
                    Action {
                        name: "start".into(),
                        method: "POST".into(),
                        path: String::new(),
                        input: vec![
                            ActionField {
                                name: "workflow".into(),
                                kind: TypeRef::String,
                                required: true,
                                options: Vec::new(),
                                description: Some("Registered workflow key.".into()),
                            },
                            ActionField {
                                name: "input".into(),
                                kind: TypeRef::Json,
                                required: false,
                                options: Vec::new(),
                                description: Some("Workflow input document.".into()),
                            },
                            ActionField {
                                name: "file".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Subject file id, when the run concerns one.".into(),
                                ),
                            },
                            ActionField {
                                name: "idempotency_key".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "Dedupe key: a retried start returns the original run.".into(),
                                ),
                            },
                            ActionField {
                                name: "mode".into(),
                                kind: TypeRef::String,
                                required: false,
                                options: Vec::new(),
                                description: Some(
                                    "\"async\" (default) enqueues for a worker; \"sync\" executes \
                                 in-request over the same journal."
                                        .into(),
                                ),
                            },
                        ],
                        output: ActionOutput::Json,
                        description: Some("Start a workflow run.".into()),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                    Action {
                        name: "retry".into(),
                        method: "POST".into(),
                        path: "/{id}/retry".into(),
                        input: vec![],
                        output: ActionOutput::Json,
                        description: Some(
                            "Retry a FAILED run with its journal intact; a failed subject file \
                         returns to scanning first, so the pipeline's finalize applies."
                                .into(),
                        ),
                        graphql_field: None,
                        requires: vec!["write".into()],
                        rate_class: Some("mutations".into()),
                    },
                ],
            filter_options: Default::default(),
            },
        ],
        // Retrieval is the product, so it answers to the contract
        // like everything else: declared parameters, declared scopes,
        // declared budget, both faces, and the differ naming any
        // tightening. Neither shape fits a listing: search ranks by
        // relevance rather than sorting by a column, and a file's
        // text is one document rather than a page of rows.
        queries: vec![
            Query {
                name: "search".into(),
                path: "/v1/search".into(),
                input: vec![
                    ActionField {
                        name: "q".into(),
                        kind: TypeRef::String,
                        required: true,
                        options: Vec::new(),
                        description: Some("The question, in the caller's own words.".into()),
                    },
                    ActionField {
                        name: "mode".into(),
                        kind: TypeRef::String,
                        required: false,
                        options: ["lexical", "semantic", "hybrid"]
                            .iter()
                            .map(|s| (*s).to_owned())
                            .collect(),
                        description: Some(
                            "lexical, semantic, or hybrid (the default). A deployment without                              an embedding service answers lexically and says so."
                                .into(),
                        ),
                    },
                    ActionField {
                        name: "limit".into(),
                        kind: TypeRef::Int,
                        required: false,
                        options: Vec::new(),
                        description: Some("Documents to return, 1..=100.".into()),
                    },
                    ActionField {
                        name: "prefix".into(),
                        kind: TypeRef::String,
                        required: false,
                        options: Vec::new(),
                        description: Some(
                            "Keep results whose file path starts with this.".into(),
                        ),
                    },
                    ActionField {
                        name: "content_type".into(),
                        kind: TypeRef::String,
                        required: false,
                        options: Vec::new(),
                        description: Some(
                            "Keep results whose file carries this content type.".into(),
                        ),
                    },
                    ActionField {
                        name: "cursor".into(),
                        kind: TypeRef::String,
                        required: false,
                        options: Vec::new(),
                        description: Some(
                            "Continue a ranking from the previous page's next_cursor;                              best-effort, since rankings shift as content changes."
                                .into(),
                        ),
                    },
                    ActionField {
                        name: "facets".into(),
                        kind: TypeRef::String,
                        required: false,
                        options: Vec::new(),
                        description: Some(
                            "Count the match set by these file fields, comma separated                              (content_type, access). Counts are documents, exact over                              every match rather than over the ranked window."
                                .into(),
                        ),
                    },
                ],
                description: Some(
                    "Retrieval across the tenant's extracted text: engine-selected candidates                      rescored in process, fused across lexical and semantic rankings."
                        .into(),
                ),
                graphql_field: None,
                requires: vec!["read".into()],
                rate_class: Some("reads".into()),
            },
            Query {
                name: "file_text".into(),
                path: "/v1/files/{id}/text".into(),
                input: vec![ActionField {
                    name: "id".into(),
                    kind: TypeRef::String,
                    required: true,
                    options: Vec::new(),
                    description: Some("The file whose extracted text to read.".into()),
                }],
                description: Some(
                    "One file's extracted text, as the pipeline stored it.".into(),
                ),
                graphql_field: None,
                requires: vec!["read".into()],
                rate_class: Some("reads".into()),
            },
        ],
    }
}
