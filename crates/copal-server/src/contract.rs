//! THE Copal contract: one object, every face.
//!
//! This single declaration drives the checked-in artifacts
//! (`docs/openapi.json`, `docs/schema.graphql`, the four generated
//! clients), the drift gate in `tests/contract.rs`, AND the live
//! GraphQL endpoint; the served schema and the published documents
//! cannot disagree because they are the same object.

use janus::{
    Action, ActionField, ActionOutput, Contract, ContractLimits, FieldExposure, Resource,
    SubResource, TypeRef,
};

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
        }),
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
                sortable: vec!["created_at".into()],
                max_page_size: 100,
                graphql: None,
                watchable: false,
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
                        FieldExposure::column("created_by"),
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
                actions: vec![
                    Action {
                        name: "issue_url".into(),
                        method: "POST".into(),
                        path: "/{id}/url".into(),
                        input: vec![
                        ActionField {
                            name: "ttl_secs".into(),
                            kind: TypeRef::Int,
                            required: false,
                            description: Some(
                                "Seconds until the URL stops working (default 900, max one year)."
                                    .into(),
                            ),
                        },
                        ActionField {
                            name: "max_uses".into(),
                            kind: TypeRef::Int,
                            required: false,
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
                    },
                    Action {
                        name: "issue_upload_url".into(),
                        method: "POST".into(),
                        path: "/{id}/upload-url".into(),
                        input: vec![ActionField {
                            name: "ttl_secs".into(),
                            kind: TypeRef::Int,
                            required: false,
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
                    },
                    Action {
                        name: "issue_edge_url".into(),
                        method: "POST".into(),
                        path: "/{id}/edge-url".into(),
                        input: vec![ActionField {
                            name: "ttl_secs".into(),
                            kind: TypeRef::Int,
                            required: false,
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
                                description: Some(
                                    "Bounding width, 16..=4096 (default 256).".into(),
                                ),
                            },
                            ActionField {
                                name: "height".into(),
                                kind: TypeRef::Int,
                                required: false,
                                description: Some(
                                    "Bounding height, 16..=4096 (default 256).".into(),
                                ),
                            },
                            ActionField {
                                name: "format".into(),
                                kind: TypeRef::String,
                                required: false,
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
                                description: Some(
                                    "Destination, which must resolve to a public address.".into(),
                                ),
                            },
                            ActionField {
                                name: "events".into(),
                                kind: TypeRef::Json,
                                required: false,
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
                    },
                ],
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
                sub_resources: vec![],
                actions: vec![],
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
                sub_resources: vec![],
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
                                description: Some("Registered workflow key.".into()),
                            },
                            ActionField {
                                name: "input".into(),
                                kind: TypeRef::Json,
                                required: false,
                                description: Some("Workflow input document.".into()),
                            },
                            ActionField {
                                name: "file".into(),
                                kind: TypeRef::String,
                                required: false,
                                description: Some(
                                    "Subject file id, when the run concerns one.".into(),
                                ),
                            },
                            ActionField {
                                name: "idempotency_key".into(),
                                kind: TypeRef::String,
                                required: false,
                                description: Some(
                                    "Dedupe key: a retried start returns the original run.".into(),
                                ),
                            },
                            ActionField {
                                name: "mode".into(),
                                kind: TypeRef::String,
                                required: false,
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
                    },
                ],
            },
        ],
    }
}
