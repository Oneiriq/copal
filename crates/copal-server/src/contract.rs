//! THE Copal contract: one object, every face.
//!
//! This single declaration drives the checked-in artifacts
//! (`docs/openapi.json`, `docs/schema.graphql`, the four generated
//! clients), the drift gate in `tests/contract.rs`, AND the live
//! GraphQL endpoint — the served schema and the published documents
//! cannot disagree because they are the same object.

use janus::{Action, ActionField, ActionOutput, Contract, FieldExposure, Resource, TypeRef};

/// The wire contract for the files resource.
pub fn contract() -> Contract {
    Contract {
        name: "copal".into(),
        version: "0.1.0".into(),
        ir_revision: 1,
        resources: vec![Resource {
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
        }],
    }
}
