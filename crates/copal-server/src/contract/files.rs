//! The files resource.
//!
//! One entity per file: everything here is that entity, and
//! nothing here is anything else. `super::contract` puts them
//! together, because what janus validates is the whole.

use janus::{Action, ActionField, ActionOutput, FieldExposure, Resource, SubResource, TypeRef};

pub fn resource() -> Resource {
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
            [
                "draft",
                "uploading",
                "scanning",
                "ready",
                "failed",
                "quarantined",
            ]
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
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "The file's path, unique among the tenant's live files.".into(),
                        ),
                    },
                    ActionField {
                        name: "content_type".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Declared type; defaults to application/octet-stream.".into(),
                        ),
                    },
                    ActionField {
                        name: "access".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: ["public", "private", "tenant", "grant"]
                            .iter()
                            .map(|s| (*s).to_owned())
                            .collect(),
                        description: Some(
                            "public, private, tenant, or grant; defaults to private.".into(),
                        ),
                    },
                    ActionField {
                        name: "metadata".into(),
                        kind: TypeRef::Json,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Caller metadata, stored verbatim.".into()),
                    },
                    ActionField {
                        name: "idempotency_key".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Replays return the original record instead of a duplicate.".into(),
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
                        multiple: false,
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
                        multiple: false,
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
                    multiple: false,
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
                    multiple: false,
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
                        multiple: false,
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
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Bounding width, 16..=4096 (default 256).".into()),
                    },
                    ActionField {
                        name: "height".into(),
                        kind: TypeRef::Int,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Bounding height, 16..=4096 (default 256).".into()),
                    },
                    ActionField {
                        name: "format".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
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
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Name of a transformer the deployment configures.".into(),
                        ),
                    },
                    ActionField {
                        name: "params".into(),
                        kind: TypeRef::Json,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Free-form parameters forwarded to the service.".into()),
                    },
                    ActionField {
                        name: "content_type".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
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
                        multiple: false,
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
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "The file's path, unique among the tenant's live files.".into(),
                        ),
                    },
                    ActionField {
                        name: "content_type".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Declared type; unset lets the source's answer stand.".into(),
                        ),
                    },
                    ActionField {
                        name: "access".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: ["public", "private", "tenant", "grant"]
                            .iter()
                            .map(|s| (*s).to_owned())
                            .collect(),
                        description: Some(
                            "public, private, tenant, or grant; defaults to private.".into(),
                        ),
                    },
                    ActionField {
                        name: "metadata".into(),
                        kind: TypeRef::Json,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Caller metadata, stored verbatim.".into()),
                    },
                    ActionField {
                        name: "idempotency_key".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Replays return the original record instead of a duplicate.".into(),
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
    }
}
