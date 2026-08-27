//! The runs resource.
//!
//! One entity per file: everything here is that entity, and
//! nothing here is anything else. `super::contract` puts them
//! together, because what kayak validates is the whole.

use kayak::{Action, ActionField, ActionOutput, FieldExposure, Resource, ResourceFaces, TypeRef};

pub fn resource() -> Resource {
    Resource {
        name: "runs".into(),
        table: "workflow_run".into(),
        identity: kayak::Identity::Id,
        // A browsable collection: paged and reachable by id.
        faces: ResourceFaces::ALL,
        fields: vec![
            FieldExposure::renamed("workflow_key", "workflow"),
            FieldExposure::column("status"),
            FieldExposure::column("output"),
            FieldExposure::renamed("run_error", "error"),
            FieldExposure::column("created_at"),
            FieldExposure::column("ended_at"),
        ],
        pinned: vec!["tenant_id".into()],
        pinned_either: vec![],
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
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Registered workflow key.".into()),
                    },
                    ActionField {
                        name: "input".into(),
                        kind: TypeRef::Json,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Workflow input document.".into()),
                    },
                    ActionField {
                        name: "file".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some("Subject file id, when the run concerns one.".into()),
                    },
                    ActionField {
                        name: "idempotency_key".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Dedupe key: a retried start returns the original run.".into(),
                        ),
                    },
                    ActionField {
                        name: "mode".into(),
                        kind: TypeRef::String,
                        required: false,
                        multiple: false,
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
    }
}
