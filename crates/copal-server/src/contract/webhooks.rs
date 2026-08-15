//! The webhooks resource.
//!
//! One entity per file: everything here is that entity, and
//! nothing here is anything else. `super::contract` puts them
//! together, because what janus validates is the whole.

use janus::{
    Action, ActionField, ActionOutput, FieldExposure, Resource, ResourceFaces, SubResource, TypeRef,
};

pub fn resource() -> Resource {
    Resource {
        name: "webhooks".into(),
        table: "webhook_endpoint".into(),
        // A browsable collection: paged and reachable by id.
        faces: ResourceFaces::ALL,
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
                        multiple: false,
                        options: Vec::new(),
                        description: Some(
                            "Destination, which must resolve to a public address.".into(),
                        ),
                    },
                    ActionField {
                        name: "events".into(),
                        kind: TypeRef::Json,
                        required: false,
                        multiple: false,
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
    }
}
