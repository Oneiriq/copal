//! The events resource.
//!
//! One entity per file: everything here is that entity, and
//! nothing here is anything else. `super::contract` puts them
//! together, because what kayak validates is the whole.

use kayak::{FieldExposure, Resource, ResourceFaces};

pub fn resource() -> Resource {
    Resource {
        name: "events".into(),
        table: "file_event".into(),
        identity: kayak::Identity::Id,
        // A browsable collection: paged and reachable by id.
        faces: ResourceFaces::ALL,
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
        pinned_either: vec![],
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
    }
}
