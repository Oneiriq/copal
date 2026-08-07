//! The search query.
//!
//! One entity per file: everything here is that entity, and
//! nothing here is anything else. `super::contract` puts them
//! together, because what janus validates is the whole.

use janus::{ActionField, Query, TypeRef};

pub fn query() -> Query {
    Query {
        name: "search".into(),
        path: "/v1/search".into(),
        input: vec![
            ActionField {
                name: "q".into(),
                kind: TypeRef::String,
                required: true,
                multiple: false,
                options: Vec::new(),
                description: Some("The question, in the caller's own words.".into()),
            },
            ActionField {
                name: "mode".into(),
                kind: TypeRef::String,
                required: false,
                multiple: false,
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
                multiple: false,
                options: Vec::new(),
                description: Some("Documents to return, 1..=100.".into()),
            },
            ActionField {
                name: "prefix".into(),
                kind: TypeRef::String,
                required: false,
                multiple: false,
                options: Vec::new(),
                description: Some(
                    "Keep results whose file path starts with this.".into(),
                ),
            },
            ActionField {
                name: "content_type".into(),
                kind: TypeRef::String,
                required: false,
                multiple: false,
                options: Vec::new(),
                description: Some(
                    "Keep results whose file carries this content type.".into(),
                ),
            },
            ActionField {
                name: "cursor".into(),
                kind: TypeRef::String,
                required: false,
                multiple: false,
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
                multiple: true,
                options: ["content_type", "access"]
                    .iter()
                    .map(|s| (*s).to_owned())
                    .collect(),
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
    }
}
