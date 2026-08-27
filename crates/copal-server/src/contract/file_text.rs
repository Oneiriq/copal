//! The file_text query.
//!
//! One entity per file: everything here is that entity, and
//! nothing here is anything else. `super::contract` puts them
//! together, because what kayak validates is the whole.

use kayak::{ActionField, Query, TypeRef};

pub fn query() -> Query {
    Query {
        name: "file_text".into(),
        path: "/v1/files/{id}/text".into(),
        // Searches nothing and rests on nothing: this is a keyed
        // read. Both empty, which is what every query that is not a
        // search declares, and is why the gate can tell the two apart.
        searches: vec![],
        backing: vec![],
        input: vec![ActionField {
            name: "id".into(),
            kind: TypeRef::String,
            required: true,
            multiple: false,
            options: Vec::new(),
            description: Some("The file whose extracted text to read.".into()),
        }],
        description: Some("One file's extracted text, as the pipeline stored it.".into()),
        graphql_field: None,
        requires: vec!["read".into()],
        rate_class: Some("reads".into()),
    }
}
