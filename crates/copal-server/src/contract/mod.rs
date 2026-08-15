//! THE Copal contract: one contract, every face.
//!
//! What this assembles drives the checked-in artifacts
//! (`docs/openapi.json`, `docs/schema.graphql`, the four generated
//! clients), the drift gate in `tests/contract.rs`, AND the live
//! GraphQL endpoint; the served schema and the published documents
//! cannot disagree because they come from the same value.
//!
//! Each entity lives in its own file beside this one, so opening
//! `files.rs` puts nothing in front of a reader except the files
//! resource. This module holds what belongs to no single entity, and
//! hands janus the whole, because the checks that matter span it: a
//! rate class a resource names, a query that collides with another.

use janus::{AuthScheme, Contract, ContractLimits};

mod events;
mod file_text;
mod files;
mod runs;
mod search;
mod webhooks;

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

/// The wire contract, assembled from the entities beside this file.
pub fn contract() -> Contract {
    Contract {
        name: "copal".into(),
        version: "0.1.0".into(),
        ir_revision: 1,
        // Every generated client has always sent this header; until
        // janus learned to carry a scheme it was hardcoded in the
        // generators, which made them clients for copal rather than
        // for contracts. Declared here, it is copal's convention
        // living in copal's contract -- and the differ now treats
        // changing it as breaking, which it is.
        auth: AuthScheme::Header {
            name: "x-copal-tenant".into(),
            credential: "tenant".into(),
        },
        // Where the resource faces hang. The default, stated because
        // copal's routes really are versioned and a reader should not
        // have to know janus's default to know copal's paths.
        api_prefix: "/v1".into(),
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
            files::resource(),
            webhooks::resource(),
            events::resource(),
            runs::resource(),
        ],
        // Retrieval is the product, so it answers to the contract
        // like everything else: declared parameters, declared scopes,
        // declared budget, both faces, and the differ naming any
        // tightening. Neither shape fits a listing: search ranks by
        // relevance rather than sorting by a column, and a file's
        // text is one document rather than a page of rows.
        queries: vec![search::query(), file_text::query()],
    }
}
