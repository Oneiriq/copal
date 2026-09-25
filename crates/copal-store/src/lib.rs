//! Copal metadata plane.
//!
//! Layering follows the store convention proven in the reference
//! codebases: a cloneable [`Store`] handle, schema as code in
//! [`schema`], row shapes in `dto`, and repositories as free functions
//! in [`repo`] that accept `&Store` and speak domain types. All
//! SurrealQL is builder-generated; the only string seams are stored
//! expressions (ASSERT/DEFAULT/VALUE/event bodies) and the transport
//! call that applies generated DDL.

mod dto;
pub mod fleet;
pub mod repo;
pub mod schema;
mod store;

pub use store::{RowChange, Store, StoreConfig};
