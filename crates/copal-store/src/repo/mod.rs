//! Repositories: one module per table, free functions over [`crate::Store`].
//!
//! [`completion`] is the one exception, and it says why in its own
//! module comment: finishing an upload writes three tables in one
//! transaction, so it belongs to none of them.

pub mod auth;
pub mod blob;
pub mod completion;
pub mod edge;
pub mod eventing;
pub mod file;
pub mod flow;
pub mod grant;
pub mod multipart;
pub mod principal;
pub mod rate;
pub mod rotate;
pub mod s3;
pub mod tenant;
pub mod text;
pub mod tus;
pub mod version;
