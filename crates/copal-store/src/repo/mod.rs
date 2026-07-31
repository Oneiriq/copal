//! Repositories: one module per table, free functions over [`crate::Store`].

pub mod auth;
pub mod blob;
pub mod eventing;
pub mod file;
pub mod flow;
pub mod grant;
pub mod s3;
pub mod tus;
pub mod version;
