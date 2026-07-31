//! Copal HTTP serving layer.
//!
//! Library shape so integration tests drive the exact router the binary
//! serves.

pub mod app;
pub mod auth;
pub mod config;
pub mod contract;
pub mod edge;
pub mod error;
pub mod graphql;
pub mod metrics;
pub mod netguard;
pub mod pipeline;
pub mod s3;
pub mod serve;
pub mod sweeps;
pub mod tus;
pub mod webhooks;
pub mod wire;

pub use app::{build_router, AppState};
pub use config::Config;
