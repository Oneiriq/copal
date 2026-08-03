//! Copal HTTP serving layer.
//!
//! Library shape so integration tests drive the exact router the binary
//! serves.

pub mod app;
pub mod auth;
pub mod clamav;
pub mod config;
pub mod contract;
pub mod edge;
pub mod embed;
pub mod engine;
pub mod error;
pub mod extract;
pub mod graphql;
pub mod mcp;
pub mod metrics;
pub mod netguard;
pub mod pipeline;
pub mod rate;
pub mod s3;
pub mod serve;
pub mod session_cache;
pub mod sweeps;
pub mod tus;
pub mod webhooks;
pub mod wire;

pub use app::{build_router, AppState};
pub use config::Config;
