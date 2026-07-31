//! Copal HTTP serving layer.
//!
//! Library shape so integration tests drive the exact router the binary
//! serves.

pub mod app;
pub mod config;
pub mod contract;
pub mod error;
pub mod graphql;
pub mod pipeline;
pub mod sweeps;
pub mod wire;

pub use app::{build_router, AppState};
pub use config::Config;
