//! Copal HTTP serving layer.
//!
//! Library shape so integration tests drive the exact router the binary
//! serves.

pub mod app;
pub mod config;
pub mod error;

pub use app::{build_router, AppState};
pub use config::Config;
