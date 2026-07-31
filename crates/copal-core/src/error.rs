//! Error taxonomy for the workspace.
//!
//! Variants are grouped by what the caller can do about them, not by
//! which subsystem raised them: `Validation` and `Conflict` are caller
//! errors, `NotFound` is a caller error with a distinct HTTP mapping,
//! `Store`/`Blob` are infrastructure failures the caller retries or
//! reports.

use thiserror::Error;

/// Workspace-wide error type.
#[derive(Debug, Error)]
pub enum CopalError {
    /// The request is malformed or violates an invariant.
    #[error("validation: {0}")]
    Validation(String),

    /// The target does not exist (or is soft-deleted).
    #[error("not found: {0}")]
    NotFound(String),

    /// The request lost a compare-and-swap race or violates uniqueness —
    /// e.g. an illegal state transition or a duplicate idempotency key.
    #[error("conflict: {0}")]
    Conflict(String),

    /// The metadata plane failed.
    #[error("store: {0}")]
    Store(String),

    /// The blob plane failed.
    #[error("blob: {0}")]
    Blob(String),
}

impl CopalError {
    /// Shorthand for a validation failure.
    pub fn validation(msg: impl Into<String>) -> Self {
        Self::Validation(msg.into())
    }

    /// Shorthand for a not-found failure.
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    /// Shorthand for a conflict failure.
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::Conflict(msg.into())
    }
}
