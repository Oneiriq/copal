//! Copal domain types.
//!
//! Everything here is pure data and pure functions: identifiers, content
//! digests, the file state machine, and the error taxonomy. No IO, no
//! database, no async. The store, blob, and server crates depend on this
//! crate; it depends on nothing of theirs.

pub mod bm25;
pub mod chunk;
pub mod digest;
pub mod error;
pub mod excerpt;
pub mod file;
pub mod id;
pub mod inspect;
pub mod state;

pub use bm25::rank as rank_lexical;
pub use chunk::split as split_passages;
pub use digest::{ContentDigest, DigestBuilder};
pub use error::CopalError;
pub use excerpt::{excerpt, Excerpt};
pub use file::{CreatedFile, FileRecord, FileSpec, FileVersion};
pub use id::{FileId, TenantId};
pub use inspect::{sniff_content_type, ExtensionPolicy};
pub use state::{AccessLevel, FileState};

/// Convenience result alias used across the workspace.
pub type Result<T> = std::result::Result<T, CopalError>;
