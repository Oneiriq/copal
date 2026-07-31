//! The file aggregate as the rest of the system sees it.
//!
//! `FileSpec` is what a caller supplies to create a file; `FileRecord` is
//! what every read path returns. Timestamps stay RFC3339 strings at this
//! layer — the database owns time (DEFAULT/VALUE clauses), the domain
//! only relays it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AccessLevel, ContentDigest, FileId, FileState, TenantId};

/// Caller-supplied description of a file to create.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSpec {
    /// Logical path, unique per tenant among live files.
    pub path: String,
    /// MIME type as declared; verified against magic bytes at completion.
    #[serde(default = "default_content_type")]
    pub content_type: String,
    /// Visibility class.
    #[serde(default = "default_access")]
    pub access: AccessLevel,
    /// Open metadata bag.
    #[serde(default)]
    pub metadata: Value,
    /// Dedupe key: a retried create with the same key returns the
    /// original record instead of a duplicate.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

fn default_content_type() -> String {
    "application/octet-stream".to_owned()
}

fn default_access() -> AccessLevel {
    AccessLevel::Private
}

impl FileSpec {
    /// Validate caller input before it reaches the store.
    pub fn validate(&self) -> crate::Result<()> {
        if self.path.is_empty() {
            return Err(crate::CopalError::validation("path cannot be empty"));
        }
        if self.path.contains('\0') {
            return Err(crate::CopalError::validation(
                "path cannot contain NUL bytes",
            ));
        }
        Ok(())
    }
}

/// A file as returned by every read path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    pub id: FileId,
    pub tenant_id: TenantId,
    pub path: String,
    pub state: FileState,
    pub access: AccessLevel,
    pub content_type: String,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub digest: Option<ContentDigest>,
    #[serde(default)]
    pub metadata: Value,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_defaults_are_sane() {
        let spec: FileSpec = serde_json::from_str(r#"{"path": "docs/a.pdf"}"#).unwrap();
        assert_eq!(spec.content_type, "application/octet-stream");
        assert_eq!(spec.access, AccessLevel::Private);
        assert!(spec.idempotency_key.is_none());
        spec.validate().unwrap();
    }

    #[test]
    fn spec_rejects_empty_and_nul_paths() {
        let empty = FileSpec {
            path: String::new(),
            content_type: default_content_type(),
            access: AccessLevel::Private,
            metadata: Value::Null,
            idempotency_key: None,
        };
        assert!(empty.validate().is_err());
        let nul = FileSpec {
            path: "a\0b".into(),
            ..empty
        };
        assert!(nul.validate().is_err());
    }
}
