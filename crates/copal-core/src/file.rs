//! The file aggregate as the rest of the system sees it.
//!
//! `FileSpec` is what a caller supplies to create a file; `FileRecord` is
//! what every read path returns. Timestamps stay RFC3339 strings at this
//! layer: the database owns time (DEFAULT/VALUE clauses), the domain
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

fn default_residency() -> String {
    "local".to_owned()
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
    /// How many versions have completed. 0 until the first upload
    /// finishes; the completion CAS increments it atomically, which is
    /// also where version numbers come from.
    #[serde(default)]
    pub version_count: u64,
    /// Owner of the active upload claim, when one exists. Operational
    /// visibility: which instance is mid-upload, and whether a claim
    /// has gone stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_lease_owner: Option<String>,
    /// When the active upload claim expires and becomes stealable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_lease_expires_at: Option<String>,
    /// Residency of the linked content, parsed from the blob link;
    /// `None` until content lands. Serving resolves its backend from
    /// this, so reassigning a tenant never strands old content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_residency: Option<String>,
}

impl FileRecord {
    /// Whether content may be served: bytes exist and the record is not
    /// quarantined. State plays no part here; a re-upload in
    /// flight (or failed) keeps the previous version serving.
    pub fn servable_content(&self) -> bool {
        self.digest.is_some() && self.state != FileState::Quarantined
    }
}

/// One frozen version of a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileVersion {
    pub number: u64,
    pub content_type: String,
    pub size_bytes: u64,
    pub digest: ContentDigest,
    /// Residency of this version's content; `local` for every row
    /// written before residencies existed.
    #[serde(default = "default_residency", skip_serializing)]
    pub blob_residency: String,
    /// The file's metadata as it stood when this version completed.
    #[serde(default)]
    pub metadata_snapshot: serde_json::Value,
    /// Attribution is guarded: the engine redacts this column for
    /// caller sessions without the admin claim, so a row can arrive
    /// without it and must deserialize anyway. The wire contract
    /// already declares the field nullable, because guarded implies
    /// nullable on every face.
    pub created_by: Option<String>,
    pub created_at: String,
}

/// Result of a create: the record plus whether this call created it.
///
/// `created == false` means an idempotency-key replay returned the
/// original record, reported as success.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatedFile {
    pub record: FileRecord,
    pub created: bool,
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
