//! Row shapes and row-to-domain mapping.
//!
//! Rows deserialize from the JSON the SDK produces: record ids arrive as
//! `table:id` strings, datetimes as RFC3339 strings. Mapping strips the
//! table prefix and parses domain newtypes, so nothing above this module
//! ever sees storage-shaped values.

use serde::Deserialize;
use serde_json::Value;

use copal_core::{AccessLevel, ContentDigest, CopalError, FileId, FileRecord, FileState, TenantId};

/// Raw `file` row.
#[derive(Debug, Deserialize)]
pub(crate) struct FileRow {
    pub id: String,
    pub tenant_id: String,
    pub path: String,
    pub state: FileState,
    pub access: AccessLevel,
    pub content_type: String,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub digest: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub version_count: u64,
    /// Raw record id of the current version link; consumed by the
    /// completion flow as the next version's `prior`.
    #[serde(default)]
    pub current_version: Option<String>,
    #[serde(default)]
    pub upload_lease_owner: Option<String>,
    #[serde(default)]
    pub upload_lease_expires_at: Option<String>,
    /// Raw record id of the blob link, when content has landed.
    #[serde(default)]
    pub blob: Option<String>,
}

/// Strip a `table:` prefix and any record-id escaping from an id
/// string.
///
/// The SDK renders record ids as `file:01jabc` plain, and escapes ids
/// with special characters as `` table:`id` `` or `table:⟨id⟩`
/// depending on version; domain ids carry no decoration.
pub(crate) fn strip_record_prefix<'a>(raw: &'a str, table: &str) -> &'a str {
    let tail = raw
        .strip_prefix(table)
        .and_then(|r| r.strip_prefix(':'))
        .unwrap_or(raw);
    tail.trim_start_matches('\u{27e8}')
        .trim_end_matches('\u{27e9}')
        .trim_start_matches('`')
        .trim_end_matches('`')
}

impl FileRow {
    pub(crate) fn into_domain(self) -> copal_core::Result<FileRecord> {
        let digest = match self.digest {
            Some(d) => Some(ContentDigest::parse(d)?),
            None => None,
        };
        Ok(FileRecord {
            id: FileId::parse(strip_record_prefix(&self.id, "file"))?,
            tenant_id: TenantId::parse(self.tenant_id)?,
            path: self.path,
            state: self.state,
            access: self.access,
            content_type: self.content_type,
            size_bytes: self.size_bytes,
            digest,
            metadata: self.metadata,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
            version_count: self.version_count,
            upload_lease_owner: self.upload_lease_owner,
            upload_lease_expires_at: self.upload_lease_expires_at,
            blob_residency: self.blob.as_deref().map(blob_link_residency),
        })
    }
}

/// Residency parsed from a blob link id: `blob:<digest>` is `local`,
/// `blob:<residency>-<digest>` names its backend. Residency names carry
/// no hyphen and digests are hex, so the last hyphen splits safely.
pub(crate) fn blob_link_residency(raw: &str) -> String {
    let bare = strip_record_prefix(raw, "blob");
    match bare.rsplit_once('-') {
        Some((residency, _)) => residency.to_owned(),
        None => "local".to_owned(),
    }
}

/// Map a surql-rs error into the workspace taxonomy, promoting unique
/// index violations to `Conflict` so callers can branch on them.
pub(crate) fn map_store_err(context: &str, err: surql::error::SurqlError) -> CopalError {
    let text = err.to_string();
    if text.contains("already contains") || text.contains("already exists") {
        CopalError::conflict(format!("{context}: {text}"))
    } else {
        CopalError::Store(format!("{context}: {text}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_plain_and_bracketed_ids() {
        assert_eq!(strip_record_prefix("file:01jabc", "file"), "01jabc");
        assert_eq!(
            strip_record_prefix("file:\u{27e8}01jabc\u{27e9}", "file"),
            "01jabc"
        );
        assert_eq!(strip_record_prefix("01jabc", "file"), "01jabc");
    }
}
