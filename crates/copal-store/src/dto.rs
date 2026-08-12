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
    // `current_version` is deliberately absent. It used to be carried
    // up so completion could pass it back down as the next version's
    // `prior`; the completion transaction reads it inside the engine
    // now, from the same statement that overwrites it, which is the
    // only place the two can be read and written without a gap.
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
///
/// A `Store` error is redacted at the API boundary and logged in full.
/// A `Conflict` is answered verbatim, because a caller who asked for
/// something that already exists should be told so, and that made the
/// engine's own words part of the response: the index name, the values
/// it holds, and the id of the record already holding them. That is a
/// description of stored data, handed to whoever guessed at it. The
/// conflict now says that something already exists and the log keeps
/// the rest.
pub(crate) fn map_store_err(context: &str, err: surql::error::SurqlError) -> CopalError {
    let text = err.to_string();
    if text.contains("already contains") || text.contains("already exists") {
        tracing::warn!(context, error = %text, "unique index violation");
        CopalError::conflict(format!("{context}: already exists"))
    } else {
        CopalError::Store(format!("{context}: {text}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A conflict says something already exists and stops there.
    ///
    /// The engine names the index, the values it holds and the record
    /// already holding them. A `Store` error is redacted at the API
    /// boundary, but a `Conflict` is answered word for word, so all
    /// of that was reaching whoever asked for a path that was taken:
    /// `create_file: database error: Database index
    /// `uniq_file_live_path` already contains ['alpha',
    /// 'alpha-secret.txt', ''], with record `file:01kzea32g4wv...``.
    #[test]
    fn a_conflict_does_not_carry_the_engine_words() {
        let engine = surql::error::SurqlError::Query {
            reason: "Database index `uniq_file_live_path` already contains \
                     ['alpha', 'secret.txt', ''], with record `file:01kzea32g4wv`"
                .to_owned(),
        };
        let mapped = map_store_err("create_file", engine);
        let said = mapped.to_string();
        assert!(
            matches!(mapped, CopalError::Conflict(_)),
            "a uniqueness violation is still a conflict: {said}",
        );
        for leaked in [
            "uniq_file_live_path",
            "file:01kzea32g4wv",
            "secret.txt",
            "alpha",
            "Database index",
        ] {
            assert!(
                !said.contains(leaked),
                "the conflict carried {leaked}: {said}",
            );
        }
        assert!(
            said.contains("already exists"),
            "and it still says what happened: {said}"
        );
    }

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
