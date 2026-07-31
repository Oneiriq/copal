//! The file state machine.
//!
//! One record, one `state` column, and a fixed transition table — this is
//! the load-bearing replacement for the separate pending-files table the
//! predecessor design carried. The optimistic-create contract holds by
//! construction: a caller that creates and immediately lists sees the
//! `draft`/`uploading` record.
//!
//! Transitions are enforced twice: here (pure, exhaustively tested) and
//! in the store as a guarded compare-and-swap, so a lost race surfaces as
//! a conflict rather than a silent overwrite.

use serde::{Deserialize, Serialize};

use crate::error::CopalError;

/// Lifecycle states of a file record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileState {
    /// Metadata exists; no upload started.
    Draft,
    /// An upload session owns the record.
    Uploading,
    /// Bytes landed; content checks are running.
    Scanning,
    /// Live and servable.
    Ready,
    /// Upload or processing failed; retryable.
    Failed,
    /// Content checks rejected the bytes; never servable.
    Quarantined,
    /// Soft-deleted tombstone.
    Deleted,
}

impl FileState {
    /// Wire name, matching the schema's `ASSERT ... INSIDE [...]` list.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Uploading => "uploading",
            Self::Scanning => "scanning",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Quarantined => "quarantined",
            Self::Deleted => "deleted",
        }
    }

    /// Every state a record may move to from `self`.
    pub fn allowed_transitions(self) -> &'static [FileState] {
        use FileState::*;
        match self {
            Draft => &[Uploading, Deleted],
            Uploading => &[Scanning, Ready, Failed, Deleted],
            Scanning => &[Ready, Quarantined, Failed, Deleted],
            // Ready -> Uploading is a re-upload: the next version. The
            // previous content keeps serving throughout (servability is
            // digest-based, not state-based).
            Ready => &[Uploading, Deleted],
            Failed => &[Uploading, Deleted],
            Quarantined => &[Deleted],
            Deleted => &[],
        }
    }

    /// Validate a transition, returning a conflict error naming both ends.
    pub fn ensure_transition(self, to: FileState) -> crate::Result<()> {
        if self.allowed_transitions().contains(&to) {
            Ok(())
        } else {
            Err(CopalError::conflict(format!(
                "illegal state transition {} -> {}",
                self.as_str(),
                to.as_str(),
            )))
        }
    }

    /// Whether content may be served in this state.
    pub fn servable(self) -> bool {
        matches!(self, Self::Ready)
    }
}

/// Who may read a file's content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessLevel {
    /// Anyone with the URL; CDN-cacheable.
    Public,
    /// Authenticated principals of the owning tenant with file scope.
    Private,
    /// Any authenticated principal of the owning tenant.
    Tenant,
    /// Only via an explicit signed grant.
    Grant,
}

impl AccessLevel {
    /// Wire name, matching the schema's `ASSERT ... INSIDE [...]` list.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Private => "private",
            Self::Tenant => "tenant",
            Self::Grant => "grant",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use FileState::*;

    const ALL: [FileState; 7] = [
        Draft,
        Uploading,
        Scanning,
        Ready,
        Failed,
        Quarantined,
        Deleted,
    ];

    #[test]
    fn deleted_is_terminal() {
        for to in ALL {
            assert!(Deleted.ensure_transition(to).is_err());
        }
    }

    #[test]
    fn quarantine_can_never_become_ready() {
        assert!(Quarantined.ensure_transition(Ready).is_err());
        // The only way out of quarantine is deletion.
        assert_eq!(Quarantined.allowed_transitions(), &[Deleted]);
    }

    #[test]
    fn happy_path_is_legal() {
        Draft.ensure_transition(Uploading).unwrap();
        Uploading.ensure_transition(Scanning).unwrap();
        Scanning.ensure_transition(Ready).unwrap();
        Ready.ensure_transition(Deleted).unwrap();
    }

    #[test]
    fn re_upload_from_ready_is_legal() {
        Ready.ensure_transition(Uploading).unwrap();
    }

    #[test]
    fn failed_uploads_are_retryable() {
        Uploading.ensure_transition(Failed).unwrap();
        Failed.ensure_transition(Uploading).unwrap();
    }

    #[test]
    fn skipping_scan_is_legal_when_no_scanner_is_configured() {
        Uploading.ensure_transition(Ready).unwrap();
    }

    #[test]
    fn only_ready_serves() {
        for s in ALL {
            assert_eq!(s.servable(), s == Ready, "{s:?}");
        }
    }

    #[test]
    fn wire_names_round_trip_serde() {
        for s in ALL {
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(json, format!("\"{}\"", s.as_str()));
            let back: FileState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, s);
        }
    }
}
