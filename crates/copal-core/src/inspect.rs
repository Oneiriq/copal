//! Content inspection: magic-byte sniffing and extension policy.
//!
//! Pure functions; the pipeline activities wrap them. The sniffer is
//! kept small on purpose: it exists to catch declared-type lies and to
//! feed policy, not to be a full type oracle.

/// Sniff a content type from the first bytes of a payload.
///
/// Returns `None` when nothing definitive matches; callers treat that
/// as "unverifiable" rather than treated as binary garbage.
pub fn sniff_content_type(prefix: &[u8]) -> Option<&'static str> {
    const SIGNATURES: &[(&[u8], &str)] = &[
        (b"%PDF-", "application/pdf"),
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
        (b"PK\x03\x04", "application/zip"),
        (b"PK\x05\x06", "application/zip"),
        (b"\x1f\x8b", "application/gzip"),
        (b"\x7fELF", "application/x-executable"),
        (b"MZ", "application/x-msdownload"),
        (b"{", "application/json"),
        (b"[", "application/json"),
    ];
    for (magic, mime) in SIGNATURES {
        if prefix.starts_with(magic) {
            return Some(mime);
        }
    }
    // Printable-or-whitespace ASCII/UTF-8 prefix reads as text.
    if !prefix.is_empty()
        && std::str::from_utf8(prefix)
            .map(|s| {
                s.chars()
                    .all(|c| !c.is_control() || c == '\n' || c == '\r' || c == '\t')
            })
            .unwrap_or(false)
    {
        return Some("text/plain");
    }
    None
}

/// Extension policy: a lowercase denylist matched against the final
/// path segment's extension.
#[derive(Debug, Clone)]
pub struct ExtensionPolicy {
    blocked: Vec<String>,
}

impl ExtensionPolicy {
    /// Build from a comma-separated list (`"exe,bat,ps1"`); entries
    /// normalise to lowercase without dots.
    pub fn from_list(list: &str) -> Self {
        Self {
            blocked: list
                .split(',')
                .map(|e| e.trim().trim_start_matches('.').to_ascii_lowercase())
                .filter(|e| !e.is_empty())
                .collect(),
        }
    }

    /// The default denylist, carried over from the predecessor
    /// service's blocked-extensions config.
    pub fn standard() -> Self {
        Self::from_list("exe,dll,bat,cmd,ps1,sh,php,asp,aspx,jsp,py,msi,scr,com,vbs,js")
    }

    /// Whether `path` ends in a blocked extension.
    pub fn blocks(&self, path: &str) -> Option<&str> {
        let name = path.rsplit('/').next().unwrap_or(path);
        if !name.contains('.') {
            return None;
        }
        let extension = name.rsplit('.').next()?;
        let lowered = extension.to_ascii_lowercase();
        self.blocked
            .iter()
            .find(|blocked| **blocked == lowered)
            .map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures_sniff() {
        assert_eq!(sniff_content_type(b"%PDF-1.7 x"), Some("application/pdf"));
        assert_eq!(
            sniff_content_type(b"\x89PNG\r\n\x1a\nrest"),
            Some("image/png")
        );
        assert_eq!(
            sniff_content_type(b"MZ\x90\x00"),
            Some("application/x-msdownload")
        );
        assert_eq!(sniff_content_type(b"plain words here"), Some("text/plain"));
        assert_eq!(sniff_content_type(b"\x00\x01\x02"), None);
        assert_eq!(sniff_content_type(b""), None);
    }

    #[test]
    fn extension_policy_blocks_case_insensitively() {
        let policy = ExtensionPolicy::standard();
        assert_eq!(policy.blocks("tools/setup.EXE"), Some("exe"));
        assert_eq!(policy.blocks("run.ps1"), Some("ps1"));
        assert!(policy.blocks("docs/report.pdf").is_none());
        assert!(policy.blocks("no_extension").is_none());
        assert!(policy.blocks("dir.d/file").is_none());
    }

    #[test]
    fn custom_lists_normalise() {
        let policy = ExtensionPolicy::from_list(" .Foo, BAR ,,baz");
        assert_eq!(policy.blocks("a.foo"), Some("foo"));
        assert_eq!(policy.blocks("a.BAR"), Some("bar"));
        assert_eq!(policy.blocks("a.baz"), Some("baz"));
        assert!(policy.blocks("a.qux").is_none());
    }
}
