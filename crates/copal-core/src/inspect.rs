//! Content inspection: magic-byte sniffing and extension policy.
//!
//! Pure functions; the pipeline activities wrap them. The sniffer is
//! kept small on purpose: it exists to catch declared-type lies and to
//! feed policy, not to be a full type oracle.

/// One recognisable opening.
///
/// Container formats rarely put their name first. An ISO base media
/// file (mp4, mov, heic, avif) opens with a four-byte box length and
/// names itself at byte four, then declares a brand at byte eight;
/// every RIFF file opens identically and only says what it is at byte
/// eight. So a signature carries where to look, and optionally a
/// second marker that separates a family into its members.
struct Signature {
    at: usize,
    magic: &'static [u8],
    then: Option<(usize, &'static [u8])>,
    mime: &'static str,
}

const fn sig(at: usize, magic: &'static [u8], mime: &'static str) -> Signature {
    Signature {
        at,
        magic,
        then: None,
        mime,
    }
}

const fn sig2(
    at: usize,
    magic: &'static [u8],
    then: (usize, &'static [u8]),
    mime: &'static str,
) -> Signature {
    Signature {
        at,
        magic,
        then: Some(then),
        mime,
    }
}

/// Whether this is a Windows executable, structurally rather than by
/// its opening letters. A DOS binary carrying no PE header reads as
/// unverifiable instead, which blocks nothing; the extension policy
/// still refuses the name.
fn looks_like_pe(prefix: &[u8]) -> bool {
    if !at(prefix, 0, b"MZ") {
        return false;
    }
    let Some(field) = prefix.get(0x3c..0x40) else {
        return false;
    };
    let start = u32::from_le_bytes([field[0], field[1], field[2], field[3]]) as usize;
    at(prefix, start, b"PE\x00\x00")
}

fn at(prefix: &[u8], offset: usize, magic: &[u8]) -> bool {
    prefix.len() >= offset + magic.len() && &prefix[offset..offset + magic.len()] == magic
}

/// Sniff a content type from the first bytes of a payload.
///
/// Returns `None` when nothing definitive matches; callers treat that
/// as "unverifiable" rather than treated as binary garbage.
pub fn sniff_content_type(prefix: &[u8]) -> Option<&'static str> {
    const SIGNATURES: &[Signature] = &[
        sig(0, b"%PDF-", "application/pdf"),
        sig(0, b"\x89PNG\r\n\x1a\n", "image/png"),
        sig(0, b"\xff\xd8\xff", "image/jpeg"),
        sig(0, b"GIF87a", "image/gif"),
        sig(0, b"GIF89a", "image/gif"),
        // The RIFF family names itself at byte eight, so these come
        // before anything that would match the container alone.
        sig2(0, b"RIFF", (8, b"WEBP"), "image/webp"),
        sig2(0, b"RIFF", (8, b"WAVE"), "audio/wav"),
        sig2(0, b"RIFF", (8, b"AVI "), "video/x-msvideo"),
        // ISO base media: the brand at byte eight decides whether the
        // same container is a video, an audio track, or a still.
        sig2(4, b"ftyp", (8, b"avif"), "image/avif"),
        sig2(4, b"ftyp", (8, b"heic"), "image/heic"),
        sig2(4, b"ftyp", (8, b"heix"), "image/heic"),
        sig2(4, b"ftyp", (8, b"mif1"), "image/heif"),
        sig2(4, b"ftyp", (8, b"M4A "), "audio/mp4"),
        sig2(4, b"ftyp", (8, b"qt  "), "video/quicktime"),
        // Everything else carrying an ftyp box is mp4 in practice.
        sig(4, b"ftyp", "video/mp4"),
        sig(0, b"OggS", "audio/ogg"),
        sig(0, b"fLaC", "audio/flac"),
        // MPEG audio frame syncs. The ID3-tagged case is handled
        // below, where a version byte separates a tag from prose.
        sig(0, b"\xff\xfb", "audio/mpeg"),
        sig(0, b"\xff\xf3", "audio/mpeg"),
        sig(0, b"\xff\xf2", "audio/mpeg"),
        sig(0, b"II*\x00", "image/tiff"),
        sig(0, b"MM\x00*", "image/tiff"),
        sig(0, b"PAR1", "application/vnd.apache.parquet"),
        sig(0, b"PK\x03\x04", "application/zip"),
        sig(0, b"PK\x05\x06", "application/zip"),
        sig(0, b"\x1f\x8b", "application/gzip"),
        sig(0, b"\x28\xb5\x2f\xfd", "application/zstd"),
        sig(0, b"\xfd7zXZ\x00", "application/x-xz"),
        sig(0, b"7z\xbc\xaf\x27\x1c", "application/x-7z-compressed"),
        sig(0, b"\x7fELF", "application/x-executable"),
        sig(0, b"{", "application/json"),
        sig(0, b"[", "application/json"),
    ];
    for signature in SIGNATURES {
        if !at(prefix, signature.at, signature.magic) {
            continue;
        }
        match signature.then {
            Some((offset, marker)) if !at(prefix, offset, marker) => continue,
            _ => return Some(signature.mime),
        }
    }
    // Matroska and WebM share an EBML header and separate themselves
    // by a DocType string that sits at no fixed offset.
    if at(prefix, 0, b"\x1a\x45\xdf\xa3") {
        let window = &prefix[..prefix.len().min(64)];
        return Some(if window.windows(4).any(|w| w == b"webm") {
            "video/webm"
        } else {
            "video/x-matroska"
        });
    }
    // "MZ" is two ordinary letters, so a document opening with them
    // would read as an executable and, with type matching enforced,
    // be quarantined for it. A real Windows binary says at byte 0x3c
    // where its PE header begins, and that header names itself.
    if looks_like_pe(prefix) {
        return Some("application/x-msdownload");
    }
    // An ID3 tag carries a major version byte that prose starting with
    // the same three letters will not.
    if at(prefix, 0, b"ID3") && prefix.len() > 3 && prefix[3] <= 4 {
        return Some("audio/mpeg");
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
            sniff_content_type(&pe_header()),
            Some("application/x-msdownload")
        );
        assert_eq!(sniff_content_type(b"plain words here"), Some("text/plain"));
        assert_eq!(sniff_content_type(b"\x00\x01\x02"), None);
        assert_eq!(sniff_content_type(b""), None);
    }

    /// One container, several meanings. RIFF and ISO base media both
    /// hold their identity past the opening bytes, so a sniffer that
    /// reads only the first four calls a video a still.
    #[test]
    fn shared_containers_separate_by_their_second_marker() {
        let riff = |kind: &[u8]| {
            let mut bytes = b"RIFF\x24\x08\x00\x00".to_vec();
            bytes.extend_from_slice(kind);
            bytes.extend_from_slice(b"fmt payload");
            bytes
        };
        assert_eq!(sniff_content_type(&riff(b"WAVE")), Some("audio/wav"));
        assert_eq!(sniff_content_type(&riff(b"AVI ")), Some("video/x-msvideo"));
        assert_eq!(sniff_content_type(&riff(b"WEBP")), Some("image/webp"));

        let iso = |brand: &[u8]| {
            let mut bytes = b"\x00\x00\x00\x20ftyp".to_vec();
            bytes.extend_from_slice(brand);
            bytes.extend_from_slice(b"\x00\x00\x02\x00mp41");
            bytes
        };
        assert_eq!(sniff_content_type(&iso(b"isom")), Some("video/mp4"));
        assert_eq!(sniff_content_type(&iso(b"mp42")), Some("video/mp4"));
        assert_eq!(sniff_content_type(&iso(b"avif")), Some("image/avif"));
        assert_eq!(sniff_content_type(&iso(b"heic")), Some("image/heic"));
        assert_eq!(sniff_content_type(&iso(b"M4A ")), Some("audio/mp4"));
        assert_eq!(sniff_content_type(&iso(b"qt  ")), Some("video/quicktime"));
    }

    /// Matroska and WebM open identically and name themselves in a
    /// DocType that sits at no fixed offset.
    #[test]
    fn matroska_and_webm_are_told_apart() {
        let mut webm = b"\x1a\x45\xdf\xa3\x01\x00\x00\x00".to_vec();
        webm.extend_from_slice(b"\x42\x82\x84webm rest");
        assert_eq!(sniff_content_type(&webm), Some("video/webm"));

        let mut mkv = b"\x1a\x45\xdf\xa3\x01\x00\x00\x00".to_vec();
        mkv.extend_from_slice(b"\x42\x82\x88matroska");
        assert_eq!(sniff_content_type(&mkv), Some("video/x-matroska"));
    }

    #[test]
    fn media_and_columnar_formats_sniff() {
        assert_eq!(sniff_content_type(b"OggS\x00\x02rest"), Some("audio/ogg"));
        assert_eq!(
            sniff_content_type(b"fLaC\x00\x00\x00\x22"),
            Some("audio/flac"),
        );
        assert_eq!(sniff_content_type(b"\xff\xfb\x90\x00"), Some("audio/mpeg"));
        assert_eq!(sniff_content_type(b"II*\x00\x08\x00"), Some("image/tiff"));
        assert_eq!(
            sniff_content_type(b"PAR1\x15\x04\x15rest"),
            Some("application/vnd.apache.parquet"),
        );
        assert_eq!(
            sniff_content_type(b"\x28\xb5\x2f\xfd\x00\x48"),
            Some("application/zstd"),
        );
    }

    /// A signature that is also ordinary prose must not swallow the
    /// prose. An ID3 tag carries a version byte; a sentence does not.
    #[test]
    fn prose_that_opens_like_a_tag_stays_text() {
        assert_eq!(
            sniff_content_type(b"ID3 tags are how mp3 files carry metadata."),
            Some("text/plain"),
        );
        let mut tagged = b"ID3\x04\x00\x00".to_vec();
        tagged.extend_from_slice(b"\x00\x00\x3fTIT2");
        assert_eq!(sniff_content_type(&tagged), Some("audio/mpeg"));
    }

    /// Short and empty inputs reach the offset checks and must not
    /// panic on them.
    #[test]
    fn truncated_input_is_survivable() {
        // A truncated container that is still printable falls
        // through to the text check, which is the honest answer.
        assert_eq!(sniff_content_type(b"RIFF"), Some("text/plain"));
        assert_eq!(sniff_content_type(b"\x00\x00\x00\x20ft"), None);
        assert_eq!(sniff_content_type(b"\x1a\x45"), None);
        assert_eq!(sniff_content_type(b"ID"), Some("text/plain"));
        assert_eq!(sniff_content_type(b""), None);
    }

    /// A Windows executable, structurally: the stub says at byte 0x3c
    /// where the PE header begins, and the header names itself there.
    fn pe_header() -> Vec<u8> {
        let mut bytes = vec![0u8; 0x88];
        bytes[..2].copy_from_slice(b"MZ");
        bytes[0x3c..0x40].copy_from_slice(&0x80u32.to_le_bytes());
        bytes[0x80..0x84].copy_from_slice(b"PE\x00\x00");
        bytes
    }

    /// Two ordinary letters are not an executable. A memo opening with
    /// them was read as one, and with type matching enforced that is a
    /// quarantined document.
    #[test]
    fn prose_opening_with_mz_is_not_an_executable() {
        assert_eq!(
            sniff_content_type(b"MZ said the machine, and the room went quiet."),
            Some("text/plain"),
        );
        assert_eq!(
            sniff_content_type(&pe_header()),
            Some("application/x-msdownload")
        );

        // A stub pointing past what was read cannot be confirmed, so it
        // reads as unverifiable, which blocks nothing.
        let mut unverifiable = pe_header();
        unverifiable[0x3c..0x40].copy_from_slice(&0xffff_0000u32.to_le_bytes());
        assert_eq!(sniff_content_type(&unverifiable), None);
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
