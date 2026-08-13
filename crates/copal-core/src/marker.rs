//! Confidentiality markers: spans an uploader declares over content.
//!
//! A marker names a span of a document and the access level of that
//! span. The uploader is the only party in the path who knows which
//! passages are sensitive, so the declaration arrives with the
//! content and nothing here ever infers one. Two addressing forms
//! share one input shape: character ranges over the uploaded content
//! (exact where Copal decoded the bytes itself, meaningless through
//! an external extractor) and text anchors that quote the document at
//! itself (resolved against the extracted text, which is the text
//! that will be chunked, so they survive extraction by construction).
//!
//! One rule governs meaning: a marker narrows, never widens. And one
//! rule governs failure: a declaration that cannot be located
//! restricts the whole file, because marking nothing would turn a
//! resolution failure into disclosure of exactly the passage the
//! uploader tried to protect. Mapping failure costs availability,
//! never confidentiality.

use serde_json::{json, Value};

use crate::error::CopalError;
use crate::state::AccessLevel;

/// Ceiling on markers per upload. A declaration is a handful of
/// sensitive spans, not a per-line annotation format; the cap keeps
/// resolution work bounded by something the uploader chose.
pub const MAX_MARKERS: usize = 64;

/// Ceiling on anchor text length, in characters. An anchor quotes a
/// recognizable phrase; a ten-thousand-character anchor is a paste
/// error, and every anchor is searched against the whole document.
pub const MAX_ANCHOR_CHARS: usize = 512;

/// One declared marker: an access level over an addressed span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    pub access: AccessLevel,
    pub address: MarkerAddress,
}

/// How a marker names its span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerAddress {
    /// Character offsets over the uploaded content, half-open. Valid
    /// only where the extracted text IS the decoded upload (`native`
    /// extraction); through an external extractor the correspondence
    /// collapses and resolution records the marker unresolvable.
    Range { start: usize, end: usize },
    /// A quoted span: begins at each occurrence of `from`, ends at
    /// the next occurrence of `until` after it (absent `until`, the
    /// document's end). A pair matching several times marks every
    /// match, which is the fail-safe reading of ambiguity.
    Anchor { from: String, until: Option<String> },
}

impl Marker {
    /// The wire shape, for persistence beside the content it
    /// describes. Round-trips through [`parse_markers`].
    pub fn to_value(&self) -> Value {
        match &self.address {
            MarkerAddress::Range { start, end } => json!({
                "access": self.access.as_str(),
                "range": { "start": start, "end": end },
            }),
            MarkerAddress::Anchor { from, until } => match until {
                Some(until) => json!({
                    "access": self.access.as_str(),
                    "from": from,
                    "until": until,
                }),
                None => json!({
                    "access": self.access.as_str(),
                    "from": from,
                }),
            },
        }
    }
}

/// Render a whole declaration as the wire array.
pub fn markers_to_value(markers: &[Marker]) -> Value {
    Value::Array(markers.iter().map(Marker::to_value).collect())
}

/// Parse the wire shape: an array of `{access, range: {start, end}}`
/// and `{access, from, until?}` objects.
///
/// Strict on purpose. A misspelled key in a confidentiality
/// declaration must refuse loudly at the API rather than resolve to
/// nothing at extraction: `{"form": "Pricing"}` silently accepted
/// would be an uploader who believes a span is protected when no
/// marker exists. Every key is checked, exactly one addressing form
/// is required, and the vocabulary is the file vocabulary, all four
/// levels. Only `grant` changes what a tenant-scoped reader sees
/// today; the rest are accepted now so the read-path divergence
/// principals are building toward reaches chunks with no wire change.
pub fn parse_markers(raw: &Value) -> crate::Result<Vec<Marker>> {
    let items = raw
        .as_array()
        .ok_or_else(|| CopalError::validation("markers must be an array"))?;
    if items.len() > MAX_MARKERS {
        return Err(CopalError::validation(format!(
            "at most {MAX_MARKERS} markers per upload",
        )));
    }
    items.iter().map(parse_marker).collect()
}

fn parse_marker(item: &Value) -> crate::Result<Marker> {
    let object = item
        .as_object()
        .ok_or_else(|| CopalError::validation("each marker must be an object"))?;
    for key in object.keys() {
        if !matches!(key.as_str(), "access" | "range" | "from" | "until") {
            return Err(CopalError::validation(format!(
                "unknown marker key {key:?}; markers carry access with range, or from and until",
            )));
        }
    }
    let access = object
        .get("access")
        .and_then(Value::as_str)
        .ok_or_else(|| CopalError::validation("each marker must name an access level"))?;
    let access = AccessLevel::parse(access)?;

    let range = object.get("range");
    let from = object.get("from");
    match (range, from) {
        (Some(range), None) => {
            if object.contains_key("until") {
                return Err(CopalError::validation(
                    "a range marker takes no until; anchors and ranges do not mix",
                ));
            }
            let range = range
                .as_object()
                .ok_or_else(|| CopalError::validation("range must be {start, end}"))?;
            for key in range.keys() {
                if !matches!(key.as_str(), "start" | "end") {
                    return Err(CopalError::validation(format!(
                        "unknown range key {key:?}; a range is {{start, end}}",
                    )));
                }
            }
            let start = range
                .get("start")
                .and_then(Value::as_u64)
                .ok_or_else(|| CopalError::validation("range.start must be a whole number"))?;
            let end = range
                .get("end")
                .and_then(Value::as_u64)
                .ok_or_else(|| CopalError::validation("range.end must be a whole number"))?;
            if end <= start {
                return Err(CopalError::validation(
                    "range.end must be greater than range.start",
                ));
            }
            Ok(Marker {
                access,
                address: MarkerAddress::Range {
                    start: start as usize,
                    end: end as usize,
                },
            })
        }
        (None, Some(from)) => {
            let from = from
                .as_str()
                .ok_or_else(|| CopalError::validation("from must be a string"))?;
            let until = match object.get("until") {
                None => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .ok_or_else(|| CopalError::validation("until must be a string"))?
                        .to_owned(),
                ),
            };
            if from.is_empty() {
                return Err(CopalError::validation("from must not be empty"));
            }
            for (name, text) in std::iter::once(("from", from))
                .chain(until.as_deref().map(|value| ("until", value)))
            {
                if text.chars().count() > MAX_ANCHOR_CHARS {
                    return Err(CopalError::validation(format!(
                        "{name} exceeds {MAX_ANCHOR_CHARS} characters",
                    )));
                }
                if text.is_empty() {
                    return Err(CopalError::validation(format!("{name} must not be empty")));
                }
            }
            Ok(Marker {
                access,
                address: MarkerAddress::Anchor {
                    from: from.to_owned(),
                    until,
                },
            })
        }
        (Some(_), Some(_)) => Err(CopalError::validation(
            "a marker addresses by range or by anchor, not both",
        )),
        (None, None) => Err(CopalError::validation(
            "a marker must carry a range or a from anchor",
        )),
    }
}

/// Refuse any marker looser than the file. Search is
/// tenant-authenticated on every face today, so a widening marker
/// could not reach anyone new yet; the moment an anonymous retrieval
/// surface exists, a widening marker becomes a leak vector that every
/// marked upload in history has already armed. Refusing now, before
/// any byte moves, means historical markers can only ever have
/// withheld too much.
pub fn validate_narrowing(markers: &[Marker], file: AccessLevel) -> crate::Result<()> {
    for marker in markers {
        if !marker.access.narrows(file) {
            return Err(CopalError::validation(format!(
                "marker level {} is looser than the file's {}; markers narrow, never widen",
                marker.access.as_str(),
                file.as_str(),
            )));
        }
    }
    Ok(())
}

/// One located span over the stored extraction, in character offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSpan {
    pub start: usize,
    pub end: usize,
    pub access: AccessLevel,
}

impl ResolvedSpan {
    /// The persisted shape, written beside the stored body so the
    /// full-text read path can enforce without re-resolving anchors.
    pub fn to_value(&self) -> Value {
        json!({ "start": self.start, "end": self.end, "access": self.access.as_str() })
    }
}

/// What resolution needs to know about the extraction it resolves
/// against. The stored text is what chunking splits and what the
/// full-text read serves, so spans in its coordinates are the ones
/// every enforcement point can use directly.
#[derive(Debug, Clone, Copy)]
pub struct ResolveContext<'a> {
    /// The stored extraction: trimmed, possibly truncated.
    pub text: &'a str,
    /// Whether Copal decoded the content itself. Ranges are exact
    /// there and meaningless through an extractor.
    pub native: bool,
    /// Characters trimmed from the head of the decoded upload before
    /// storage, so ranges declared over the upload map onto the
    /// stored text without shifting what the uploader chose.
    pub lead_trim: usize,
    /// Characters in the decoded upload, before trim and truncation.
    /// A range ending past this points at content that never existed.
    pub source_chars: usize,
    /// Whether storage stopped at the extraction ceiling. A range
    /// ending past a truncated text names characters nobody stored,
    /// which is a declaration that cannot be honored.
    pub truncated: bool,
}

/// The outcome of resolving one declaration.
#[derive(Debug, Clone, Default)]
pub struct MarkerResolution {
    /// Every located span, whole-file spans for failures included.
    pub spans: Vec<ResolvedSpan>,
    /// Why each unresolvable marker failed, for the processing
    /// verdict the uploader reads back.
    pub unresolved: Vec<String>,
}

/// Resolve a declaration against the stored extraction.
///
/// A marker that cannot be located applies its level to the WHOLE
/// text: the span lands as `(0, len)` and the reason is recorded.
/// The alternative, marking nothing, would disclose exactly the
/// passage the declaration tried to protect, so failure costs
/// availability and never confidentiality. Callers surface the
/// reasons under `metadata.processing`, where the uploader can see
/// the miss and re-upload a corrected declaration.
pub fn resolve(markers: &[Marker], ctx: &ResolveContext) -> MarkerResolution {
    let chars: Vec<char> = ctx.text.chars().collect();
    let len = chars.len();
    let mut resolution = MarkerResolution::default();
    for (index, marker) in markers.iter().enumerate() {
        match &marker.address {
            MarkerAddress::Range { start, end } => match resolve_range(*start, *end, len, ctx) {
                Ok((start, end)) => resolution.spans.push(ResolvedSpan {
                    start,
                    end,
                    access: marker.access,
                }),
                Err(reason) => {
                    resolution.spans.push(ResolvedSpan {
                        start: 0,
                        end: len,
                        access: marker.access,
                    });
                    resolution
                        .unresolved
                        .push(format!("marker {index}: {reason}"));
                }
            },
            MarkerAddress::Anchor { from, until } => {
                match resolve_anchor(&chars, from, until.as_deref()) {
                    Ok(spans) => {
                        for (start, end) in spans {
                            resolution.spans.push(ResolvedSpan {
                                start,
                                end,
                                access: marker.access,
                            });
                        }
                    }
                    Err(reason) => {
                        resolution.spans.push(ResolvedSpan {
                            start: 0,
                            end: len,
                            access: marker.access,
                        });
                        resolution
                            .unresolved
                            .push(format!("marker {index}: {reason}"));
                    }
                }
            }
        }
    }
    resolution
}

/// Map one declared range from upload coordinates onto the stored
/// text, or say why it cannot be.
fn resolve_range(
    start: usize,
    end: usize,
    text_len: usize,
    ctx: &ResolveContext,
) -> Result<(usize, usize), String> {
    if !ctx.native {
        // A PDF's bytes bear no offset relationship to the text an
        // extractor returns; honoring the range would mark spans
        // nobody chose.
        return Err("character ranges apply to natively decoded content only".to_owned());
    }
    if end > ctx.source_chars {
        return Err(format!(
            "range ends at {end} but the content has {} characters",
            ctx.source_chars,
        ));
    }
    let mapped_start = start.saturating_sub(ctx.lead_trim);
    let mapped_end = end.saturating_sub(ctx.lead_trim);
    if ctx.truncated && mapped_end > text_len {
        return Err(format!(
            "range ends past the extraction ceiling at {text_len} characters",
        ));
    }
    // Past here any overhang is trimmed whitespace, which carries no
    // text to protect; the clamp keeps exactly the marked characters
    // that were stored.
    let mapped_start = mapped_start.min(text_len);
    let mapped_end = mapped_end.min(text_len);
    if mapped_start >= mapped_end {
        return Err("range covers no stored text".to_owned());
    }
    Ok((mapped_start, mapped_end))
}

/// Locate every span an anchor pair names, or say why none can be.
fn resolve_anchor(
    chars: &[char],
    from: &str,
    until: Option<&str>,
) -> Result<Vec<(usize, usize)>, String> {
    let from_chars: Vec<char> = from.chars().collect();
    let from_hits = occurrences(chars, &from_chars);
    if from_hits.is_empty() {
        return Err(format!(
            "anchor {from:?} never occurs in the extracted text"
        ));
    }
    let until_hits = match until {
        None => Vec::new(),
        Some(until) => {
            let until_chars: Vec<char> = until.chars().collect();
            let hits = occurrences(chars, &until_chars);
            if hits.is_empty() {
                // The declared endpoint exists nowhere, so no span
                // this pair describes can be located at all.
                return Err(format!(
                    "anchor {until:?} never occurs in the extracted text",
                ));
            }
            hits.into_iter()
                .map(|start| (start, start + until.chars().count()))
                .collect()
        }
    };
    let mut spans = Vec::new();
    for from_start in from_hits {
        let search_from = from_start + from_chars.len();
        let end = until_hits
            .iter()
            .find(|(start, _)| *start >= search_from)
            // The until text occurs, just not after this match; the
            // span runs to the document's end, which over-withholds
            // and never under.
            .map_or(chars.len(), |(_, end)| *end);
        spans.push((from_start, end));
    }
    Ok(spans)
}

/// Every character offset where `needle` begins in `haystack`.
/// Overlapping occurrences count; each is somewhere the uploader's
/// quoted text appears, and each starts a span.
fn occurrences(haystack: &[char], needle: &[char]) -> Vec<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return Vec::new();
    }
    (0..=haystack.len() - needle.len())
        .filter(|&at| haystack[at..at + needle.len()] == *needle)
        .collect()
}

/// The level a chunk inherits from the spans it overlaps: the most
/// restrictive among them, or nothing where none touch it.
///
/// Any overlap inherits, deliberately. Chunk windows share text with
/// their neighbors, so a sensitive sentence's tail appears at the
/// head of the next chunk; a finer rule (a threshold, splitting at
/// marker edges) creates cases where a fragment of a marked span
/// sits in a chunk that did not inherit, and a fragment of a
/// confidential passage is a leak of the confidential passage. Where
/// chunking and confidentiality collide, retrieval loses a passage
/// and confidentiality loses nothing.
pub fn chunk_access(spans: &[ResolvedSpan], start: usize, end: usize) -> Option<AccessLevel> {
    spans
        .iter()
        .filter(|span| span.start < end && start < span.end)
        .map(|span| span.access)
        .max_by_key(|access| access.restrictiveness())
}

/// Cut the given spans out of a text, answering the served remainder
/// and how many contiguous regions went. Overlapping and adjacent
/// spans merge first, so the count says how many gaps a reader sees
/// rather than how many markers produced them: span positions and
/// lengths are not disclosed, because the length of a secret is part
/// of the secret.
pub fn elide(text: &str, spans: &[(usize, usize)]) -> (String, usize) {
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    let mut clamped: Vec<(usize, usize)> = spans
        .iter()
        .map(|&(start, end)| (start.min(len), end.min(len)))
        .filter(|(start, end)| start < end)
        .collect();
    clamped.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in clamped {
        match merged.last_mut() {
            Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
            _ => merged.push((start, end)),
        }
    }
    let mut served = String::new();
    let mut at = 0usize;
    for &(start, end) in &merged {
        served.extend(&chars[at..start]);
        at = end;
    }
    served.extend(&chars[at..]);
    (served, merged.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(text: &str) -> ResolveContext<'_> {
        ResolveContext {
            text,
            native: true,
            lead_trim: 0,
            source_chars: text.chars().count(),
            truncated: false,
        }
    }

    #[test]
    fn the_wire_shape_round_trips() {
        let raw = serde_json::json!([
            { "access": "grant", "range": { "start": 12, "end": 34 } },
            { "access": "private", "from": "Pricing Schedule", "until": "Appendix B" },
            { "access": "grant", "from": "Salaries" },
        ]);
        let markers = parse_markers(&raw).unwrap();
        assert_eq!(markers.len(), 3);
        assert_eq!(markers_to_value(&markers), raw);
    }

    #[test]
    fn malformed_declarations_refuse_by_name() {
        for (raw, needle) in [
            (serde_json::json!({}), "array"),
            (serde_json::json!([[]]), "object"),
            (
                serde_json::json!([{ "range": { "start": 0, "end": 1 } }]),
                "access level",
            ),
            (
                serde_json::json!([{ "access": "secret", "from": "x" }]),
                "unknown access",
            ),
            (
                serde_json::json!([{ "access": "grant" }]),
                "range or a from",
            ),
            (
                serde_json::json!([{ "access": "grant", "form": "typo" }]),
                "unknown marker key",
            ),
            (
                serde_json::json!([{ "access": "grant", "range": { "start": 5, "end": 5 } }]),
                "greater than",
            ),
            (
                serde_json::json!([
                    { "access": "grant", "range": { "start": 0, "end": 1 }, "from": "x" }
                ]),
                "not both",
            ),
            (
                serde_json::json!([
                    { "access": "grant", "range": { "start": 0, "end": 1 }, "until": "x" }
                ]),
                "no until",
            ),
            (
                serde_json::json!([{ "access": "grant", "from": "" }]),
                "empty",
            ),
        ] {
            let err = parse_markers(&raw).expect_err(&raw.to_string()).to_string();
            assert!(err.contains(needle), "{raw}: {err}");
        }
    }

    #[test]
    fn widening_markers_refuse_before_any_byte_moves() {
        let markers = parse_markers(&serde_json::json!([
            { "access": "tenant", "from": "x" }
        ]))
        .unwrap();
        assert!(validate_narrowing(&markers, AccessLevel::Private).is_err());
        assert!(validate_narrowing(&markers, AccessLevel::Tenant).is_ok());
        assert!(validate_narrowing(&markers, AccessLevel::Public).is_ok());
    }

    #[test]
    fn ranges_resolve_exactly_over_native_text() {
        let text = "public part CONFIDENTIAL SECTION public again";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "range": { "start": 12, "end": 32 } }
        ]))
        .unwrap();
        let resolution = resolve(&markers, &ctx(text));
        assert!(resolution.unresolved.is_empty());
        assert_eq!(resolution.spans.len(), 1);
        let span = &resolution.spans[0];
        let marked: String = text
            .chars()
            .skip(span.start)
            .take(span.end - span.start)
            .collect();
        assert_eq!(marked, "CONFIDENTIAL SECTION");
    }

    #[test]
    fn ranges_survive_the_lead_trim_without_shifting() {
        // The uploader counted characters in the upload, whitespace
        // included; storage trimmed three of them from the head.
        let upload = "   abcdefghij";
        let stored = upload.trim();
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "range": { "start": 6, "end": 9 } }
        ]))
        .unwrap();
        let resolution = resolve(
            &markers,
            &ResolveContext {
                text: stored,
                native: true,
                lead_trim: 3,
                source_chars: upload.chars().count(),
                truncated: false,
            },
        );
        assert!(resolution.unresolved.is_empty());
        assert_eq!((resolution.spans[0].start, resolution.spans[0].end), (3, 6));
        let marked: String = stored.chars().skip(3).take(3).collect();
        assert_eq!(marked, "def");
    }

    #[test]
    fn a_range_past_the_content_restricts_the_whole_file() {
        let text = "short";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "range": { "start": 2, "end": 400 } }
        ]))
        .unwrap();
        let resolution = resolve(&markers, &ctx(text));
        assert_eq!(resolution.unresolved.len(), 1);
        assert_eq!(
            (resolution.spans[0].start, resolution.spans[0].end),
            (0, text.chars().count()),
            "the failure withholds everything rather than nothing",
        );
    }

    #[test]
    fn a_range_on_extracted_content_restricts_the_whole_file() {
        let text = "text an extractor produced";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "range": { "start": 0, "end": 4 } }
        ]))
        .unwrap();
        let mut context = ctx(text);
        context.native = false;
        let resolution = resolve(&markers, &context);
        assert_eq!(resolution.unresolved.len(), 1);
        assert!(resolution.unresolved[0].contains("natively decoded"));
        assert_eq!(
            (resolution.spans[0].start, resolution.spans[0].end),
            (0, text.chars().count()),
        );
    }

    #[test]
    fn a_range_past_the_truncation_ceiling_restricts_the_whole_file() {
        // The upload held more text than storage kept; a range ending
        // in the lost tail cannot be honored.
        let stored = "kept";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "range": { "start": 2, "end": 8 } }
        ]))
        .unwrap();
        let resolution = resolve(
            &markers,
            &ResolveContext {
                text: stored,
                native: true,
                lead_trim: 0,
                source_chars: 10,
                truncated: true,
            },
        );
        assert_eq!(resolution.unresolved.len(), 1);
        assert!(resolution.unresolved[0].contains("ceiling"));
    }

    #[test]
    fn anchors_mark_from_the_quote_to_the_end_of_the_until_text() {
        let text = "intro Pricing Schedule secret numbers Appendix B outro";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "from": "Pricing Schedule", "until": "Appendix B" }
        ]))
        .unwrap();
        let resolution = resolve(&markers, &ctx(text));
        assert!(resolution.unresolved.is_empty());
        let span = &resolution.spans[0];
        let marked: String = text
            .chars()
            .skip(span.start)
            .take(span.end - span.start)
            .collect();
        // Both anchor texts sit inside the span: including the
        // endpoint text over-withholds a few words, excluding it
        // would serve the phrase that names the secret.
        assert_eq!(marked, "Pricing Schedule secret numbers Appendix B");
    }

    #[test]
    fn an_anchor_without_until_runs_to_the_document_end() {
        let text = "open part Salaries and everything after";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "from": "Salaries" }
        ]))
        .unwrap();
        let resolution = resolve(&markers, &ctx(text));
        let span = &resolution.spans[0];
        assert_eq!(span.end, text.chars().count());
        assert_eq!(span.start, "open part ".chars().count());
    }

    #[test]
    fn an_ambiguous_anchor_marks_every_match() {
        let text = "a SECRET one b SECRET two c";
        let markers = parse_markers(&serde_json::json!([
            { "access": "grant", "from": "SECRET" }
        ]))
        .unwrap();
        let resolution = resolve(&markers, &ctx(text));
        // Two occurrences, two spans; both run to wherever their
        // terminator is, here the document's end.
        assert_eq!(resolution.spans.len(), 2);
        assert!(resolution.spans[0].start < resolution.spans[1].start);
    }

    #[test]
    fn an_anchor_that_never_occurs_restricts_the_whole_file() {
        let text = "nothing in here matches";
        for declaration in [
            serde_json::json!([{ "access": "grant", "from": "Pricing" }]),
            serde_json::json!([{ "access": "grant", "from": "nothing", "until": "Appendix" }]),
        ] {
            let markers = parse_markers(&declaration).unwrap();
            let resolution = resolve(&markers, &ctx(text));
            assert_eq!(resolution.unresolved.len(), 1, "{declaration}");
            assert_eq!(
                (resolution.spans[0].start, resolution.spans[0].end),
                (0, text.chars().count()),
                "{declaration}",
            );
        }
    }

    #[test]
    fn chunks_inherit_on_any_overlap_and_take_the_strictest_level() {
        let spans = vec![
            ResolvedSpan {
                start: 10,
                end: 20,
                access: AccessLevel::Private,
            },
            ResolvedSpan {
                start: 18,
                end: 25,
                access: AccessLevel::Grant,
            },
        ];
        // A chunk grazing the tail of the private span only.
        assert_eq!(chunk_access(&spans, 0, 11), Some(AccessLevel::Private));
        // A chunk overlapping both takes the strictest.
        assert_eq!(chunk_access(&spans, 15, 30), Some(AccessLevel::Grant));
        // A chunk touching neither inherits nothing.
        assert_eq!(chunk_access(&spans, 25, 40), None);
        // Half-open ends: a chunk ending exactly where a span begins
        // does not overlap it.
        assert_eq!(chunk_access(&spans, 0, 10), None);
    }

    #[test]
    fn elision_merges_overlaps_and_counts_regions_not_markers() {
        let text = "aaaa SECRET bbbb HIDDEN cccc";
        let (served, withheld) = elide(text, &[(5, 11), (8, 11), (17, 23)]);
        assert_eq!(served, "aaaa  bbbb  cccc");
        assert_eq!(withheld, 2, "two gaps, though three spans were cut");
        // Nothing marked serves everything.
        let (all, none) = elide(text, &[]);
        assert_eq!(all, text);
        assert_eq!(none, 0);
        // A whole-file span serves nothing and says one region went.
        let (nothing, one) = elide(text, &[(0, text.chars().count())]);
        assert_eq!(nothing, "");
        assert_eq!(one, 1);
    }

    #[test]
    fn elision_offsets_are_characters_not_bytes() {
        let text = "héllo wörld";
        // Chars 2..4 are "ll": a byte-offset reading would split the
        // two-byte é and panic or cut the wrong letters.
        let (served, withheld) = elide(text, &[(2, 4)]);
        assert_eq!(served, "héo wörld");
        assert_eq!(withheld, 1);
    }
}
