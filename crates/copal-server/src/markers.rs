//! The marker ingestion seam, shared by every content-bearing face.
//!
//! Markers describe content, not the record, so they ride exactly the
//! calls that carry or name content: the content PUT (an
//! `x-copal-markers` header, beside the digest and conditional
//! headers the byte routes already speak), `POST /v1/files/fetch` (a
//! `markers` body field), and tus creation (a `markers` key in
//! `Upload-Metadata`). Create-time markers would describe bytes that
//! had not arrived and silently apply to whatever future upload
//! replaced them, wrong in both fail directions at once, so
//! `POST /v1/files` takes none. The S3 face carries none either:
//! S3's vocabulary has no such concept, and inventing an
//! `x-amz-meta-` convention is a decision for the day someone
//! migrating a bucket asks for it - absence there means file-level
//! behavior, which is today's.
//!
//! Every face funnels through [`accept_declaration`], so a malformed
//! or widening declaration refuses with the same words everywhere,
//! before any byte moves. The accepted canonical form persists on the
//! version row; [`resolve_declaration`] reads it back at extraction
//! time and turns it into the spans every enforcement point uses.

use axum::http::HeaderMap;
use serde_json::Value;

use copal_core::marker::{MarkerResolution, ResolveContext, ResolvedSpan};
use copal_core::{AccessLevel, CopalError};
use copal_store::repo::text::{ChunkInput, WithheldSpan};

/// Pull a raw declaration out of the content PUT's headers, parsed as
/// JSON but not yet validated: validation needs the file's level,
/// which the handler holds.
pub fn from_header(headers: &HeaderMap) -> copal_core::Result<Option<Value>> {
    let Some(value) = headers.get("x-copal-markers") else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| CopalError::validation("x-copal-markers must be ASCII JSON"))?;
    let parsed: Value = serde_json::from_str(raw)
        .map_err(|_| CopalError::validation("x-copal-markers does not parse as JSON"))?;
    Ok(Some(parsed))
}

/// Validate a declaration against the file's level and answer the
/// canonical wire form to persist.
///
/// This is the 400-before-bytes moment: an unknown level, a malformed
/// shape, or a marker looser than the file refuses here, on every
/// face, before any content moves. All four levels are accepted; only
/// `grant` changes what a tenant-scoped reader sees today, and the
/// enforcement points say so where they enforce it.
pub fn accept_declaration(raw: &Value, file_level: AccessLevel) -> copal_core::Result<Value> {
    let markers = copal_core::marker::parse_markers(raw)?;
    copal_core::marker::validate_narrowing(&markers, file_level)?;
    Ok(copal_core::marker::markers_to_value(&markers))
}

/// Resolve a persisted declaration against the stored extraction.
///
/// `declared` is whatever the version row holds: `None` (or JSON
/// null) means the upload declared nothing, which resolves to no
/// spans and today's behavior. A persisted declaration that no longer
/// parses cannot name its own level, so it restricts the whole text
/// at `grant`, the strictest word in the vocabulary: the one wrong
/// direction here would be serving a document whose declaration
/// became unreadable.
pub fn resolve_declaration(declared: Option<&Value>, context: &ResolveContext) -> MarkerResolution {
    let raw = match declared {
        None | Some(Value::Null) => return MarkerResolution::default(),
        Some(raw) => raw,
    };
    match copal_core::marker::parse_markers(raw) {
        Ok(markers) => copal_core::marker::resolve(&markers, context),
        Err(error) => MarkerResolution {
            spans: vec![ResolvedSpan {
                start: 0,
                end: context.text.chars().count(),
                access: AccessLevel::Grant,
            }],
            unresolved: vec![format!("declaration does not parse: {error}")],
        },
    }
}

/// Map resolved spans onto split passages: a chunk overlapping ANY
/// marked span inherits the marker's level, the most restrictive
/// where several touch it. The overlap windows make this deliberately
/// coarse - a sensitive sentence's tail appears at the head of the
/// next chunk, and that next chunk inherits too. Over-withholding a
/// neighbor's worth of text is the chosen cost: any finer rule leaves
/// a fragment of a marked span in a chunk that did not inherit, and a
/// fragment of a confidential passage is a leak of the confidential
/// passage.
pub fn leveled_chunks(passages: &[copal_core::Passage], spans: &[ResolvedSpan]) -> Vec<ChunkInput> {
    passages
        .iter()
        .map(|passage| ChunkInput {
            body: passage.body.clone(),
            access: copal_core::marker::chunk_access(spans, passage.start, passage.end),
        })
        .collect()
}

/// The persisted shape of resolved spans, for the document row.
pub fn withheld_spans(spans: &[ResolvedSpan]) -> Vec<WithheldSpan> {
    spans
        .iter()
        .map(|span| WithheldSpan {
            start: span.start,
            end: span.end,
            access: span.access.as_str().to_owned(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_declarations_parse_or_refuse_loudly() {
        let mut headers = HeaderMap::new();
        assert!(from_header(&headers).unwrap().is_none());
        headers.insert(
            "x-copal-markers",
            "[{\"access\": \"grant\", \"from\": \"x\"}]"
                .parse()
                .unwrap(),
        );
        assert!(from_header(&headers).unwrap().is_some());
        headers.insert("x-copal-markers", "not json".parse().unwrap());
        assert!(from_header(&headers).is_err());
    }

    #[test]
    fn acceptance_refuses_widening_and_returns_the_canonical_form() {
        let raw = serde_json::json!([{ "access": "grant", "from": "Pricing" }]);
        let canonical = accept_declaration(&raw, AccessLevel::Private).unwrap();
        assert_eq!(canonical, raw);
        let loose = serde_json::json!([{ "access": "public", "from": "Pricing" }]);
        let refusal = accept_declaration(&loose, AccessLevel::Private).unwrap_err();
        assert!(refusal.to_string().contains("never widen"), "{refusal}");
    }

    #[test]
    fn an_unparseable_persisted_declaration_withholds_everything_at_grant() {
        let context = ResolveContext {
            text: "the whole document",
            native: true,
            lead_trim: 0,
            source_chars: 18,
            truncated: false,
        };
        let resolution = resolve_declaration(Some(&serde_json::json!("garbage")), &context);
        assert_eq!(resolution.unresolved.len(), 1);
        assert_eq!(resolution.spans.len(), 1);
        assert_eq!(resolution.spans[0].access, AccessLevel::Grant);
        assert_eq!(
            (resolution.spans[0].start, resolution.spans[0].end),
            (0, 18),
        );
    }

    #[test]
    fn nothing_declared_resolves_to_nothing() {
        let context = ResolveContext {
            text: "plain",
            native: true,
            lead_trim: 0,
            source_chars: 5,
            truncated: false,
        };
        for declared in [None, Some(Value::Null)] {
            let resolution = resolve_declaration(declared.as_ref(), &context);
            assert!(resolution.spans.is_empty());
            assert!(resolution.unresolved.is_empty());
        }
    }
}
