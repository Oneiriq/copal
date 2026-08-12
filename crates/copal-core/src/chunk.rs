//! Splitting documents into retrievable passages.
//!
//! Retrieval answers "which passage says this", so the unit stored
//! and embedded is a passage, not a file. The split prefers
//! boundaries a reader would recognize (blank lines, then sentence
//! ends) and falls back to a hard cut only when a single run of text
//! exceeds the window, because a chunk that begins mid-clause embeds
//! to something nobody asked about.
//!
//! Consecutive chunks overlap, so a sentence spanning a boundary
//! still appears whole in one of them.

/// Target characters per passage.
pub const CHUNK_CHARS: usize = 1_000;
/// Characters repeated from the previous passage.
pub const CHUNK_OVERLAP: usize = 150;
/// Ceiling on passages per document, so one enormous file cannot
/// dominate an index or a batch of embedding calls.
pub const MAX_CHUNKS: usize = 500;

/// One passage and where it sits in the text it was split from, as
/// half-open character offsets. The span is what lets a marker
/// resolved over the document say which passages it touches: without
/// it, a chunk is a string with no address, and confidentiality
/// cannot be mapped onto it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Passage {
    pub body: String,
    pub start: usize,
    pub end: usize,
}

/// Split text into overlapping passages.
///
/// Returns whole text as a single passage when it fits, so short
/// documents cost one row and one embedding.
pub fn split(text: &str) -> Vec<String> {
    split_spans(text)
        .into_iter()
        .map(|passage| passage.body)
        .collect()
}

/// Split text into overlapping passages, each carrying its span in
/// the input's character coordinates. Bodies are identical to what
/// [`split`] returns; the spans account for the whole-text trim and
/// each passage's own trim, so `text[start..end]` (by characters) IS
/// the passage body.
pub fn split_spans(text: &str) -> Vec<Passage> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    // Offset of the trimmed region within the input, in characters,
    // so spans stay meaningful against the text the caller holds.
    let lead = text.chars().count() - text.trim_start().chars().count();
    let chars: Vec<char> = trimmed.chars().collect();
    if chars.len() <= CHUNK_CHARS {
        return vec![Passage {
            body: trimmed.to_owned(),
            start: lead,
            end: lead + chars.len(),
        }];
    }

    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < chars.len() && chunks.len() < MAX_CHUNKS {
        let hard_end = (start + CHUNK_CHARS).min(chars.len());
        let end = if hard_end == chars.len() {
            hard_end
        } else {
            boundary_before(&chars, start, hard_end)
        };
        if let Some(passage) = trimmed_passage(&chars, start, end, lead) {
            chunks.push(passage);
        }
        if end >= chars.len() {
            break;
        }
        // Step forward with overlap, never backward: a boundary found
        // inside the overlap window would otherwise loop forever.
        start = end.saturating_sub(CHUNK_OVERLAP).max(start + 1);
    }
    chunks
}

/// Trim one sliced window and keep its span honest: the whitespace a
/// passage sheds at its edges moves the offsets inward with it.
fn trimmed_passage(chars: &[char], start: usize, end: usize, lead: usize) -> Option<Passage> {
    let mut from = start;
    let mut to = end;
    while from < to && chars[from].is_whitespace() {
        from += 1;
    }
    while to > from && chars[to - 1].is_whitespace() {
        to -= 1;
    }
    if from == to {
        return None;
    }
    Some(Passage {
        body: chars[from..to].iter().collect(),
        start: lead + from,
        end: lead + to,
    })
}

/// The most reader-recognisable break in `[start, hard_end)`, or the
/// hard end when the window holds no boundary at all.
fn boundary_before(chars: &[char], start: usize, hard_end: usize) -> usize {
    // Only look in the back half: a break near the start would make
    // passages far shorter than the target.
    let floor = start + (hard_end - start) / 2;

    // A blank line is the strongest signal a writer gives.
    let mut index = hard_end;
    while index > floor {
        index -= 1;
        if chars[index] == '\n' && index > 0 && chars[index - 1] == '\n' {
            return index + 1;
        }
    }
    // Then a sentence end followed by space.
    let mut index = hard_end;
    while index > floor {
        index -= 1;
        if matches!(chars[index], '.' | '!' | '?')
            && chars.get(index + 1).is_some_and(|c| c.is_whitespace())
        {
            return index + 1;
        }
    }
    // Then any whitespace, so a passage does not end mid-word.
    let mut index = hard_end;
    while index > floor {
        index -= 1;
        if chars[index].is_whitespace() {
            return index + 1;
        }
    }
    hard_end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_passage() {
        assert_eq!(split("a short note"), vec!["a short note".to_owned()]);
        assert!(split("   ").is_empty());
    }

    #[test]
    fn long_text_splits_with_overlap_and_covers_everything() {
        // Sentences of known length so boundaries are predictable.
        let sentence = "the quick brown fox jumps over the lazy dog. ";
        let document = sentence.repeat(120);
        let chunks = split(&document);

        assert!(chunks.len() > 1, "a long document splits");
        assert!(
            chunks.iter().all(|c| c.chars().count() <= CHUNK_CHARS),
            "no passage exceeds the window",
        );
        // Every passage ends at a sentence boundary rather than
        // mid-clause (the last may end at the document's end).
        for chunk in &chunks[..chunks.len() - 1] {
            assert!(
                chunk.ends_with('.'),
                "passages end where a reader would: {chunk:?}",
            );
        }
        // Consecutive passages overlap, so a sentence crossing a
        // boundary survives whole somewhere.
        let joined: String = chunks.join(" ");
        assert!(joined.len() > document.trim().len(), "passages overlap");
    }

    #[test]
    fn a_single_unbroken_run_still_splits() {
        // No whitespace anywhere: the hard cut is the only option.
        let document = "x".repeat(CHUNK_CHARS * 3);
        let chunks = split(&document);
        assert!(chunks.len() >= 3, "{}", chunks.len());
        assert!(chunks.iter().all(|c| c.chars().count() <= CHUNK_CHARS));
    }

    #[test]
    fn paragraph_breaks_win_over_sentence_ends() {
        let first = "First paragraph. ".repeat(40);
        let second = "Second paragraph. ".repeat(40);
        let document = format!("{first}\n\n{second}");
        let chunks = split(&document);
        assert!(
            chunks[0].trim_end().ends_with("First paragraph."),
            "the blank line is the break: {:?}",
            chunks[0],
        );
    }

    #[test]
    fn passage_count_is_bounded() {
        let document = "word ".repeat(CHUNK_CHARS * MAX_CHUNKS);
        assert_eq!(split(&document).len(), MAX_CHUNKS);
    }

    #[test]
    fn spans_address_their_own_bodies_exactly() {
        // Leading whitespace on the document and boundaries inside
        // it: every span, read back out of the input by characters,
        // must be its passage verbatim, or a marker resolved over
        // the document would land on the wrong passages.
        let sentence = "the quick brown fox jumps over the lazy dog. ";
        let document = format!("   \n{}", sentence.repeat(120));
        let chars: Vec<char> = document.chars().collect();
        let passages = split_spans(&document);
        assert!(passages.len() > 1);
        for passage in &passages {
            let slice: String = chars[passage.start..passage.end].iter().collect();
            assert_eq!(slice, passage.body, "span drifted from its body");
        }
        // The two forms agree on bodies, so nothing downstream can
        // see different text depending on which it called.
        assert_eq!(
            split(&document),
            passages.iter().map(|p| p.body.clone()).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn consecutive_spans_overlap_in_coordinates() {
        let sentence = "the quick brown fox jumps over the lazy dog. ";
        let document = sentence.repeat(120);
        let passages = split_spans(&document);
        for pair in passages.windows(2) {
            assert!(
                pair[1].start < pair[0].end,
                "the overlap window is what makes marker inheritance coarse: \
                 {} !< {}",
                pair[1].start,
                pair[0].end,
            );
        }
    }
}
