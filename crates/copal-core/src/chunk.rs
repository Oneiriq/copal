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

/// Split text into overlapping passages.
///
/// Returns whole text as a single passage when it fits, so short
/// documents cost one row and one embedding.
pub fn split(text: &str) -> Vec<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = trimmed.chars().collect();
    if chars.len() <= CHUNK_CHARS {
        return vec![trimmed.to_owned()];
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
        let passage: String = chars[start..end].iter().collect();
        let passage = passage.trim().to_owned();
        if !passage.is_empty() {
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
}
