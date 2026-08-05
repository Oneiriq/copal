//! The passage a reader sees, and where in it the query matched.
//!
//! Retrieval and ranking run over stemmed terms: the index resolves
//! `running` and `runs` to one term, and [`bm25`](crate::bm25) rescores
//! against the same analysis. An excerpt chosen by searching the source
//! for the words a caller typed answers a different question, and gets
//! it wrong in the case stemming exists for. Ask for `running`, match a
//! passage that says `runs`, and a literal search finds nothing, so the
//! reader is handed the opening of the passage and no reason it was
//! returned.
//!
//! So the window is chosen with the analyzer that decided the match,
//! and the matched spans come back with it. A caller that wants to mark
//! them can; one that wants the text alone ignores them.

use std::collections::HashSet;

use crate::bm25::{analyze, analyze_spans};

/// A window of a passage, with the query's matches located inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Excerpt {
    /// The window itself.
    pub text: String,
    /// Half-open `[start, end)` ranges within `text` that matched,
    /// in order, counted in characters (Unicode scalar values) rather
    /// than bytes, because the text travels as a JSON string and its
    /// consumers index strings that way.
    pub matches: Vec<(usize, usize)>,
}

/// Cut `width` characters of `body` around the query's best match.
///
/// Best means the window covering the most matched tokens. A passage
/// mentioning a term once at the top and four times together lower down
/// is about the latter, and that is the part worth showing.
pub fn excerpt(body: &str, query: &str, width: usize) -> Excerpt {
    let terms: HashSet<String> = analyze(query).into_iter().collect();
    let hits: Vec<(usize, usize)> = analyze_spans(body)
        .into_iter()
        .filter(|token| terms.contains(&token.term))
        .map(|token| (token.start, token.end))
        .collect();

    let characters: Vec<char> = body.chars().collect();
    if characters.len() <= width {
        return Excerpt {
            text: body.to_owned(),
            matches: hits,
        };
    }
    // Nothing matched: the engine found this passage through a term the
    // analyzer here does not reproduce, or the caller asked with terms
    // that all stem away. The opening is as good an answer as any.
    if hits.is_empty() {
        return Excerpt {
            text: characters[..width].iter().collect(),
            matches: Vec::new(),
        };
    }

    // Each hit anchors a candidate window, offset back a third so the
    // match reads in context rather than flush against the left edge.
    let last_start = characters.len() - width;
    let mut best_start = 0;
    let mut best_count = 0;
    for (start, _) in &hits {
        let candidate = start.saturating_sub(width / 3).min(last_start);
        let count = hits
            .iter()
            .filter(|(s, e)| *s >= candidate && *e <= candidate + width)
            .count();
        if count > best_count {
            best_count = count;
            best_start = candidate;
        }
    }

    let end = best_start + width;
    Excerpt {
        text: characters[best_start..end].iter().collect(),
        // Only whole matches, so a caller marking a span never marks
        // half a word cut off by the window's edge.
        matches: hits
            .iter()
            .filter(|(s, e)| *s >= best_start && *e <= end)
            .map(|(s, e)| (s - best_start, e - best_start))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case the literal search got wrong: the query and the passage
    /// share a stem and share no characters at the end.
    #[test]
    fn a_stemmed_match_is_found_and_located() {
        let body = format!(
            "{}The service runs every night without supervision.{}",
            "padding text that says nothing at all. ".repeat(12),
            " More padding follows here.".repeat(12),
        );
        let found = excerpt(&body, "running", 200);
        assert!(
            found.text.contains("runs every night"),
            "a literal search for `running` finds nothing here: {}",
            found.text,
        );
        assert_eq!(found.matches.len(), 1);
        let (start, end) = found.matches[0];
        let marked: String = found.text.chars().skip(start).take(end - start).collect();
        assert_eq!(marked, "runs", "the span points at the word, whole");
    }

    /// A term mentioned once early and clustered later: the cluster is
    /// what the passage is about.
    #[test]
    fn the_window_lands_where_the_matches_cluster() {
        let body = format!(
            "A ledger is mentioned here once.{}Then ledger, ledger, and ledger again.",
            " filler that carries no terms at all.".repeat(20),
        );
        let found = excerpt(&body, "ledger", 120);
        assert!(
            found.text.contains("ledger, ledger, and ledger again"),
            "the cluster is what the passage is about: {}",
            found.text,
        );
        assert_eq!(
            found.matches.len(),
            3,
            "the three clustered mentions, and not the lone early one",
        );
    }

    #[test]
    fn a_short_passage_comes_back_whole_and_marked() {
        let found = excerpt("the quick brown fox", "fox", 400);
        assert_eq!(found.text, "the quick brown fox");
        assert_eq!(found.matches, vec![(16, 19)]);
    }

    /// A window that would run past the end clamps rather than panics,
    /// and a match at the very end survives the clamp.
    #[test]
    fn a_match_at_the_end_still_lands_inside_the_window() {
        let body = format!("{}terminus", "abcdefghij ".repeat(40));
        let found = excerpt(&body, "terminus", 100);
        assert!(found.text.ends_with("terminus"), "{}", found.text);
        assert_eq!(found.matches.len(), 1);
        let (start, end) = found.matches[0];
        let marked: String = found.text.chars().skip(start).take(end - start).collect();
        assert_eq!(marked, "terminus");
    }

    #[test]
    fn no_match_gives_the_opening_and_claims_nothing() {
        let body = "a".repeat(500);
        let found = excerpt(&body, "absent", 100);
        assert_eq!(found.text.chars().count(), 100);
        assert!(found.matches.is_empty());
    }

    /// Offsets count characters, so a passage carrying multi-byte text
    /// reports spans a caller can slice with.
    #[test]
    fn offsets_count_characters_rather_than_bytes() {
        let found = excerpt("café naïve fox", "fox", 400);
        let (start, end) = found.matches[0];
        let marked: String = found.text.chars().skip(start).take(end - start).collect();
        assert_eq!(marked, "fox");
    }
}
