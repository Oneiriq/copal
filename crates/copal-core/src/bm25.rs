//! Lexical relevance, computed here because the engine does not.
//!
//! SurrealDB 3.x accepts a `BM25` full-text index and uses it to decide
//! WHICH rows match, but it reports no per-row score (`search::score`
//! returns 0) and its scan yields matches in insertion order. Both are
//! pinned by tests in `copal-store`. So a lexical search that ranked on
//! the engine's output would be returning the oldest matches rather
//! than the best ones, and a hybrid search fusing that order would be
//! fusing noise.
//!
//! The engine still does the expensive part: finding the matching rows
//! through the index. This module rescores that candidate window, which
//! is the same two-stage shape production search engines use.

use std::collections::HashMap;

/// Term-frequency saturation. 1.2 is the standard default and matches
/// what the engine's own BM25 uses when the index names no parameters.
const K1: f64 = 1.2;

/// Length normalisation strength. 0.75 is the same standard default.
const B: f64 = 0.75;

/// Split text the way the index's analyzer does: class tokenizer,
/// lowercase, English Snowball stems.
///
/// Matching the analyzer matters. The index resolves `running` to the
/// same term as `run`, so a scorer that skipped stemming would score
/// zero on a document the engine correctly matched, and sort a real
/// hit to the bottom.
pub fn analyze(text: &str) -> Vec<String> {
    let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| stemmer.stem(&token.to_lowercase()).into_owned())
        .collect()
}

/// One scored candidate: its position in the input and its score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scored {
    pub index: usize,
    pub score: f64,
}

/// Rank `documents` against `query`, best first.
///
/// Returns every document with its score, including zero-scoring ones,
/// so a caller can decide whether to keep them. Ties hold their input
/// order, which keeps the result stable for equal-scoring passages.
///
/// Document frequency comes from the candidate window rather than the
/// whole corpus, because the corpus statistics are inside the engine
/// and it will not report them. Within a window where every document
/// already matched, this still separates a rare query term from a
/// common one, which is the discrimination that matters for ordering.
/// It does mean scores are comparable within one result set and
/// nowhere else, which is why they rank rows here and are not served
/// as an API field.
pub fn rank(query: &str, documents: &[String]) -> Vec<Scored> {
    let terms = analyze(query);
    if terms.is_empty() || documents.is_empty() {
        return documents
            .iter()
            .enumerate()
            .map(|(index, _)| Scored { index, score: 0.0 })
            .collect();
    }

    let analyzed: Vec<Vec<String>> = documents.iter().map(|body| analyze(body)).collect();
    let total = analyzed.len() as f64;
    let average_length = analyzed.iter().map(|d| d.len()).sum::<usize>() as f64 / total;

    // How many candidates contain each query term, counted once per
    // document however often the term repeats there.
    let mut containing: HashMap<&str, f64> = HashMap::new();
    for document in &analyzed {
        for term in terms.iter().collect::<std::collections::BTreeSet<_>>() {
            if document.iter().any(|t| t == term) {
                *containing.entry(term.as_str()).or_insert(0.0) += 1.0;
            }
        }
    }

    let mut scored: Vec<Scored> = analyzed
        .iter()
        .enumerate()
        .map(|(index, document)| {
            let length = document.len() as f64;
            let mut score = 0.0;
            for term in &terms {
                let frequency = document.iter().filter(|t| *t == term).count() as f64;
                if frequency == 0.0 {
                    continue;
                }
                let n = containing.get(term.as_str()).copied().unwrap_or(0.0);
                // The standard probabilistic IDF, in the form that
                // stays positive when a term is in every document.
                let idf = (1.0 + (total - n + 0.5) / (n + 0.5)).ln();
                let denominator = frequency + K1 * (1.0 - B + B * length / average_length.max(1.0));
                score += idf * (frequency * (K1 + 1.0)) / denominator;
            }
            Scored { index, score }
        })
        .collect();

    // Descending by score; equal scores keep their input order, which
    // `sort_by` guarantees because it is stable.
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stemming_matches_the_analyzer() {
        assert_eq!(analyze("Running RUNS ran"), vec!["run", "run", "ran"]);
        // Punctuation splits, empties drop.
        assert_eq!(analyze("a-b, c!"), vec!["a", "b", "c"]);
        assert!(analyze("   ").is_empty());
    }

    #[test]
    fn a_dense_short_document_outranks_a_long_diluted_one() {
        let documents = vec![
            "the quick brown fox jumps over the lazy dog on a long and rambling \
             afternoon full of unrelated words about nothing at all"
                .to_owned(),
            "quick quick quick fox".to_owned(),
        ];
        let ranked = rank("quick fox", &documents);
        // Length normalisation is the whole reason BM25 beats a term
        // count, so the short dense document must come first even
        // though it was second in the input.
        assert_eq!(ranked[0].index, 1, "{ranked:?}");
        assert!(ranked[0].score > ranked[1].score);
    }

    #[test]
    fn a_rare_term_outweighs_a_common_one() {
        let documents = vec![
            // Contains only the term every candidate has.
            "common common common".to_owned(),
            // Contains the term almost nothing has.
            "common zebra".to_owned(),
            "common filler".to_owned(),
            "common filler".to_owned(),
        ];
        let ranked = rank("common zebra", &documents);
        assert_eq!(ranked[0].index, 1, "the rare term wins: {ranked:?}");
    }

    #[test]
    fn a_document_without_any_query_term_scores_zero() {
        let documents = vec!["nothing relevant here".to_owned(), "quick fox".to_owned()];
        let ranked = rank("quick", &documents);
        assert_eq!(ranked[0].index, 1);
        assert_eq!(ranked[1].score, 0.0);
    }

    #[test]
    fn empty_inputs_rank_without_panicking() {
        assert!(rank("anything", &[]).is_empty());
        let documents = vec!["a".to_owned(), "b".to_owned()];
        // A query of pure punctuation analyzes to nothing.
        let ranked = rank("!!!", &documents);
        assert_eq!(ranked.len(), 2);
        assert!(ranked.iter().all(|s| s.score == 0.0));
    }

    #[test]
    fn ties_keep_their_input_order() {
        let documents = vec![
            "same terms here".to_owned(),
            "same terms here".to_owned(),
            "same terms here".to_owned(),
        ];
        let ranked = rank("same terms", &documents);
        assert_eq!(
            ranked.iter().map(|s| s.index).collect::<Vec<_>>(),
            vec![0, 1, 2],
        );
    }
}
