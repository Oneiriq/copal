//! The words a reader sees in an HTML page.
//!
//! Search and embeddings want what a page says, not how it is built.
//! Indexing raw markup ranks pages by their CSS class names and inline
//! script, and a page carrying a chart's data inline packs so many
//! tokens into a passage that an embedding model refuses it outright.
//! This keeps text and drops everything else: tags, comments, and the
//! contents of `script`, `style` and `template`. Block elements become
//! paragraph breaks, so the passage splitter still finds the page's own
//! structure.
//!
//! It is a reader, not a parser: it never builds a tree and never fails.
//! Malformed markup yields whatever text it can find.

/// Elements whose contents are never shown as text.
const HIDDEN: &[&str] = &["script", "style", "template"];

/// Elements that start a new block, rendered as a paragraph break.
const BLOCKS: &[&str] = &[
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "caption",
    "dd",
    "details",
    "div",
    "dl",
    "dt",
    "figcaption",
    "figure",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "summary",
    "table",
    "td",
    "th",
    "title",
    "tr",
    "ul",
];

/// Extract the visible text of an HTML document.
pub fn visible_text(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let bytes = html.as_bytes();
    let mut raw = String::with_capacity(html.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'<' {
            if lower[i..].starts_with("<!--") {
                i = lower[i..]
                    .find("-->")
                    .map_or(bytes.len(), |end| i + end + 3);
                continue;
            }
            let (name, closing, end) = read_tag(&lower, i);
            if !closing && HIDDEN.contains(&name.as_str()) {
                let close = format!("</{name}");
                i = match lower[end..].find(&close) {
                    Some(at) => read_tag(&lower, end + at).2,
                    None => bytes.len(),
                };
                continue;
            }
            if BLOCKS.contains(&name.as_str()) {
                raw.push_str("\n\n");
            }
            i = end;
        } else if bytes[i] == b'&' {
            let (text, next) = read_entity(html, i);
            raw.push_str(&text);
            i = next;
        } else {
            // Copy up to the next markup character in one step, which
            // also keeps multi-byte characters whole.
            let next = html[i..].find(['<', '&']).map_or(bytes.len(), |at| i + at);
            raw.push_str(&html[i..next]);
            i = next;
        }
    }
    tidy(&raw)
}

/// Read the tag starting at `start` (a `<`): its lowercase name,
/// whether it closes an element, and the index just past its `>`.
/// Quoted attribute values may hold `>`, so quotes are honored.
fn read_tag(lower: &str, start: usize) -> (String, bool, usize) {
    let bytes = lower.as_bytes();
    let mut i = start + 1;
    let closing = bytes.get(i) == Some(&b'/');
    if closing {
        i += 1;
    }
    let name_start = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'-') {
        i += 1;
    }
    let name = lower[name_start..i].to_owned();
    let mut quote = None;
    while i < bytes.len() {
        match (quote, bytes[i]) {
            (None, b'"' | b'\'') => quote = Some(bytes[i]),
            (Some(q), b) if b == q => quote = None,
            (None, b'>') => return (name, closing, i + 1),
            _ => {}
        }
        i += 1;
    }
    (name, closing, bytes.len())
}

/// Decode the character reference at `start` (a `&`), returning its
/// text and the index after it. An unknown or malformed reference is
/// kept as written.
fn read_entity(html: &str, start: usize) -> (String, usize) {
    let rest = &html[start + 1..];
    let Some(semi) = rest.find(';').filter(|&at| at <= 10) else {
        return ("&".to_owned(), start + 1);
    };
    let name = &rest[..semi];
    let decoded = if let Some(code) = name.strip_prefix("#x").or_else(|| name.strip_prefix("#X")) {
        u32::from_str_radix(code, 16).ok().and_then(char::from_u32)
    } else if let Some(code) = name.strip_prefix('#') {
        code.parse().ok().and_then(char::from_u32)
    } else {
        named(name)
    };
    match decoded {
        Some(c) => (c.to_string(), start + semi + 2),
        None => ("&".to_owned(), start + 1),
    }
}

/// The named references agent-written pages actually use.
fn named(name: &str) -> Option<char> {
    Some(match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => ' ',
        "middot" => '·',
        "hellip" => '…',
        "mdash" => '—',
        "ndash" => '–',
        "lsquo" => '‘',
        "rsquo" => '’',
        "ldquo" => '“',
        "rdquo" => '”',
        "copy" => '©',
        "times" => '×',
        "rarr" => '→',
        "larr" => '←',
        "eacute" => 'é',
        "oslash" => 'ø',
        _ => return None,
    })
}

/// Collapse runs of spaces within lines and runs of blank lines between
/// them, so paragraphs end up separated by exactly one blank line.
fn tidy(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut blank_run = 0;
    for line in raw.lines() {
        let words: Vec<&str> = line.split_whitespace().collect();
        if words.is_empty() {
            blank_run += 1;
            continue;
        }
        if !out.is_empty() {
            out.push_str(if blank_run > 0 { "\n\n" } else { "\n" });
        }
        out.push_str(&words.join(" "));
        blank_run = 0;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_the_words_and_drops_the_machinery() {
        let page = r#"<!doctype html><html><head><title>The Ceiling</title>
            <style>.lead { color: red }</style></head><body>
            <!-- a note to self -->
            <h1>The ceiling pays</h1><p class="lead">People are <em>hurting</em> today.</p>
            <script>var DATA = {"labor":[[1947,65.2],[1948,64.7]]};</script>
            <p>Second&nbsp;paragraph &amp; more.</p></body></html>"#;
        assert_eq!(
            visible_text(page),
            "The Ceiling\n\nThe ceiling pays\n\nPeople are hurting today.\n\nSecond paragraph & more."
        );
    }

    #[test]
    fn quoted_attributes_and_entities_do_not_confuse_it() {
        let page = r#"<a title="a > b" href="x">G&oslash;sta &#8212; &#x2014; &bogus; AT&T</a>"#;
        assert_eq!(visible_text(page), "Gøsta — — &bogus; AT&T");
    }

    #[test]
    fn malformed_markup_still_yields_text() {
        assert_eq!(visible_text("before <script>never closed"), "before");
        assert_eq!(visible_text("a <b unterminated"), "a");
        assert_eq!(visible_text("tail <!-- open comment"), "tail");
        assert_eq!(visible_text("Ünïcödé <i>text</i> ✓"), "Ünïcödé text ✓");
    }
}
