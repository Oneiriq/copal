//! Short links: a name a person can type, for a render grant.
//!
//! A render link carries a 64-character secret, which is right for a
//! capability and useless for someone typing an address into a phone.
//! A short link is `<name>-<code>`: a name the publisher chose, then a
//! random code, so that knowing the name is not enough to open the
//! page. The link points at one render grant and can be pointed at
//! another, so one address follows a page across versions; revoking the
//! grant it points at closes it.
//!
//! The code is four characters from an alphabet without look-alikes
//! (no `0`/`o`, `1`/`l`/`i`), so it survives being read aloud or typed
//! from a screenshot. Links are matched case-insensitively, because
//! phone keyboards capitalize the first letter.

use copal_core::CopalError;
use rand::RngExt as _;

/// Characters a code is drawn from: lowercase letters and digits minus
/// those easily confused with one another.
pub const CODE_ALPHABET: &[u8] = b"23456789abcdefghjkmnpqrstuvwxyz";
/// Characters in a code.
pub const CODE_LEN: usize = 4;
/// Longest name a publisher may choose.
pub const MAX_NAME_LEN: usize = 40;

/// Validate a publisher's chosen name and append a fresh random code.
///
/// Names are lowercase letters, digits and single hyphens, starting
/// and ending with a letter or digit; uppercase input is folded.
pub fn mint(name: &str) -> copal_core::Result<String> {
    let name = name.trim().to_ascii_lowercase();
    if !valid_name(&name) {
        return Err(CopalError::validation(format!(
            "short_name must be 1 to {MAX_NAME_LEN} lowercase letters, digits or single \
             hyphens, starting and ending with a letter or digit",
        )));
    }
    let mut rng = rand::rng();
    let code: String = (0..CODE_LEN)
        .map(|_| CODE_ALPHABET[rng.random_range(0..CODE_ALPHABET.len())] as char)
        .collect();
    Ok(format!("{name}-{code}"))
}

/// Fold a typed or pasted link to its stored form, or `None` when it
/// cannot be one. Callers answer `None` exactly as they answer an
/// unknown link, so the shape check is not an oracle.
pub fn normalize(raw: &str) -> Option<String> {
    let link = raw.trim().to_ascii_lowercase();
    let (name, code) = link.rsplit_once('-')?;
    let code_ok = code.len() == CODE_LEN && code.bytes().all(|b| CODE_ALPHABET.contains(&b));
    (code_ok && valid_name(name)).then_some(link)
}

fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_NAME_LEN
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes.first() != Some(&b'-')
        && bytes.last() != Some(&b'-')
        && !name.contains("--")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minted_links_carry_the_name_and_a_typeable_code() {
        let link = mint("Ceiling").unwrap();
        let (name, code) = link.rsplit_once('-').unwrap();
        assert_eq!(name, "ceiling");
        assert_eq!(code.len(), CODE_LEN);
        assert!(code.bytes().all(|b| CODE_ALPHABET.contains(&b)), "{code}");
        assert_eq!(normalize(&link).as_deref(), Some(link.as_str()));
    }

    #[test]
    fn names_may_hold_hyphens_but_not_at_the_edges() {
        assert!(mint("the-ceiling-v4").is_ok());
        for bad in [
            "",
            "-ceiling",
            "ceiling-",
            "two--hyphens",
            "spaces here",
            "émoji",
            &"x".repeat(41),
        ] {
            assert!(mint(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn typed_links_fold_case_and_refuse_the_wrong_shape() {
        assert_eq!(normalize(" Ceiling-K7Q2 ").as_deref(), Some("ceiling-k7q2"));
        assert_eq!(
            normalize("the-ceiling-v4-k7q2").as_deref(),
            Some("the-ceiling-v4-k7q2")
        );
        // Missing code, look-alike characters, wrong length, no name.
        for bad in [
            "ceiling",
            "ceiling-k7q0",
            "ceiling-k7ql",
            "ceiling-k7q",
            "-k7q2",
            "ceiling-k7q22",
        ] {
            assert_eq!(normalize(bad), None, "{bad:?} should be refused");
        }
    }
}
