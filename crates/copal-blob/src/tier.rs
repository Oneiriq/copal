//! The tier vocabulary: a second named backend inside a residency,
//! plus the class that decides its recall story.
//!
//! Residency answers where bytes may live; tier answers what they cost
//! right now. The residency is baked into the blob row id and every
//! link, so it is immutable per blob; the tier is one mutable column,
//! where `NONE` means the residency's primary backend. This module
//! carries only the configuration shapes and their validation --
//! nothing here moves a byte.

use std::collections::HashMap;

use copal_core::CopalError;

use crate::BackendConfig;

/// What a GET against the tier answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierClass {
    /// A GET answers in ordinary latency at a retrieval fee (S3
    /// Standard-IA, Glacier Instant Retrieval, GCS Nearline/Coldline,
    /// Azure Cool). No face changes; no recall machinery exists.
    Online,
    /// A GET cannot answer until a restore (Glacier Flexible and Deep
    /// Archive, Azure Archive). Refused at configuration until recall
    /// ships: no deployment may strand bytes behind a GET nothing
    /// answers.
    Archive,
}

impl TierClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Archive => "archive",
        }
    }
}

/// One tier: a backend and its declared class. A tier never carries
/// its own key -- hot and cold copies of one digest are the same
/// object and seal under the residency's key, so the move ships the
/// envelope opaque and the cold backend needs no key material at all.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TierConfig {
    pub class: TierClass,
    #[serde(flatten)]
    pub backend: BackendConfig,
}

/// A residency's full configuration: its primary backend plus any
/// tiers nested inside it. The bare backend form residencies have
/// always used still parses -- `tiers` is an addition, not a change.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ResidencyConfig {
    #[serde(flatten)]
    pub backend: BackendConfig,
    #[serde(default)]
    pub tiers: HashMap<String, TierConfig>,
}

/// Tier names ride the same alphabet residency names do: they land in
/// a blob-row column and in policy rows, and one naming rule is
/// easier to hold than two.
pub fn valid_tier_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// Validate one residency's tier block. Refusals, each with the rule
/// it enforces:
///
/// - an invalid tier name (the alphabet above);
/// - `class: "archive"` -- invalid until recall lands;
/// - a tier carrying its own encryption key -- keys are per residency
///   because a key boundary scopes dedupe exactly as a backend
///   boundary does, and a per-tier key would turn every move into a
///   re-seal.
///
/// The backend itself is proven openable by the caller (opening is a
/// backend concern, not a vocabulary one).
pub fn validate_tiers(
    residency: &str,
    tiers: &HashMap<String, TierConfig>,
) -> copal_core::Result<()> {
    for (name, tier) in tiers {
        if !valid_tier_name(name) {
            return Err(CopalError::validation(format!(
                "tier name {name} in residency {residency} is invalid: \
                 1..=32 lowercase alphanumeric",
            )));
        }
        if tier.class == TierClass::Archive {
            return Err(CopalError::validation(format!(
                "tier {residency}/{name} declares class \"archive\", which is not \
                 servable yet: a GET against it would have no recall to answer with",
            )));
        }
        if tier.backend.encryption_key().is_some()
            || tier.backend.previous_encryption_key().is_some()
        {
            return Err(CopalError::validation(format!(
                "tier {residency}/{name} carries its own encryption key; tiers seal \
                 under their residency's key so a move ships the envelope opaque",
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_backend_still_parses_with_no_tiers() {
        let raw = r#"{"scheme": "fs", "root": "/data/eu"}"#;
        let config: ResidencyConfig = serde_json::from_str(raw).unwrap();
        assert!(config.tiers.is_empty());
        assert!(matches!(config.backend, BackendConfig::Fs { .. }));
    }

    #[test]
    fn tiers_nest_inside_the_residency() {
        let raw = r#"{
            "scheme": "s3", "bucket": "acme-eu",
            "access_key_id": "ak", "secret_access_key": "sk",
            "tiers": { "cold": { "scheme": "s3", "bucket": "acme-eu-cold",
                                 "access_key_id": "ak", "secret_access_key": "sk",
                                 "class": "online" } }
        }"#;
        let config: ResidencyConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(config.tiers.len(), 1);
        let cold = &config.tiers["cold"];
        assert_eq!(cold.class, TierClass::Online);
        assert!(validate_tiers("eu", &config.tiers).is_ok());
    }

    #[test]
    fn archive_class_refuses_until_recall_ships() {
        let raw = r#"{
            "scheme": "fs", "root": "/data/eu",
            "tiers": { "deep": { "scheme": "fs", "root": "/data/eu-deep",
                                 "class": "archive" } }
        }"#;
        let config: ResidencyConfig = serde_json::from_str(raw).unwrap();
        let err = validate_tiers("eu", &config.tiers).unwrap_err();
        assert!(err.to_string().contains("archive"), "{err}");
    }

    #[test]
    fn a_tier_never_carries_its_own_key() {
        let key = "a".repeat(64);
        let raw = format!(
            r#"{{
                "scheme": "fs", "root": "/data/eu",
                "tiers": {{ "cold": {{ "scheme": "fs", "root": "/data/eu-cold",
                                       "class": "online",
                                       "encryption_key": "{key}" }} }}
            }}"#
        );
        let config: ResidencyConfig = serde_json::from_str(&raw).unwrap();
        let err = validate_tiers("eu", &config.tiers).unwrap_err();
        assert!(err.to_string().contains("encryption key"), "{err}");
    }

    #[test]
    fn tier_names_hold_the_residency_alphabet() {
        assert!(valid_tier_name("cold"));
        assert!(valid_tier_name("tier2"));
        assert!(!valid_tier_name(""));
        assert!(!valid_tier_name("Cold"));
        assert!(!valid_tier_name("eu-cold"));
        assert!(!valid_tier_name(&"c".repeat(33)));
    }
}
