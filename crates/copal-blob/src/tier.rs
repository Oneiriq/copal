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
    /// Archive classes only: how many days a restored copy stays
    /// readable at the backend (S3 `RestoreObject` `Days`). Default 7.
    #[serde(default)]
    pub restore_days: Option<u32>,
    #[serde(flatten)]
    pub backend: BackendConfig,
}

/// How a recall makes an archive-cold object readable again. Derived
/// from the tier's backend, because the restore call is a backend
/// dialect, not a policy.
#[derive(Debug, Clone)]
pub enum RestoreSpec {
    /// No restore call exists or is needed: filesystem tiers are
    /// always readable, and GCS archive classes answer GETs
    /// directly. The recall's readability probe passes immediately.
    Instant,
    /// S3 `RestoreObject`, signed with the tier's own credentials.
    S3Restore {
        bucket: String,
        root: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        days: u32,
    },
    /// A rehydration this build does not drive. Archive classes on
    /// such backends refuse at configuration, so no deployment can
    /// strand bytes behind a restore nothing issues.
    Undriven(&'static str),
}

impl TierConfig {
    /// The restore dialect this tier's backend speaks.
    pub fn restore(&self) -> RestoreSpec {
        match &self.backend {
            BackendConfig::Fs { .. } | BackendConfig::Gcs { .. } => RestoreSpec::Instant,
            BackendConfig::S3 {
                bucket,
                root,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                ..
            } => RestoreSpec::S3Restore {
                bucket: bucket.clone(),
                root: root.clone(),
                endpoint: endpoint.clone(),
                region: region.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                days: self.restore_days.unwrap_or(7).max(1),
            },
            BackendConfig::Azblob { .. } => {
                RestoreSpec::Undriven("azure archive rehydration is not driven yet")
            }
        }
    }
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
/// - `class: "archive"` on a backend whose rehydration this build
///   does not drive -- a GET against it would have no recall to
///   answer with;
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
            if let RestoreSpec::Undriven(reason) = tier.restore() {
                return Err(CopalError::validation(format!(
                    "tier {residency}/{name} declares class \"archive\", but {reason}: \
                     a GET against it would have no recall to answer with",
                )));
            }
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
    fn archive_class_needs_a_driven_restore() {
        // Filesystem archive tiers are always readable: Instant
        // restore, valid.
        let raw = r#"{
            "scheme": "fs", "root": "/data/eu",
            "tiers": { "deep": { "scheme": "fs", "root": "/data/eu-deep",
                                 "class": "archive" } }
        }"#;
        let config: ResidencyConfig = serde_json::from_str(raw).unwrap();
        assert!(validate_tiers("eu", &config.tiers).is_ok());
        assert!(matches!(
            config.tiers["deep"].restore(),
            RestoreSpec::Instant
        ));

        // S3 archive tiers restore through RestoreObject, with the
        // declared days riding along.
        let raw = r#"{
            "scheme": "fs", "root": "/data/eu",
            "tiers": { "deep": { "scheme": "s3", "bucket": "eu-deep",
                                 "access_key_id": "ak", "secret_access_key": "sk",
                                 "class": "archive", "restore_days": 3 } }
        }"#;
        let config: ResidencyConfig = serde_json::from_str(raw).unwrap();
        assert!(validate_tiers("eu", &config.tiers).is_ok());
        assert!(matches!(
            config.tiers["deep"].restore(),
            RestoreSpec::S3Restore { days: 3, .. }
        ));

        // A rehydration this build does not drive refuses at
        // configuration rather than stranding bytes.
        let raw = r#"{
            "scheme": "fs", "root": "/data/eu",
            "tiers": { "deep": { "scheme": "azblob", "container": "eu-deep",
                                 "endpoint": "http://127.0.0.1:10000/dev",
                                 "account_name": "dev", "account_key": "a2V5",
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
