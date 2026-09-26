//! Identifier newtypes.
//!
//! Ids are ULID strings: sortable by creation time, no coordination, and
//! safe inside a SurrealDB record id without bracket escaping. Newtypes
//! keep a `TenantId` from ever being passed where a `FileId` belongs.

use serde::{Deserialize, Serialize};

use crate::error::CopalError;

macro_rules! id_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wrap an existing id after validating it is non-empty and
            /// free of characters that would need SurrealDB record-id
            /// escaping.
            pub fn parse(value: impl Into<String>) -> crate::Result<Self> {
                let value = value.into();
                if value.is_empty() {
                    return Err(CopalError::validation(concat!(
                        stringify!($name),
                        " cannot be empty"
                    )));
                }
                if !value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                {
                    return Err(CopalError::validation(format!(
                        "{} contains characters outside [A-Za-z0-9_-]: {value:?}",
                        stringify!($name),
                    )));
                }
                Ok(Self(value))
            }

            /// View as `&str`.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_newtype! {
    /// Identifies one file record. Generated as a ULID.
    FileId
}

id_newtype! {
    /// Identifies a tenant. Caller-supplied slug, not generated.
    TenantId
}

impl FileId {
    /// Mint a new ULID-backed id.
    pub fn generate() -> Self {
        Self(ulid::Ulid::generate().to_string().to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_file_ids_are_valid_and_unique() {
        let a = FileId::generate();
        let b = FileId::generate();
        assert_ne!(a, b);
        assert!(FileId::parse(a.as_str()).is_ok());
    }

    #[test]
    fn parse_rejects_empty_and_hostile_input() {
        assert!(TenantId::parse("").is_err());
        assert!(TenantId::parse("acme corp").is_err()); // space
        assert!(TenantId::parse("acme;DROP").is_err());
        assert!(TenantId::parse("acme-prod_1").is_ok());
    }

    #[test]
    fn serde_is_transparent() {
        let t = TenantId::parse("acme").unwrap();
        assert_eq!(serde_json::to_string(&t).unwrap(), "\"acme\"");
    }
}
