use serde::Deserialize;

/// `[source]` settings for `type = "mongodb"`.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct MongoConfig {
    /// Connection string; carries auth and `tls=true` options. Requires a replica set.
    pub uri: String,
    pub database: String,
    /// Collections to watch. Empty means the whole database.
    #[serde(default)]
    pub collections: Vec<String>,
    /// Ask for pre-images (`fullDocumentBeforeChange`) when the collection has them enabled.
    #[serde(default = "default_true")]
    pub pre_images: bool,
}

fn default_true() -> bool {
    true
}

impl MongoConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.uri.starts_with("mongodb://") && !self.uri.starts_with("mongodb+srv://") {
            return Err("source.uri must start with mongodb:// or mongodb+srv://".into());
        }
        if self.database.is_empty() {
            return Err("source.database must not be empty".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_with_defaults() {
        let c: MongoConfig = toml_like(r#"{"uri":"mongodb://h/?replicaSet=rs0","database":"app"}"#);
        assert!(c.collections.is_empty() && c.pre_images);
        c.validate().unwrap();
    }

    #[test]
    fn rejects_bad_uri() {
        let c: MongoConfig = toml_like(r#"{"uri":"postgres://x","database":"app"}"#);
        assert!(c.validate().is_err());
    }

    fn toml_like(s: &str) -> MongoConfig {
        serde_json::from_str(s).unwrap()
    }
}
