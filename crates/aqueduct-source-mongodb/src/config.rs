use serde::Deserialize;

/// When a snapshot of existing documents runs (ADR 0001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// No snapshots, and nothing is written to the database.
    #[default]
    Never,
    /// Snapshots run only when requested (admin API).
    OnDemand,
    /// Snapshot every watched collection on a start without a checkpoint, and on request.
    Initial,
}

/// Default for [`MongoConfig::snapshot_chunk_size`].
pub const DEFAULT_CHUNK_SIZE: u32 = 1024;

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
    /// `never | on_demand | initial`: whether existing documents are copied. Default never.
    #[serde(default)]
    pub snapshot: SnapshotMode,
    /// Documents per snapshot chunk. Default 1024.
    #[serde(default = "default_chunk_size")]
    pub snapshot_chunk_size: u32,
    /// Upper bound on snapshot documents read per second. Default (and `0`) unthrottled.
    #[serde(default)]
    pub snapshot_max_rows_per_sec: Option<u32>,
}

fn default_true() -> bool {
    true
}

fn default_chunk_size() -> u32 {
    DEFAULT_CHUNK_SIZE
}

impl MongoConfig {
    /// Settings with every optional key at its default.
    pub fn new(uri: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            database: database.into(),
            collections: Vec::new(),
            pre_images: true,
            snapshot: SnapshotMode::Never,
            snapshot_chunk_size: DEFAULT_CHUNK_SIZE,
            snapshot_max_rows_per_sec: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.uri.starts_with("mongodb://") && !self.uri.starts_with("mongodb+srv://") {
            return Err("source.uri must start with mongodb:// or mongodb+srv://".into());
        }
        if self.database.is_empty() {
            return Err("source.database must not be empty".into());
        }
        if self.snapshot_chunk_size == 0 {
            return Err("source.snapshot_chunk_size must be at least 1".into());
        }
        if self
            .collections
            .iter()
            .any(|c| c == crate::snapshot::WATERMARK_COLLECTION)
        {
            return Err(format!(
                "source.collections must not name {:?}: it holds Aqueduct's snapshot watermarks",
                crate::snapshot::WATERMARK_COLLECTION
            ));
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
        assert_eq!(c.snapshot, SnapshotMode::Never);
        assert_eq!(c.snapshot_chunk_size, 1024);
        assert_eq!(c.snapshot_max_rows_per_sec, None);
        assert_eq!(c, MongoConfig::new("mongodb://h/?replicaSet=rs0", "app"));
        c.validate().unwrap();
    }

    #[test]
    fn parses_snapshot_settings() {
        let c: MongoConfig = toml_like(
            r#"{"uri":"mongodb://h/","database":"app","snapshot":"on_demand",
            "snapshot_chunk_size":10,"snapshot_max_rows_per_sec":500}"#,
        );
        assert_eq!(c.snapshot, SnapshotMode::OnDemand);
        assert_eq!(
            (c.snapshot_chunk_size, c.snapshot_max_rows_per_sec),
            (10, Some(500))
        );
        let c: MongoConfig =
            toml_like(r#"{"uri":"mongodb://h/","database":"app","snapshot":"initial"}"#);
        assert_eq!(c.snapshot, SnapshotMode::Initial);
        assert!(serde_json::from_str::<MongoConfig>(
            r#"{"uri":"mongodb://h/","database":"app","snapshot":"always"}"#
        )
        .unwrap_err()
        .to_string()
        .contains("on_demand"));
        let mut c = c;
        c.snapshot_chunk_size = 0;
        assert!(c.validate().unwrap_err().contains("snapshot_chunk_size"));
    }

    #[test]
    fn rejects_bad_uri() {
        let c: MongoConfig = toml_like(r#"{"uri":"postgres://x","database":"app"}"#);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_watching_the_watermark_collection() {
        let mut c = MongoConfig::new("mongodb://h/", "app");
        c.collections = vec!["aqueduct_watermarks".into()];
        assert!(c.validate().unwrap_err().contains("watermarks"));
    }

    fn toml_like(s: &str) -> MongoConfig {
        serde_json::from_str(s).unwrap()
    }
}
