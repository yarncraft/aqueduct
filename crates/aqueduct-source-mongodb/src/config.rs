use mongodb::options::{ConnectionString, HostInfo, ServerAddress, Tls};
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
#[derive(Clone, Deserialize, PartialEq)]
pub struct MongoConfig {
    /// Connection string; carries auth and `tls=true` options. Requires a replica set. Plaintext
    /// to a non-loopback host is refused unless the URI says `tls=false` explicitly.
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

/// The URI's password is redacted: `Debug` output can end up in logs.
impl std::fmt::Debug for MongoConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoConfig")
            .field("uri", &aqueduct_core::redact_password(&self.uri))
            .field("database", &self.database)
            .field("collections", &self.collections)
            .field("pre_images", &self.pre_images)
            .field("snapshot", &self.snapshot)
            .field("snapshot_chunk_size", &self.snapshot_chunk_size)
            .field("snapshot_max_rows_per_sec", &self.snapshot_max_rows_per_sec)
            .finish()
    }
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
        if let Some(cs) = self.connection_string() {
            if transport(&cs) == (Transport::Plaintext { explicit: false }) && !is_local(&cs) {
                return Err(
                    "source.uri connects to a non-loopback MongoDB host without TLS, so \
                    documents and credentials would cross the network in cleartext; add tls=true \
                    to the URI, or tls=false to accept plaintext explicitly"
                        .into(),
                );
            }
        }
        Ok(())
    }

    /// What to log at startup when the URI allows a connection that is not fully secured:
    /// explicit plaintext to a non-loopback host, or TLS without certificate checks.
    pub fn transport_warning(&self) -> Option<&'static str> {
        let cs = self.connection_string()?;
        match transport(&cs) {
            Transport::Plaintext { .. } if !is_local(&cs) => Some(
                "mongodb connection is PLAINTEXT to a non-loopback host (tls=false): documents \
                 and credentials cross the network unencrypted; use tls=true",
            ),
            Transport::Unverified => Some(
                "mongodb connection is encrypted but the server certificate is NOT verified \
                 (tlsInsecure or tlsAllowInvalidCertificates); remove it, and set tlsCAFile if \
                 the CA is private",
            ),
            _ => None,
        }
    }

    /// The URI as the driver reads it. A URI it cannot parse is left for the connect to report:
    /// the driver fails there with the same parser, before any connection is made.
    fn connection_string(&self) -> Option<ConnectionString> {
        ConnectionString::parse(&self.uri).ok()
    }
}

/// How a connection string secures its connection.
#[derive(Debug, PartialEq)]
enum Transport {
    /// TLS with the server certificate verified.
    Verified,
    /// TLS without checking the server certificate.
    Unverified,
    /// No TLS. `explicit` when the URI says `tls=false` (or `ssl=false`).
    Plaintext { explicit: bool },
}

fn transport(cs: &ConnectionString) -> Transport {
    match &cs.tls {
        // `tlsInsecure=true` sets this too.
        Some(Tls::Enabled(o)) if o.allow_invalid_certificates == Some(true) => {
            Transport::Unverified
        }
        Some(Tls::Enabled(_)) => Transport::Verified,
        Some(Tls::Disabled) => Transport::Plaintext { explicit: true },
        // `mongodb+srv://` turns TLS on unless the URI turns it off.
        None if matches!(cs.host_info, HostInfo::DnsRecord(_)) => Transport::Verified,
        None => Transport::Plaintext { explicit: false },
    }
}

/// True when every host is loopback or a unix socket, where plaintext is acceptable. An SRV
/// record names remote hosts.
fn is_local(cs: &ConnectionString) -> bool {
    let HostInfo::HostIdentifiers(hosts) = &cs.host_info else {
        return false;
    };
    hosts.iter().all(|h| match h {
        ServerAddress::Tcp { host, .. } => {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        }
        #[cfg(unix)]
        ServerAddress::Unix { .. } => true,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_with_defaults() {
        let c: MongoConfig =
            toml_like(r#"{"uri":"mongodb://h/?replicaSet=rs0&tls=true","database":"app"}"#);
        assert!(c.collections.is_empty() && c.pre_images);
        assert_eq!(c.snapshot, SnapshotMode::Never);
        assert_eq!(c.snapshot_chunk_size, 1024);
        assert_eq!(c.snapshot_max_rows_per_sec, None);
        assert_eq!(
            c,
            MongoConfig::new("mongodb://h/?replicaSet=rs0&tls=true", "app")
        );
        c.validate().unwrap();
    }

    #[test]
    fn parses_snapshot_settings() {
        let c: MongoConfig = toml_like(
            r#"{"uri":"mongodb://h/?tls=true","database":"app","snapshot":"on_demand",
            "snapshot_chunk_size":10,"snapshot_max_rows_per_sec":500}"#,
        );
        assert_eq!(c.snapshot, SnapshotMode::OnDemand);
        assert_eq!(
            (c.snapshot_chunk_size, c.snapshot_max_rows_per_sec),
            (10, Some(500))
        );
        let c: MongoConfig =
            toml_like(r#"{"uri":"mongodb://h/?tls=true","database":"app","snapshot":"initial"}"#);
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
        let mut c = MongoConfig::new("mongodb://h/?tls=true", "app");
        c.collections = vec!["aqueduct_watermarks".into()];
        assert!(c.validate().unwrap_err().contains("watermarks"));
    }

    #[test]
    fn plaintext_to_a_remote_host_is_refused_unless_explicit() {
        for uri in [
            "mongodb://h/?replicaSet=rs0",
            "mongodb://user:secret@db.example.com:27017/",
            "mongodb://10.0.0.5/",
            // One remote host in the seed list is enough.
            "mongodb://localhost,db.example.com/",
        ] {
            let err = MongoConfig::new(uri, "app").validate().unwrap_err();
            assert!(err.contains("without TLS"), "{uri}: {err}");
            assert!(!err.contains("secret"), "{err}");
        }
        for uri in ["mongodb://h/?tls=false", "mongodb://h/?ssl=false"] {
            let c = MongoConfig::new(uri, "app");
            c.validate().unwrap();
            assert!(
                c.transport_warning().unwrap().contains("PLAINTEXT"),
                "{uri}"
            );
        }
    }

    #[test]
    fn plaintext_to_loopback_and_tls_pass_quietly() {
        for uri in [
            "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true",
            "mongodb://LOCALHOST/",
            "mongodb://127.0.0.1,127.0.0.2:27018/",
            "mongodb://[::1]:27017/",
            "mongodb://localhost/?tls=false",
            "mongodb://h/?tls=true",
            "mongodb://h/?ssl=true",
            "mongodb://h/?tlsCAFile=/etc/ca.pem",
            "mongodb+srv://cluster.example.com/",
        ] {
            let c = MongoConfig::new(uri, "app");
            c.validate().unwrap_or_else(|e| panic!("{uri}: {e}"));
            assert_eq!(c.transport_warning(), None, "{uri}");
        }
        #[cfg(unix)]
        {
            let c = MongoConfig::new("mongodb://%2Ftmp%2Fmongodb-27017.sock/", "app");
            c.validate().unwrap();
            assert_eq!(c.transport_warning(), None);
        }
    }

    #[test]
    fn plaintext_over_srv_is_remote() {
        let c = MongoConfig::new("mongodb+srv://cluster.example.com/?tls=false", "app");
        c.validate().unwrap();
        assert!(c.transport_warning().unwrap().contains("PLAINTEXT"));
    }

    #[test]
    fn unverified_certificates_warn() {
        for uri in [
            "mongodb://h/?tls=true&tlsAllowInvalidCertificates=true",
            "mongodb://h/?tlsInsecure=true",
            "mongodb+srv://cluster.example.com/?tlsAllowInvalidCertificates=true",
            "mongodb://localhost/?tlsInsecure=true",
        ] {
            let c = MongoConfig::new(uri, "app");
            c.validate().unwrap();
            assert!(
                c.transport_warning().unwrap().contains("NOT verified"),
                "{uri}"
            );
        }
    }

    #[test]
    fn an_unparseable_uri_is_left_to_the_driver() {
        // The connect reports it with the driver's message; validate adds nothing.
        let c = MongoConfig::new("mongodb://u:p@h/?tls=maybe", "app");
        c.validate().unwrap();
        assert_eq!(c.transport_warning(), None);
    }

    fn toml_like(s: &str) -> MongoConfig {
        serde_json::from_str(s).unwrap()
    }
}
