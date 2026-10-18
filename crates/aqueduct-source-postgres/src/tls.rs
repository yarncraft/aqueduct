//! Connection security for the Postgres source: `sslmode` handling on top of
//! rustls (tokio-postgres itself only understands disable/prefer/require and
//! cannot verify certificates without a connector).

use std::str::FromStr;

use aqueduct_core::{Error, Result};
use aqueduct_tls::Verify;
use tokio_postgres::config::Host;
use tokio_postgres::Config;

/// libpq-compatible `sslmode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    Disable,
    /// Use TLS if the server offers it, else plaintext (libpq default). No verification.
    #[default]
    Prefer,
    /// TLS required, certificate not verified (verified against `ssl_root_cert` if one is set).
    Require,
    /// TLS required, certificate chain verified, hostname not checked.
    VerifyCa,
    /// TLS required, certificate chain and hostname verified.
    VerifyFull,
}

impl FromStr for SslMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        Ok(match s {
            "disable" => Self::Disable,
            "prefer" => Self::Prefer,
            "require" => Self::Require,
            "verify-ca" => Self::VerifyCa,
            "verify-full" => Self::VerifyFull,
            other => {
                return Err(format!(
                    "invalid sslmode `{other}` (expected disable, prefer, require, verify-ca or verify-full)"
                ))
            }
        })
    }
}

impl SslMode {
    pub(crate) fn verify(self, has_root_cert: bool) -> Verify {
        match self {
            Self::VerifyFull => Verify::Full,
            Self::VerifyCa => Verify::Chain,
            // libpq: a root cert with `require` implies verification.
            Self::Require if has_root_cert => Verify::Chain,
            _ => Verify::None,
        }
    }

    /// What we ask tokio-postgres to do at the protocol level.
    pub(crate) fn pg(self) -> tokio_postgres::config::SslMode {
        match self {
            Self::Disable => tokio_postgres::config::SslMode::Disable,
            Self::Prefer => tokio_postgres::config::SslMode::Prefer,
            _ => tokio_postgres::config::SslMode::Require,
        }
    }
}

/// Connection string with the libpq-only TLS parameters (`sslmode`,
/// `sslrootcert`) split out, since tokio-postgres rejects `verify-*`.
pub(crate) struct Parsed {
    pub config: Config,
    pub url_mode: Option<SslMode>,
    pub url_root_cert: Option<String>,
}

pub(crate) fn parse(url: &str) -> Result<Parsed> {
    let (mut mode, mut root) = (None, None);
    let mut take = |k: &str, v: &str| -> Result<bool> {
        match k {
            "sslmode" => mode = Some(v.parse::<SslMode>().map_err(Error::Config)?),
            "sslrootcert" => root = Some(v.to_string()),
            _ => return Ok(false),
        }
        Ok(true)
    };
    let rewritten = if let Some((base, query)) = url.split_once('?') {
        let mut kept = Vec::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if !take(k, v)? {
                kept.push(pair);
            }
        }
        if kept.is_empty() {
            base.to_string()
        } else {
            format!("{base}?{}", kept.join("&"))
        }
    } else if !url.contains("://") {
        // key=value DSN form
        let mut kept = Vec::new();
        for pair in url.split_whitespace() {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if !take(k, v)? {
                kept.push(pair);
            }
        }
        kept.join(" ")
    } else {
        url.to_string()
    };
    let config =
        Config::from_str(&rewritten).map_err(|e| Error::Config(format!("postgres url: {e}")))?;
    Ok(Parsed {
        config,
        url_mode: mode,
        url_root_cert: root,
    })
}

/// True when every host is loopback or a unix socket (plaintext is acceptable there).
pub(crate) fn is_local(config: &Config) -> bool {
    config.get_hosts().iter().all(|h| match h {
        Host::Tcp(h) => {
            h == "localhost"
                || h.parse::<std::net::IpAddr>()
                    .map(|ip| ip.is_loopback())
                    .unwrap_or(false)
        }
        #[cfg(unix)]
        Host::Unix(_) => true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_libpq_only_params_from_url() {
        let p = parse(
            "postgres://u:p@db:5432/app?sslmode=verify-full&sslrootcert=/ca.pem&application_name=x",
        )
        .unwrap();
        assert_eq!(p.url_mode, Some(SslMode::VerifyFull));
        assert_eq!(p.url_root_cert.as_deref(), Some("/ca.pem"));
        assert_eq!(p.config.get_application_name(), Some("x"));
    }

    #[test]
    fn parses_dsn_form_and_defaults() {
        let p = parse("host=db user=u sslmode=require").unwrap();
        assert_eq!(p.url_mode, Some(SslMode::Require));
        assert_eq!(parse("postgres://u@db/app").unwrap().url_mode, None);
    }

    #[test]
    fn rejects_unknown_sslmode() {
        assert!(parse("postgres://u@db/app?sslmode=bogus").is_err());
    }

    #[test]
    fn loopback_detection() {
        assert!(is_local(
            &parse("postgres://u@localhost/app").unwrap().config
        ));
        assert!(is_local(
            &parse("postgres://u@127.0.0.1/app").unwrap().config
        ));
        assert!(!is_local(
            &parse("postgres://u@db.example.com/app").unwrap().config
        ));
        assert!(!is_local(
            &parse("postgres://u@10.0.0.5/app").unwrap().config
        ));
    }

    #[test]
    fn verify_mapping() {
        assert_eq!(SslMode::Require.verify(false), Verify::None);
        assert_eq!(SslMode::Require.verify(true), Verify::Chain);
        assert_eq!(SslMode::VerifyFull.verify(false), Verify::Full);
    }
}
