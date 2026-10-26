//! Connection security for the Postgres source: `sslmode` handling on top of
//! rustls (tokio-postgres itself only understands disable/prefer/require and
//! cannot verify certificates without a connector).

use std::str::FromStr;

use aqueduct_core::{Error, Result};
use aqueduct_tls::Verify;
use tokio_postgres::Config;
use tokio_postgres::config::Host;

/// libpq-compatible `sslmode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    Disable,
    /// Use TLS if the server offers it, else plaintext. No verification. Must be chosen explicitly.
    Prefer,
    /// TLS required, certificate not verified (verified against `ssl_root_cert` if one is set).
    Require,
    /// TLS required, certificate chain verified, hostname not checked.
    VerifyCa,
    /// TLS required, certificate chain and hostname verified. The default: weaker modes need explicit config.
    #[default]
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
                ));
            }
        })
    }
}

impl SslMode {
    /// The libpq name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }

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

/// True when every host is loopback or a unix socket (plaintext is acceptable there). A
/// `hostaddr` is the address actually dialled (`host` then only names the server for TLS), so
/// each one must be loopback too: `host=localhost hostaddr=203.0.113.9` is not local.
pub(crate) fn is_local(config: &Config) -> bool {
    config.get_hostaddrs().iter().all(|ip| ip.is_loopback())
        && config.get_hosts().iter().all(|h| match h {
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

/// Without a `connect_timeout` in the URL, a connection attempt gives up after this.
pub(crate) const DEFAULT_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// TCP keepalive defaults for the SQL connection, applied unless the URL sets its own
/// (`keepalives_idle`, `keepalives_interval`, `keepalives_retries`). A poll waits on its query
/// with nothing to send, so only keepalives notice a peer that vanished (a failover, a NAT that
/// dropped the flow): with these, about 60 s instead of the OS default of over 2 h, during which
/// the pipeline hangs, `/healthz` stays ok and the slot pins WAL.
pub(crate) fn apply_connection_defaults(config: &mut Config) {
    use std::time::Duration;
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(DEFAULT_CONNECT_TIMEOUT);
    }
    // tokio-postgres' own idle default is 2 h; a different value came from the URL.
    if config.get_keepalives_idle() == Duration::from_secs(2 * 60 * 60) {
        config.keepalives_idle(Duration::from_secs(30));
    }
    if config.get_keepalives_interval().is_none() {
        config.keepalives_interval(Duration::from_secs(10));
    }
    if config.get_keepalives_retries().is_none() {
        config.keepalives_retries(3);
    }
}

/// The mode to dial with. `prefer` to a non-loopback host falls back to plaintext when the server
/// refuses TLS, and [`check_negotiated`] would then refuse the session, but only after the
/// password was sent in the clear. Requiring TLS for the dial fails before any credential is
/// sent, with the same outcome.
pub(crate) fn dial_mode(mode: SslMode, local: bool) -> SslMode {
    if mode == SslMode::Prefer && !local {
        SslMode::Require
    } else {
        mode
    }
}

/// Policy on what the server actually negotiated. Fails closed: plaintext to a
/// non-loopback host is an error unless `ssl_mode = "disable"` was chosen explicitly.
pub(crate) fn check_negotiated(
    mode: SslMode,
    encrypted: bool,
    local: bool,
    has_root_cert: bool,
) -> Result<()> {
    match (encrypted, local) {
        (true, _) => {
            if matches!(mode, SslMode::Prefer | SslMode::Require) && !has_root_cert {
                tracing::warn!("postgres connection is encrypted but the server certificate is NOT verified; set sslmode=verify-full and a CA (postgres.ssl_root_cert)");
            }
        }
        (false, true) => tracing::info!("postgres connection is plaintext (local host)"),
        (false, false) if mode == SslMode::Disable => tracing::warn!(
            "postgres connection is PLAINTEXT to a non-loopback host (ssl_mode = \"disable\"): credentials and row data cross the network unencrypted"
        ),
        (false, false) => {
            return Err(Error::Config(
                "postgres connection is PLAINTEXT to a non-loopback host: refusing to run; use TLS (postgres.ssl_mode = \"verify-full\") or set ssl_mode = \"disable\" explicitly to accept plaintext".into(),
            ))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // #12: secure by default. Weaker modes must be an explicit choice.
    #[test]
    fn default_ssl_mode_is_verify_full() {
        assert_eq!(SslMode::default(), SslMode::VerifyFull);
    }

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
        // The dialled address counts, not the name.
        assert!(!is_local(
            &parse("host=localhost hostaddr=203.0.113.9 user=u dbname=app")
                .unwrap()
                .config
        ));
        assert!(is_local(
            &parse("host=localhost hostaddr=127.0.0.1 user=u dbname=app")
                .unwrap()
                .config
        ));
    }

    #[test]
    fn connections_get_keepalives_and_a_connect_timeout_unless_the_url_sets_them() {
        use std::time::Duration;
        let mut c = parse("postgres://u@db/app").unwrap().config;
        apply_connection_defaults(&mut c);
        assert!(c.get_keepalives());
        assert_eq!(c.get_keepalives_idle(), Duration::from_secs(30));
        assert_eq!(c.get_keepalives_interval(), Some(Duration::from_secs(10)));
        assert_eq!(c.get_keepalives_retries(), Some(3));
        assert_eq!(c.get_connect_timeout(), Some(&DEFAULT_CONNECT_TIMEOUT));
        let mut c = parse(
            "postgres://u@db/app?connect_timeout=5&keepalives_idle=600&keepalives_interval=60&keepalives_retries=9",
        )
        .unwrap()
        .config;
        apply_connection_defaults(&mut c);
        assert_eq!(c.get_keepalives_idle(), Duration::from_secs(600));
        assert_eq!(c.get_keepalives_interval(), Some(Duration::from_secs(60)));
        assert_eq!(c.get_keepalives_retries(), Some(9));
        assert_eq!(c.get_connect_timeout(), Some(&Duration::from_secs(5)));
    }

    #[test]
    fn prefer_to_a_remote_host_never_sends_credentials_in_plaintext() {
        assert_eq!(dial_mode(SslMode::Prefer, false), SslMode::Require);
        assert_eq!(dial_mode(SslMode::Prefer, true), SslMode::Prefer);
        for m in [
            SslMode::Disable,
            SslMode::Require,
            SslMode::VerifyCa,
            SslMode::VerifyFull,
        ] {
            assert_eq!(dial_mode(m, false), m);
        }
    }

    #[test]
    fn bare_url_leaves_mode_to_default() {
        assert_eq!(parse("postgres://u@db/app").unwrap().url_mode, None);
    }

    #[test]
    fn plaintext_to_remote_fails_unless_disable() {
        for mode in [
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
            SslMode::VerifyFull,
        ] {
            assert!(
                check_negotiated(mode, false, false, true).is_err(),
                "{mode:?}"
            );
        }
        assert!(check_negotiated(SslMode::Disable, false, false, false).is_ok());
    }

    #[test]
    fn plaintext_to_loopback_and_tls_pass() {
        assert!(check_negotiated(SslMode::Prefer, false, true, false).is_ok());
        assert!(check_negotiated(SslMode::VerifyFull, true, false, false).is_ok());
    }

    #[test]
    fn verify_mapping() {
        assert_eq!(SslMode::Require.verify(false), Verify::None);
        assert_eq!(SslMode::Require.verify(true), Verify::Chain);
        assert_eq!(SslMode::VerifyFull.verify(false), Verify::Full);
    }
}
