use aqueduct_core::{Error, Result};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::config::Host;

use super::frame::{
    self, copy_data, lsn_text, pg_micros, query, quote_ident, quote_literal, read_backend,
    reject_nul, startup_message, status_update, terminate, Backend, Replication,
};
use crate::source::parse_lsn;

fn err(msg: impl Into<String>) -> Error {
    Error::Source(format!("replication connection: {}", msg.into()))
}

/// What `IDENTIFY_SYSTEM` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemInfo {
    pub system_id: String,
    pub timeline: u32,
    /// Current WAL flush position.
    pub xlogpos: u64,
    pub database: Option<String>,
}

/// Where to connect, validated. Fails closed while the client cannot do TLS: any `sslmode`
/// other than `disable` and any non-loopback host is refused before a socket is opened.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
    user: String,
    database: String,
}

impl Target {
    fn parse(url: &str) -> Result<Self> {
        let parsed = crate::tls::parse(url)?;
        let mode = parsed.url_mode.unwrap_or_default();
        if mode != crate::SslMode::Disable {
            return Err(Error::Config(format!(
                "streaming replication cannot use TLS yet, so it refuses sslmode={mode:?};                  only sslmode=disable on a loopback host is accepted"
            )));
        }
        let cfg = parsed.config;
        if !crate::tls::is_local(&cfg) {
            return Err(Error::Config(
                "streaming replication cannot use TLS yet, so it refuses a non-loopback host"
                    .into(),
            ));
        }
        let host = match cfg.get_hosts() {
            [Host::Tcp(h)] => h.clone(),
            [] => return Err(Error::Config("postgres url has no host".into())),
            [_] => {
                return Err(Error::Config(
                    "streaming replication needs a TCP host, not a unix socket".into(),
                ))
            }
            _ => {
                return Err(Error::Config(
                    "streaming replication does not support a multi-host postgres url".into(),
                ))
            }
        };
        let port = match cfg.get_ports() {
            [p] => *p,
            [] => 5432,
            _ => return Err(Error::Config("postgres url has several ports".into())),
        };
        let user = cfg
            .get_user()
            .ok_or_else(|| Error::Config("postgres url has no user".into()))?
            .to_string();
        let database = cfg.get_dbname().unwrap_or(&user).to_string();
        reject_nul("user", &user)?;
        reject_nul("database", &database)?;
        Ok(Target {
            host,
            port,
            user,
            database,
        })
    }
}

/// One logical replication session over a plaintext TCP connection with trust authentication.
pub struct ReplicationClient {
    io: BufReader<TcpStream>,
    streaming: bool,
}

impl ReplicationClient {
    /// Connects and completes startup. `url` is the same URL as the SQL connection; it must name
    /// exactly one TCP host. The server must accept the user without a password (`trust`):
    /// password authentication and TLS are not part of this slice and are refused with an error.
    pub async fn connect(url: &str) -> Result<Self> {
        let target = Target::parse(url)?;
        let (host, port) = (&target.host, target.port);
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| err(format!("connecting to {host}:{port}: {e}")))?;
        tcp.set_nodelay(true).ok();
        let mut c = ReplicationClient {
            io: BufReader::new(tcp),
            streaming: false,
        };
        c.send(&startup_message(&target.user, &target.database))
            .await?;
        loop {
            match c.read().await? {
                Backend::Auth(0) => {}
                Backend::Auth(code) => {
                    return Err(err(format!(
                        "server requested authentication method {code}; only trust \
                         authentication is supported yet"
                    )))
                }
                Backend::ReadyForQuery => return Ok(c),
                Backend::Error { code, message } => {
                    return Err(err(format!("startup failed: {message} ({code})")))
                }
                _ => {}
            }
        }
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        let w = self.io.get_mut();
        w.write_all(bytes)
            .await
            .map_err(|e| err(format!("write: {e}")))?;
        w.flush().await.map_err(|e| err(format!("flush: {e}")))
    }

    async fn read(&mut self) -> Result<Backend> {
        read_backend(&mut self.io).await
    }

    /// Runs a replication command that returns rows, and returns them.
    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        if self.streaming {
            return Err(err("cannot run a command while streaming"));
        }
        self.send(&query(sql)).await?;
        let mut rows = Vec::new();
        loop {
            match self.read().await? {
                Backend::DataRow(r) => rows.push(r),
                Backend::ReadyForQuery => return Ok(rows),
                Backend::Error { code, message } => {
                    // Drain to ReadyForQuery so the session stays usable.
                    while !matches!(self.read().await?, Backend::ReadyForQuery) {}
                    return Err(err(format!("{message} ({code})")));
                }
                _ => {}
            }
        }
    }

    /// `IDENTIFY_SYSTEM`.
    pub async fn identify_system(&mut self) -> Result<SystemInfo> {
        let rows = self.rows("IDENTIFY_SYSTEM").await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| err("IDENTIFY_SYSTEM returned no row"))?;
        let text = |i: usize| -> Result<Option<String>> {
            row.get(i)
                .ok_or_else(|| err("IDENTIFY_SYSTEM row is too short"))?
                .as_ref()
                .map(|b| {
                    String::from_utf8(b.clone()).map_err(|_| err("non-UTF-8 IDENTIFY_SYSTEM value"))
                })
                .transpose()
        };
        let need = |v: Option<String>| v.ok_or_else(|| err("IDENTIFY_SYSTEM returned NULL"));
        Ok(SystemInfo {
            system_id: need(text(0)?)?,
            timeline: need(text(1)?)?
                .parse()
                .map_err(|_| err("bad timeline in IDENTIFY_SYSTEM"))?,
            xlogpos: parse_lsn(&need(text(2)?)?)?,
            database: text(3)?,
        })
    }

    /// `START_REPLICATION SLOT .. LOGICAL ..` with pgoutput protocol 1 and one publication.
    /// `start` is the LSN to resume from; `0` lets the slot choose its confirmed position.
    pub async fn start_replication(
        &mut self,
        slot: &str,
        start: u64,
        publication: &str,
    ) -> Result<()> {
        if self.streaming {
            return Err(err("replication already started"));
        }
        reject_nul("slot name", slot)?;
        reject_nul("publication name", publication)?;
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {})",
            quote_ident(slot),
            lsn_text(start),
            quote_literal(&quote_ident(publication)),
        );
        self.send(&query(&sql)).await?;
        loop {
            match self.read().await? {
                Backend::CopyBothResponse => {
                    self.streaming = true;
                    return Ok(());
                }
                Backend::Error { code, message } => {
                    while !matches!(self.read().await?, Backend::ReadyForQuery) {}
                    return Err(err(format!("START_REPLICATION failed: {message} ({code})")));
                }
                Backend::ReadyForQuery => {
                    return Err(err("server ended START_REPLICATION without streaming"))
                }
                _ => {}
            }
        }
    }

    /// Next replication message; `Ok(None)` when the server ends the stream cleanly.
    pub async fn next_message(&mut self) -> Result<Option<Replication>> {
        if !self.streaming {
            return Err(err("not streaming"));
        }
        loop {
            match self.read().await? {
                Backend::CopyData(payload) => return frame::parse_copy_data(&payload).map(Some),
                Backend::CopyDone => {
                    self.streaming = false;
                    return Ok(None);
                }
                Backend::Error { code, message } => {
                    return Err(err(format!("stream failed: {message} ({code})")))
                }
                _ => {}
            }
        }
    }

    /// Standby status update. `flush` is the position the server may discard WAL up to.
    pub async fn send_status(&mut self, write: u64, flush: u64, reply: bool) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let payload = status_update(write, flush, flush, pg_micros(now), reply);
        self.send(&copy_data(&payload)).await
    }

    /// Sends `Terminate` and drops the connection. Best effort.
    pub async fn close(mut self) {
        let _ = self.send(&terminate()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_loopback_is_the_only_target() {
        let t = Target::parse("postgres://u@localhost:5436/d?sslmode=disable").unwrap();
        assert_eq!(
            (t.host.as_str(), t.port, t.database.as_str()),
            ("localhost", 5436, "d")
        );
        assert!(Target::parse("postgres://u@127.0.0.1/d?sslmode=disable").is_ok());
    }

    #[test]
    fn any_tls_mode_is_refused_until_tls_exists() {
        // No sslmode means verify-full, which this client cannot honour.
        for url in [
            "postgres://u@localhost/d",
            "postgres://u@localhost/d?sslmode=prefer",
            "postgres://u@localhost/d?sslmode=require",
            "postgres://u@localhost/d?sslmode=verify-ca",
            "postgres://u@localhost/d?sslmode=verify-full",
        ] {
            let e = Target::parse(url).unwrap_err().to_string();
            assert!(e.contains("cannot use TLS yet"), "{url}: {e}");
        }
    }

    #[test]
    fn non_loopback_hosts_are_refused_even_with_sslmode_disable() {
        for url in [
            "postgres://u@db.example.com/d?sslmode=disable",
            "postgres://u@10.0.0.5/d?sslmode=disable",
            "postgres://u@localhost,db.example.com/d?sslmode=disable",
        ] {
            assert!(Target::parse(url).is_err(), "{url}");
        }
    }

    #[test]
    fn nul_in_user_or_database_is_refused() {
        assert!(Target::parse("postgres://u@localhost/d%00x?sslmode=disable").is_err());
    }
}
