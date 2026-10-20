use aqueduct_core::{Error, Result};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_postgres::config::Host;

use super::auth::{self, AuthContext, AuthMethod};
use super::frame::{
    self, copy_data, lsn_text, pg_micros, proto, query, quote_ident, quote_literal, reject_nul,
    startup_message, status_update, terminate, Backend, FrameReader, Replication,
};
use super::secure;
use crate::source::parse_lsn;
use crate::SslMode;

fn err(msg: impl Into<String>) -> Error {
    Error::Source(format!("replication connection: {}", msg.into()))
}

/// The byte stream under a replication session: TCP, TLS over TCP, or an in-memory pipe in
/// tests.
pub(crate) trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
pub(crate) type BoxIo = Box<dyn Io>;

pub(super) type FrameRead = FrameReader<ReadHalf<BoxIo>>;

/// What `IDENTIFY_SYSTEM` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemInfo {
    pub system_id: String,
    pub timeline: u32,
    /// Current WAL flush position.
    pub xlogpos: u64,
    pub database: Option<String>,
}

/// TLS settings that override the URL's `sslmode` and `sslrootcert`, as
/// [`crate::PostgresSourceConfig`]'s `ssl_mode` and `ssl_root_cert` do for the SQL connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectOptions {
    pub ssl_mode: Option<SslMode>,
    pub ssl_root_cert: Option<String>,
}

/// Where and how to connect, validated before a socket is opened.
struct Target {
    host: String,
    port: u16,
    user: String,
    password: Option<Vec<u8>>,
    database: String,
    mode: SslMode,
    root_cert: Option<String>,
    /// Loopback host: plaintext and password methods are acceptable there.
    local: bool,
    application_name: Option<String>,
    /// Command-line options for the backend (`options=-c ...`), as the SQL connection sends.
    options: Option<String>,
    connect_timeout: Option<std::time::Duration>,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the password.
        f.debug_struct("Target")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("database", &self.database)
            .field("mode", &self.mode)
            .field("root_cert", &self.root_cert)
            .field("local", &self.local)
            .finish()
    }
}

impl Target {
    fn parse(url: &str, opts: &ConnectOptions) -> Result<Self> {
        let parsed = crate::tls::parse(url)?;
        let mode = opts.ssl_mode.or(parsed.url_mode).unwrap_or_default();
        let root_cert = opts.ssl_root_cert.clone().or(parsed.url_root_cert);
        let cfg = parsed.config;
        // `hostaddr` makes the SQL connection dial that address instead of `host`. This client
        // does not implement it, and dialing `host` instead would reach a different server than
        // the SQL connection, so it is refused.
        if !cfg.get_hostaddrs().is_empty() {
            return Err(Error::Config(
                "streaming replication does not support hostaddr in the postgres url; use host"
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
        let application_name = cfg.get_application_name().map(str::to_string);
        let options = cfg.get_options().map(str::to_string);
        for (what, v) in [
            ("application_name", &application_name),
            ("options", &options),
        ] {
            if let Some(v) = v {
                reject_nul(what, v)?;
            }
        }
        Ok(Target {
            host,
            port,
            user,
            password: cfg.get_password().map(<[u8]>::to_vec),
            database,
            mode,
            root_cert,
            local: crate::tls::is_local(&cfg),
            application_name,
            options,
            connect_timeout: cfg.get_connect_timeout().copied(),
        })
    }
}

impl Target {
    /// `application_name` and `options` from the URL, for the startup message.
    fn startup_extras(&self) -> Vec<(&str, &str)> {
        let mut extra = Vec::new();
        if let Some(v) = &self.application_name {
            extra.push(("application_name", v.as_str()));
        }
        if let Some(v) = &self.options {
            extra.push(("options", v.as_str()));
        }
        extra
    }
}

/// What to stream. `START_REPLICATION SLOT <slot> LOGICAL <start_lsn> (proto_version '1',
/// publication_names <publication>[, messages 'true'])`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartReplication<'a> {
    pub slot: &'a str,
    /// The LSN to resume from; `0` lets the slot choose its confirmed position.
    pub start_lsn: u64,
    /// One publication name, exactly as poll mode passes it to `publication_names`.
    pub publication: &'a str,
    /// Also send logical decoding messages (`pg_logical_emit_message`). Postgres 14+.
    pub logical_messages: bool,
}

impl StartReplication<'_> {
    fn sql(&self) -> Result<String> {
        reject_nul("slot name", self.slot)?;
        reject_nul("publication name", self.publication)?;
        // `publication_names` is a string that pgoutput splits on commas and case-folds like an
        // identifier list, the same parsing poll mode's peek gets for the same text. Quoting it
        // as a literal only, never as an identifier first, keeps both transports reading the
        // same publications.
        Ok(format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {}{})",
            quote_ident(self.slot),
            lsn_text(self.start_lsn),
            quote_literal(self.publication),
            if self.logical_messages {
                ", messages 'true'"
            } else {
                ""
            },
        ))
    }
}

/// One logical replication session before streaming starts: TLS and authentication are done,
/// replication commands can run.
pub struct ReplicationClient {
    reader: FrameRead,
    writer: StatusSender,
    encrypted: bool,
    auth: AuthMethod,
}

impl ReplicationClient {
    /// Connects, negotiates TLS, authenticates and completes startup.
    ///
    /// `url` is the same URL as the SQL connection and must name exactly one TCP host. TLS
    /// follows the same `sslmode` semantics as the SQL connection (default `verify-full`), and
    /// the same fail-closed rule: plaintext to a non-loopback host only with an explicit
    /// `sslmode=disable`. Cleartext and md5 passwords are never sent in plaintext to a
    /// non-loopback host. See the `auth` module for the SCRAM rules.
    pub async fn connect(url: &str, opts: &ConnectOptions) -> Result<Self> {
        let target = Target::parse(url, opts)?;
        match target.connect_timeout {
            Some(t) => tokio::time::timeout(t, Self::connect_to(&target))
                .await
                .map_err(|_| err(format!("connecting timed out after {t:?}")))?,
            None => Self::connect_to(&target).await,
        }
    }

    async fn connect_to(target: &Target) -> Result<Self> {
        let (host, port) = (&target.host, target.port);
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| err(format!("connecting to {host}:{port}: {e}")))?;
        tcp.set_nodelay(true).ok();
        let negotiated =
            secure::negotiate(tcp, host, target.mode, target.root_cert.as_deref()).await?;
        // The same policy as the SQL connection, checked before the startup message carries
        // the user name and before any password is sent.
        crate::tls::check_negotiated(
            target.mode,
            negotiated.encrypted,
            target.local,
            target.root_cert.is_some(),
        )?;
        let mut c = Self::from_io(negotiated.io);
        c.encrypted = negotiated.encrypted;
        c.writer
            .send(&startup_message(
                &target.user,
                &target.database,
                &target.startup_extras(),
            ))
            .await?;
        let ctx = AuthContext {
            user: &target.user,
            password: target.password.as_deref(),
            mode: target.mode,
            encrypted: negotiated.encrypted,
            local: target.local,
            cert_hash: negotiated.cert_hash,
            cert_algorithm: negotiated.cert_algorithm,
        };
        c.auth = auth::authenticate(&mut c.reader, &mut c.writer, &ctx).await?;
        Ok(c)
    }

    /// Whether the session runs over TLS.
    pub fn encrypted(&self) -> bool {
        self.encrypted
    }

    /// How the session authenticated.
    pub fn auth_method(&self) -> AuthMethod {
        self.auth
    }

    fn from_io(io: BoxIo) -> Self {
        let (r, w) = tokio::io::split(io);
        ReplicationClient {
            reader: FrameReader::new(r),
            writer: StatusSender { io: w },
            encrypted: false,
            auth: AuthMethod::Trust,
        }
    }

    /// Runs a replication command that returns rows, and returns them.
    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        self.writer.send(&query(sql)).await?;
        let mut rows = Vec::new();
        loop {
            match self.reader.read().await? {
                Backend::DataRow(r) => rows.push(r),
                Backend::RowDescription
                | Backend::CommandComplete(_)
                | Backend::Notice
                | Backend::ParameterStatus => {}
                Backend::ReadyForQuery => return Ok(rows),
                Backend::Error { code, message } => {
                    self.drain_to_ready().await?;
                    return Err(err(format!("{message} ({code})")));
                }
                other => return Err(proto(format!("unexpected {other:?} after a command"))),
            }
        }
    }

    /// After an `ErrorResponse`, reads to `ReadyForQuery` so the session stays usable.
    async fn drain_to_ready(&mut self) -> Result<()> {
        while !matches!(self.reader.read().await?, Backend::ReadyForQuery) {}
        Ok(())
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

    /// `START_REPLICATION SLOT .. LOGICAL ..` with pgoutput protocol 1. On success the session
    /// is in `CopyBoth` mode and splits into its two directions, which can be used from
    /// different tasks: reading never waits for a status update and the reverse.
    pub async fn start_replication(
        mut self,
        start: &StartReplication<'_>,
    ) -> Result<(ReplicationStream, StatusSender)> {
        let sql = start.sql()?;
        self.writer.send(&query(&sql)).await?;
        loop {
            match self.reader.read().await? {
                Backend::CopyBothResponse => {
                    return Ok((
                        ReplicationStream {
                            reader: self.reader,
                            ended: false,
                        },
                        self.writer,
                    ))
                }
                Backend::Notice | Backend::ParameterStatus => {}
                Backend::Error { code, message } => {
                    return Err(err(format!("START_REPLICATION failed: {message} ({code})")))
                }
                Backend::ReadyForQuery => {
                    return Err(err("server ended START_REPLICATION without streaming"))
                }
                other => {
                    return Err(proto(format!(
                        "unexpected {other:?} in reply to START_REPLICATION"
                    )))
                }
            }
        }
    }

    /// Sends `Terminate` and drops the connection. Best effort.
    pub async fn close(self) {
        self.writer.close().await;
    }
}

/// The receiving direction of a streaming session.
pub struct ReplicationStream {
    reader: FrameRead,
    ended: bool,
}

impl ReplicationStream {
    /// The next replication message, or `Ok(None)` when the server ends the stream with
    /// `CopyDone`.
    ///
    /// **Cancel safety:** cancel-safe. A call dropped before it completes (a losing `select!`
    /// branch, a timeout) loses no bytes; the next call continues the same message.
    ///
    /// After `Ok(None)` or an error the session is over: this client does not finish the
    /// `CopyDone` handshake or run further commands, so the caller must drop both halves and
    /// reconnect. Later calls return an error.
    pub async fn next_message(&mut self) -> Result<Option<Replication>> {
        if self.ended {
            return Err(err(
                "the server ended the stream; drop the connection and reconnect",
            ));
        }
        loop {
            match self.reader.read().await? {
                Backend::CopyData(payload) => return frame::parse_copy_data(payload).map(Some),
                Backend::CopyDone => {
                    self.ended = true;
                    return Ok(None);
                }
                // Asynchronous messages the server may send at any time.
                Backend::Notice | Backend::ParameterStatus | Backend::Notification => {}
                Backend::Error { code, message } => {
                    self.ended = true;
                    return Err(err(format!("stream failed: {message} ({code})")));
                }
                other => {
                    self.ended = true;
                    return Err(proto(format!("unexpected {other:?} while streaming")));
                }
            }
        }
    }
}

/// The sending direction of a session: standby status updates and `Terminate`.
///
/// Writes are **not** cancel-safe: a write dropped halfway leaves a partial message on the
/// wire. Await each call to completion; do not race it in `select!`.
pub struct StatusSender {
    io: WriteHalf<BoxIo>,
}

impl StatusSender {
    pub(super) async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.io
            .write_all(bytes)
            .await
            .map_err(|e| err(format!("write: {e}")))?;
        self.io
            .flush()
            .await
            .map_err(|e| err(format!("flush: {e}")))
    }

    /// Standby status update. `flush_lsn` is reported as written, flushed and applied: the
    /// server may discard WAL up to it and confirms the slot to it. `reply` asks the server to
    /// answer with a keepalive.
    pub async fn send_status(&mut self, flush_lsn: u64, reply: bool) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let payload = status_update(flush_lsn, pg_micros(now), reply);
        self.send(&copy_data(&payload)).await
    }

    /// Sends `Terminate` and drops this half. Best effort.
    pub async fn close(mut self) {
        let _ = self.send(&terminate()).await;
    }
}

/// A streaming session over an arbitrary byte stream, as if `START_REPLICATION` had succeeded.
/// For tests and fuzzing that script the server side.
pub(crate) fn streaming(io: BoxIo) -> (ReplicationStream, StatusSender) {
    let c = ReplicationClient::from_io(io);
    (
        ReplicationStream {
            reader: c.reader,
            ended: false,
        },
        c.writer,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut m = vec![tag];
        m.extend(((body.len() + 4) as i32).to_be_bytes());
        m.extend(body);
        m
    }

    fn keepalive(wal_end: u64, reply: bool) -> Vec<u8> {
        let mut k = vec![b'k'];
        k.extend(wal_end.to_be_bytes());
        k.extend(0i64.to_be_bytes());
        k.push(u8::from(reply));
        wire(b'd', &k)
    }

    fn target(url: &str) -> Result<Target> {
        Target::parse(url, &ConnectOptions::default())
    }

    #[test]
    fn target_takes_tls_settings_from_the_url_or_the_overrides() {
        let t = target("postgres://u:secret@localhost:5436/d?sslmode=require&sslrootcert=/ca.pem")
            .unwrap();
        assert_eq!(
            (t.host.as_str(), t.port, t.database.as_str()),
            ("localhost", 5436, "d")
        );
        assert_eq!(t.mode, SslMode::Require);
        assert_eq!(t.root_cert.as_deref(), Some("/ca.pem"));
        assert_eq!(t.password.as_deref(), Some(&b"secret"[..]));
        assert!(t.local);
        // No sslmode means verify-full.
        assert_eq!(
            target("postgres://u@db.example.com/d").unwrap().mode,
            SslMode::VerifyFull
        );
        assert!(!target("postgres://u@db.example.com/d").unwrap().local);
        // Config overrides win over the URL, as for the SQL connection.
        let opts = ConnectOptions {
            ssl_mode: Some(SslMode::VerifyCa),
            ssl_root_cert: Some("/other.pem".into()),
        };
        let t = Target::parse("postgres://u@localhost/d?sslmode=disable", &opts).unwrap();
        assert_eq!(t.mode, SslMode::VerifyCa);
        assert_eq!(t.root_cert.as_deref(), Some("/other.pem"));
    }

    #[test]
    fn password_is_never_in_debug_output() {
        let t = target("postgres://u:hunter2@localhost/d").unwrap();
        let shown = format!("{t:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    #[test]
    fn multi_host_and_unix_socket_urls_are_refused() {
        let e = target("postgres://u@localhost,db.example.com/d")
            .unwrap_err()
            .to_string();
        assert!(e.contains("multi-host"), "{e}");
        assert!(target("host=/var/run/postgresql user=u").is_err());
    }

    #[test]
    fn application_name_and_options_reach_the_startup_message() {
        let t = target(
            "postgres://u@localhost/d?sslmode=disable&application_name=aq&options=-c%20statement_timeout%3D0",
        )
        .unwrap();
        assert_eq!(
            t.startup_extras(),
            vec![
                ("application_name", "aq"),
                ("options", "-c statement_timeout=0")
            ]
        );
        let t = target("postgres://u@localhost/d?sslmode=disable").unwrap();
        assert!(t.startup_extras().is_empty());
    }

    #[test]
    fn hostaddr_is_refused() {
        // The SQL connection would dial 10.0.0.5; dialing localhost instead would reach another
        // server.
        let e = target("host=localhost hostaddr=10.0.0.5 user=u sslmode=disable")
            .unwrap_err()
            .to_string();
        assert!(e.contains("hostaddr"), "{e}");
    }

    #[test]
    fn nul_in_user_or_database_is_refused() {
        assert!(target("postgres://u@localhost/d%00x?sslmode=disable").is_err());
    }

    #[test]
    fn publication_is_quoted_as_a_literal_only() {
        let start = |publication| StartReplication {
            slot: "My\"Slot",
            start_lsn: 0x1_0000_00A0,
            publication,
            logical_messages: false,
        };
        assert_eq!(
            start("Aq_Pub").sql().unwrap(),
            "START_REPLICATION SLOT \"My\"\"Slot\" LOGICAL 1/A0 \
             (proto_version '1', publication_names 'Aq_Pub')"
        );
        // Same text pgoutput gets from the peek: commas split, unquoted names case-fold.
        assert!(start("a,\"B\"")
            .sql()
            .unwrap()
            .ends_with("publication_names 'a,\"B\"')"));
        assert!(start("it's").sql().unwrap().ends_with("'it''s')"));
        let with_messages = StartReplication {
            logical_messages: true,
            ..start("p")
        };
        assert!(with_messages
            .sql()
            .unwrap()
            .ends_with("publication_names 'p', messages 'true')"));
        assert!(start("p\0").sql().is_err());
    }

    async fn stream_of(server_bytes: Vec<u8>) -> (ReplicationStream, tokio::io::DuplexStream) {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        server.write_all(&server_bytes).await.unwrap();
        let (stream, _status) = streaming(Box::new(client));
        // `_status` is dropped: the write half going away does not close reading.
        (stream, server)
    }

    #[tokio::test]
    async fn async_messages_are_skipped_while_streaming() {
        let mut bytes = wire(b'N', b"SNOTICE\0Mhello\0\0");
        bytes.extend(wire(b'S', b"application_name\0x\0"));
        bytes.extend(wire(b'A', b"\0\0\0\x01chan\0payload\0"));
        bytes.extend(keepalive(42, true));
        let (mut s, _server) = stream_of(bytes).await;
        assert_eq!(
            s.next_message().await.unwrap(),
            Some(Replication::Keepalive {
                wal_end: 42,
                server_time_micros: 0,
                reply_requested: true
            })
        );
    }

    #[tokio::test]
    async fn unexpected_messages_while_streaming_are_protocol_errors() {
        for (tag, body) in [
            (b'Z', &b"I"[..]),
            (b'C', b"COPY 0\0"),
            (b'T', b"\0\0"),
            (b'W', b"\0\0\0"),
            (b'R', b"\0\0\0\0"),
            (b'?', b""),
        ] {
            let (mut s, _server) = stream_of(wire(tag, body)).await;
            let e = s.next_message().await.unwrap_err().to_string();
            assert!(e.contains("while streaming"), "{}: {e}", tag as char);
            assert!(s.next_message().await.is_err());
        }
    }

    #[tokio::test]
    async fn server_copy_done_ends_the_stream_for_good() {
        let mut bytes = keepalive(7, false);
        bytes.extend(wire(b'c', b""));
        let (mut s, _server) = stream_of(bytes).await;
        assert!(matches!(
            s.next_message().await.unwrap(),
            Some(Replication::Keepalive { wal_end: 7, .. })
        ));
        assert_eq!(s.next_message().await.unwrap(), None);
        let e = s.next_message().await.unwrap_err().to_string();
        assert!(e.contains("reconnect"), "{e}");
    }

    #[tokio::test]
    async fn status_updates_do_not_wait_for_reads() {
        // Nothing is ever sent to the client, so a read would block forever; the status update
        // goes out on the other half regardless.
        let (client, mut server) = tokio::io::duplex(1024);
        let (mut stream, mut status) = streaming(Box::new(client));
        let reading = tokio::spawn(async move { stream.next_message().await });
        status.send_status(0x0A0B, true).await.unwrap();
        let mut msg = [0u8; 39];
        server.read_exact(&mut msg).await.unwrap();
        assert_eq!(&msg[..6], &[b'd', 0, 0, 0, 38, b'r']);
        for i in 0..3 {
            assert_eq!(&msg[6 + 8 * i..14 + 8 * i], &0x0A0Bu64.to_be_bytes());
        }
        assert_eq!(msg[38], 1);
        assert!(!reading.is_finished());
        reading.abort();
    }
}

/// The connection handshake against scripted servers on loopback: TLS negotiation answers and
/// the authentication exchange, including servers that misbehave.
#[cfg(test)]
mod handshake_tests {
    use super::*;
    use std::future::Future;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    async fn server<F, Fut>(script: F) -> (u16, tokio::task::JoinHandle<()>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            script(s).await;
        });
        (port, h)
    }

    /// An untagged message: `SSLRequest` or the startup message.
    async fn read_untagged(s: &mut TcpStream) -> Vec<u8> {
        let len = s.read_i32().await.unwrap() as usize;
        let mut body = vec![0; len - 4];
        s.read_exact(&mut body).await.unwrap();
        body
    }

    async fn read_msg(s: &mut TcpStream) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.unwrap();
        (tag, read_untagged(s).await)
    }

    fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut m = vec![tag];
        m.extend(((body.len() + 4) as i32).to_be_bytes());
        m.extend(body);
        m
    }

    fn auth(code: i32, data: &[u8]) -> Vec<u8> {
        let mut b = code.to_be_bytes().to_vec();
        b.extend(data);
        msg(b'R', &b)
    }

    fn ok_and_ready() -> Vec<u8> {
        let mut m = auth(0, b"");
        m.extend(msg(b'Z', b"I"));
        m
    }

    async fn connect(url: String) -> Result<ReplicationClient> {
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            ReplicationClient::connect(&url, &ConnectOptions::default()),
        )
        .await
        .expect("connect hung")
    }

    #[tokio::test]
    async fn plaintext_after_the_tls_answer_never_reaches_the_protocol() {
        // CVE-2021-23214: a server (or someone on the path) answers `S` and appends a
        // plaintext AuthenticationOk + ReadyForQuery. Those bytes must go to the TLS handshake,
        // which rejects them, not be read as a successful startup.
        let (port, _h) = server(|mut s| async move {
            assert_eq!(read_untagged(&mut s).await, 80877103i32.to_be_bytes());
            let mut answer = b"S".to_vec();
            answer.extend(ok_and_ready());
            s.write_all(&answer).await.unwrap();
            let mut sink = Vec::new();
            let _ = s.read_to_end(&mut sink).await;
        })
        .await;
        let e = connect(format!("postgres://u@127.0.0.1:{port}/d?sslmode=require"))
            .await
            .err()
            .expect("must not connect")
            .to_string();
        assert!(e.contains("TLS handshake"), "{e}");
    }

    #[tokio::test]
    async fn a_server_without_tls_is_refused_unless_prefer() {
        for mode in ["require", "verify-ca", "verify-full"] {
            let (port, _h) = server(|mut s| async move {
                read_untagged(&mut s).await;
                s.write_all(b"N").await.unwrap();
            })
            .await;
            let e = connect(format!("postgres://u@localhost:{port}/d?sslmode={mode}"))
                .await
                .err()
                .expect("must not connect")
                .to_string();
            assert!(e.contains("server does not support TLS"), "{mode}: {e}");
        }
        // prefer continues in plaintext (loopback, so the policy allows it).
        let (port, h) = server(|mut s| async move {
            read_untagged(&mut s).await;
            s.write_all(b"N").await.unwrap();
            let startup = read_untagged(&mut s).await;
            assert!(startup.ends_with(b"replication\0database\0client_encoding\0UTF8\0\0"));
            s.write_all(&ok_and_ready()).await.unwrap();
        })
        .await;
        let c = connect(format!("postgres://u@localhost:{port}/d?sslmode=prefer"))
            .await
            .unwrap();
        assert!(!c.encrypted());
        assert_eq!(c.auth_method(), AuthMethod::Trust);
        h.await.unwrap();
    }

    /// Runs a SCRAM exchange up to the client-final message, then `finish` decides the end.
    async fn scram_server(
        finish: impl FnOnce(TcpStream) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + 'static,
    ) -> u16 {
        let (port, _h) = server(|mut s| async move {
            read_untagged(&mut s).await; // startup
            s.write_all(&auth(10, b"SCRAM-SHA-256\0\0")).await.unwrap();
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'p');
            assert!(body.starts_with(b"SCRAM-SHA-256\0"));
            let first = String::from_utf8(body[14 + 4..].to_vec()).unwrap();
            assert!(
                first.starts_with("n,,"),
                "plaintext session must not claim binding"
            );
            let nonce = first.split("r=").nth(1).unwrap().to_string();
            let server_first = format!("r={nonce}srvnonce,s=c2FsdA==,i=4096");
            s.write_all(&auth(11, server_first.as_bytes()))
                .await
                .unwrap();
            let (tag, _client_final) = read_msg(&mut s).await;
            assert_eq!(tag, b'p');
            finish(s).await;
        })
        .await;
        port
    }

    #[tokio::test]
    async fn a_wrong_scram_server_signature_is_rejected() {
        // The server does not know the password, so it cannot produce the right signature.
        let port = scram_server(|mut s| {
            Box::pin(async move {
                let wrong = format!("v={}=", "A".repeat(43));
                s.write_all(&auth(12, wrong.as_bytes())).await.unwrap();
                s.write_all(&ok_and_ready()).await.unwrap();
            })
        })
        .await;
        let e = connect(format!(
            "postgres://u:pw@localhost:{port}/d?sslmode=disable"
        ))
        .await
        .err()
        .expect("must not connect")
        .to_string();
        assert!(e.contains("signature does not verify"), "{e}");
        assert!(!e.contains("pw@"), "{e}");
    }

    #[tokio::test]
    async fn authentication_ok_that_skips_the_scram_proof_is_rejected() {
        let port = scram_server(|mut s| {
            Box::pin(async move {
                s.write_all(&ok_and_ready()).await.unwrap();
            })
        })
        .await;
        let e = connect(format!(
            "postgres://u:pw@localhost:{port}/d?sslmode=disable"
        ))
        .await
        .err()
        .expect("must not connect")
        .to_string();
        assert!(e.contains("without proving"), "{e}");
    }

    #[tokio::test]
    async fn md5_and_cleartext_send_the_expected_password_on_loopback() {
        let salt = [1u8, 2, 3, 4];
        let (port, h) = server(move |mut s| async move {
            read_untagged(&mut s).await;
            s.write_all(&auth(5, &salt)).await.unwrap();
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'p');
            let mut expected =
                postgres_protocol::authentication::md5_hash(b"u", b"pw", salt).into_bytes();
            expected.push(0);
            assert_eq!(body, expected);
            s.write_all(&ok_and_ready()).await.unwrap();
        })
        .await;
        let c = connect(format!(
            "postgres://u:pw@localhost:{port}/d?sslmode=disable"
        ))
        .await
        .unwrap();
        assert_eq!(c.auth_method(), AuthMethod::Md5);
        h.await.unwrap();

        let (port, h) = server(|mut s| async move {
            read_untagged(&mut s).await;
            s.write_all(&auth(3, b"")).await.unwrap();
            assert_eq!(read_msg(&mut s).await, (b'p', b"pw\0".to_vec()));
            s.write_all(&ok_and_ready()).await.unwrap();
        })
        .await;
        let c = connect(format!(
            "postgres://u:pw@127.0.0.1:{port}/d?sslmode=disable"
        ))
        .await
        .unwrap();
        assert_eq!(c.auth_method(), AuthMethod::Cleartext);
        h.await.unwrap();
    }

    #[tokio::test]
    async fn unsupported_methods_and_missing_passwords_fail_clearly() {
        let (port, _h) = server(|mut s| async move {
            read_untagged(&mut s).await;
            s.write_all(&auth(7, b"")).await.unwrap();
        })
        .await;
        let e = connect(format!(
            "postgres://u:pw@localhost:{port}/d?sslmode=disable"
        ))
        .await
        .err()
        .unwrap()
        .to_string();
        assert!(e.contains("GSSAPI"), "{e}");

        let (port, _h) = server(|mut s| async move {
            read_untagged(&mut s).await;
            s.write_all(&auth(3, b"")).await.unwrap();
        })
        .await;
        let e = connect(format!("postgres://u@localhost:{port}/d?sslmode=disable"))
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("has none"), "{e}");
    }
}
