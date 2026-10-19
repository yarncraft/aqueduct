//! Pure framing for the Postgres frontend/backend protocol as used by a logical replication
//! connection. No I/O except [`read_backend`], which checks every length header **before** it
//! allocates the body.
//!
//! `postgres-protocol` has no `CopyBothResponse` and no replication messages, so the few
//! messages a replication session needs are framed here. Authentication is `postgres-protocol`'s
//! job and arrives with the TLS/auth slice; this module only recognises the auth request code.

use aqueduct_core::{Error, Result};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Largest accepted message body: Postgres' `MaxAllocSize`. Anything larger is a protocol error.
pub const MAX_FRAME: usize = 0x3fff_ffff;

/// Seconds between the Unix epoch and the Postgres epoch (2000-01-01).
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;

fn proto(msg: impl Into<String>) -> Error {
    Error::Source(format!("replication protocol: {}", msg.into()))
}

/// A backend message, as far as a replication session cares about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// `R`: authentication request or result; the code is 0 for `AuthenticationOk`.
    Auth(i32),
    /// `S`: runtime parameter report.
    ParameterStatus,
    /// `K`: cancellation key data.
    BackendKeyData,
    /// `Z`: ready for query.
    ReadyForQuery,
    /// `T`: row description. Replication commands return text columns in a fixed order.
    RowDescription,
    /// `D`: one row of text values (`None` is SQL NULL).
    DataRow(Vec<Option<Vec<u8>>>),
    /// `C`: command tag.
    CommandComplete(String),
    /// `W`: the server switched to `CopyBoth` mode (`START_REPLICATION` accepted).
    CopyBothResponse,
    /// `d`: a `CopyData` payload; see [`parse_copy_data`].
    CopyData(Vec<u8>),
    /// `c`: the server ended the copy stream.
    CopyDone,
    /// `E`: error response.
    Error { code: String, message: String },
    /// `N`: notice, ignored by callers.
    Notice,
    /// Any other tag, with its body length.
    Other(u8, usize),
}

/// Reads one backend message. The length header is validated against [`MAX_FRAME`] before any
/// buffer for the body exists, so a hostile or corrupt server cannot make us allocate more.
pub async fn read_backend<R: AsyncRead + Unpin>(r: &mut R) -> Result<Backend> {
    let tag = r
        .read_u8()
        .await
        .map_err(|e| proto(format!("reading message tag: {e}")))?;
    let len = r
        .read_i32()
        .await
        .map_err(|e| proto(format!("reading length of {:?}: {e}", tag as char)))?;
    let body_len = body_len(tag, len)?;
    let mut body = vec![0u8; body_len];
    r.read_exact(&mut body)
        .await
        .map_err(|e| proto(format!("reading body of {:?}: {e}", tag as char)))?;
    parse_backend(tag, &body)
}

/// Body length for a header, or a protocol error for a length that is too small or over the cap.
pub fn body_len(tag: u8, len: i32) -> Result<usize> {
    let body = usize::try_from(len)
        .ok()
        .and_then(|n| n.checked_sub(4))
        .ok_or_else(|| {
            proto(format!(
                "message {:?} has invalid length {len}",
                tag as char
            ))
        })?;
    if body > MAX_FRAME {
        return Err(proto(format!(
            "message {:?} announces {body} bytes, over the {MAX_FRAME} byte limit",
            tag as char
        )));
    }
    Ok(body)
}

/// Parses a message body already read from the wire.
pub fn parse_backend(tag: u8, body: &[u8]) -> Result<Backend> {
    let mut c = Cursor::new(body);
    Ok(match tag {
        b'R' => Backend::Auth(c.i32()?),
        b'S' => Backend::ParameterStatus,
        b'K' => Backend::BackendKeyData,
        b'Z' => Backend::ReadyForQuery,
        b'T' => Backend::RowDescription,
        b'D' => {
            let n = c.i16()?;
            let n = usize::try_from(n).map_err(|_| proto("negative column count in DataRow"))?;
            // Each column needs at least its 4 length bytes, so a count the body cannot hold is
            // rejected here instead of reserving for it.
            if n > body.len() / 4 {
                return Err(proto("DataRow column count exceeds its body"));
            }
            let mut cols = Vec::with_capacity(n);
            for _ in 0..n {
                let l = c.i32()?;
                if l == -1 {
                    cols.push(None);
                } else {
                    let l = usize::try_from(l).map_err(|_| proto("negative column length"))?;
                    cols.push(Some(c.bytes(l)?.to_vec()));
                }
            }
            Backend::DataRow(cols)
        }
        b'C' => Backend::CommandComplete(c.cstr()?),
        b'W' => Backend::CopyBothResponse,
        b'd' => Backend::CopyData(body.to_vec()),
        b'c' => Backend::CopyDone,
        b'E' | b'N' => {
            let (mut code, mut message) = (String::new(), String::new());
            loop {
                let field = c.u8()?;
                if field == 0 {
                    break;
                }
                let value = c.cstr()?;
                match field {
                    b'C' => code = value,
                    b'M' => message = value,
                    _ => {}
                }
            }
            if tag == b'E' {
                Backend::Error { code, message }
            } else {
                Backend::Notice
            }
        }
        other => Backend::Other(other, body.len()),
    })
}

/// A message carried inside `CopyData` on a replication connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replication {
    /// `w`: a chunk of WAL, here one pgoutput message.
    XLogData {
        wal_start: u64,
        wal_end: u64,
        /// Server clock, microseconds since 2000-01-01.
        server_time_micros: i64,
        data: Vec<u8>,
    },
    /// `k`: primary keepalive.
    Keepalive {
        wal_end: u64,
        server_time_micros: i64,
        reply_requested: bool,
    },
}

/// Parses a `CopyData` payload received after `START_REPLICATION`.
pub fn parse_copy_data(payload: &[u8]) -> Result<Replication> {
    let mut c = Cursor::new(payload);
    match c.u8()? {
        b'w' => {
            let wal_start = c.u64()?;
            let wal_end = c.u64()?;
            let server_time_micros = c.i64()?;
            Ok(Replication::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data: c.rest().to_vec(),
            })
        }
        b'k' => {
            let wal_end = c.u64()?;
            let server_time_micros = c.i64()?;
            let reply_requested = c.u8()? != 0;
            Ok(Replication::Keepalive {
                wal_end,
                server_time_micros,
                reply_requested,
            })
        }
        other => Err(proto(format!(
            "unknown CopyData message {:?}",
            other as char
        ))),
    }
}

/// Microseconds since 2000-01-01 for a Unix time in microseconds.
pub fn pg_micros(unix_micros: i64) -> i64 {
    unix_micros - PG_EPOCH_OFFSET_SECS * 1_000_000
}

// ---- frontend messages ----------------------------------------------------------------------

/// The startup message for a logical replication connection (`replication=database`).
pub fn startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes());
    for (k, v) in [
        ("user", user),
        ("database", database),
        ("replication", "database"),
    ] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = Vec::with_capacity(body.len() + 4);
    msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(body.len() + 5);
    msg.push(tag);
    msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    msg.extend_from_slice(body);
    msg
}

/// A simple `Query` message.
pub fn query(sql: &str) -> Vec<u8> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    tagged(b'Q', &body)
}

/// A `CopyData` message wrapping `payload`.
pub fn copy_data(payload: &[u8]) -> Vec<u8> {
    tagged(b'd', payload)
}

/// A `Terminate` message.
pub fn terminate() -> Vec<u8> {
    tagged(b'X', &[])
}

/// A standby status update (`r`) payload, to be wrapped by [`copy_data`].
pub fn status_update(
    write: u64,
    flush: u64,
    apply: u64,
    clock_micros: i64,
    reply: bool,
) -> Vec<u8> {
    let mut p = Vec::with_capacity(34);
    p.push(b'r');
    p.extend_from_slice(&write.to_be_bytes());
    p.extend_from_slice(&flush.to_be_bytes());
    p.extend_from_slice(&apply.to_be_bytes());
    p.extend_from_slice(&clock_micros.to_be_bytes());
    p.push(u8::from(reply));
    p
}

/// Quotes an SQL identifier for a replication command.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Quotes an SQL string literal for a replication command.
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Postgres `X/Y` hex text for an LSN.
pub fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

// ---- cursor ---------------------------------------------------------------------------------

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(|| proto("message is shorter than its contents"))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.bytes(N)?);
        Ok(a)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.array()?))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn cstr(&mut self) -> Result<String> {
        let rest = &self.buf[self.pos..];
        let n = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| proto("unterminated string"))?;
        let s = String::from_utf8_lossy(&rest[..n]).into_owned();
        self.pos += n + 1;
        Ok(s)
    }

    fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
        tagged(tag, body)
    }

    #[tokio::test]
    async fn reads_a_message_and_stops_at_its_end() {
        let mut bytes = wire(b'Z', b"I");
        bytes.extend(wire(b'C', b"START_REPLICATION\0"));
        let mut r = &bytes[..];
        assert_eq!(read_backend(&mut r).await.unwrap(), Backend::ReadyForQuery);
        assert_eq!(
            read_backend(&mut r).await.unwrap(),
            Backend::CommandComplete("START_REPLICATION".into())
        );
        assert!(read_backend(&mut r).await.is_err());
    }

    #[tokio::test]
    async fn over_cap_length_is_rejected_before_the_body_is_read() {
        // The header claims 1 GiB + 1 and no body follows: reading must fail on the length, not
        // on the missing bytes, and must not try to reserve the announced size.
        let mut bytes = vec![b'd'];
        bytes.extend(((MAX_FRAME as i32) + 5).to_be_bytes());
        let mut r = &bytes[..];
        let err = read_backend(&mut r).await.unwrap_err().to_string();
        assert!(err.contains("over the"), "{err}");
    }

    #[test]
    fn body_length_edges() {
        assert_eq!(body_len(b'd', 4).unwrap(), 0);
        assert_eq!(body_len(b'd', MAX_FRAME as i32 + 4).unwrap(), MAX_FRAME);
        assert!(body_len(b'd', MAX_FRAME as i32 + 5).is_err());
        assert!(body_len(b'd', 3).is_err());
        assert!(body_len(b'd', -1).is_err());
        assert!(body_len(b'd', i32::MIN).is_err());
    }

    #[test]
    fn xlogdata_and_keepalive_roundtrip() {
        let mut w = vec![b'w'];
        w.extend(0x10u64.to_be_bytes());
        w.extend(0x20u64.to_be_bytes());
        w.extend(7i64.to_be_bytes());
        w.extend(b"BEGIN");
        assert_eq!(
            parse_copy_data(&w).unwrap(),
            Replication::XLogData {
                wal_start: 0x10,
                wal_end: 0x20,
                server_time_micros: 7,
                data: b"BEGIN".to_vec()
            }
        );
        let mut k = vec![b'k'];
        k.extend(0x30u64.to_be_bytes());
        k.extend(9i64.to_be_bytes());
        k.push(1);
        assert_eq!(
            parse_copy_data(&k).unwrap(),
            Replication::Keepalive {
                wal_end: 0x30,
                server_time_micros: 9,
                reply_requested: true
            }
        );
        k.truncate(10);
        assert!(parse_copy_data(&k).is_err());
        assert!(parse_copy_data(b"z").is_err());
        assert!(parse_copy_data(b"").is_err());
    }

    #[test]
    fn data_row_and_error_response() {
        let mut row = 2i16.to_be_bytes().to_vec();
        row.extend(3i32.to_be_bytes());
        row.extend(b"abc");
        row.extend((-1i32).to_be_bytes());
        assert_eq!(
            parse_backend(b'D', &row).unwrap(),
            Backend::DataRow(vec![Some(b"abc".to_vec()), None])
        );
        // A column count the body cannot hold is an error, not a huge reservation.
        assert!(parse_backend(b'D', &i16::MAX.to_be_bytes()).is_err());
        assert!(parse_backend(b'D', &[0xff, 0xff]).is_err());

        let e = parse_backend(b'E', b"SERROR\0C28000\0Mno access\0\0").unwrap();
        assert_eq!(
            e,
            Backend::Error {
                code: "28000".into(),
                message: "no access".into()
            }
        );
        assert!(parse_backend(b'E', b"Mno terminator").is_err());
    }

    #[test]
    fn frontend_messages() {
        let s = startup_message("u", "d");
        assert_eq!(
            i32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize,
            s.len()
        );
        assert_eq!(&s[4..8], &196_608i32.to_be_bytes());
        assert!(s.ends_with(b"replication\0database\0\0"));
        assert_eq!(query("X"), b"Q\0\0\0\x06X\0");
        let u = status_update(1, 2, 3, 4, true);
        assert_eq!(u.len(), 34);
        assert_eq!(u[0], b'r');
        assert_eq!(copy_data(&u).len(), 39);
        assert_eq!(terminate(), b"X\0\0\0\x04");
    }

    #[test]
    fn quoting_and_lsn_text() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(lsn_text(0x16_B374_D848), "16/B374D848");
        assert_eq!(lsn_text(0), "0/0");
    }

    /// Random and truncated input must produce errors, never a panic. A stand-in for a fuzz
    /// target: deterministic, so a failure reproduces.
    #[tokio::test]
    async fn arbitrary_bytes_never_panic() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..5_000 {
            let n = (next() % 64) as usize;
            let buf: Vec<u8> = (0..n).map(|_| next() as u8).collect();
            let mut r = &buf[..];
            let _ = read_backend(&mut r).await;
            let _ = parse_copy_data(&buf);
            if let Some((tag, body)) = buf.split_first() {
                let _ = parse_backend(*tag, body);
            }
        }
    }
}
