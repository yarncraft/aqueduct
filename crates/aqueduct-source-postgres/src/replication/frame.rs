//! Framing for the Postgres frontend/backend protocol as used by a logical replication
//! connection.
//!
//! [`decode`] is pure: it takes complete messages off the front of a buffer and leaves partial
//! ones in place. [`FrameReader`] wraps it around a socket with a persistent buffer, so a read
//! that is cancelled (a losing `select!` branch, a timeout) loses nothing: the bytes it already
//! received stay in the buffer and the next read continues the same message. Every length
//! header is checked **before** the buffer grows for the body, and the buffer only grows with
//! bytes that arrive, so a hostile or corrupt server cannot make us allocate what it announces.
//!
//! `postgres-protocol` has no `CopyBothResponse` and no replication messages, so the few
//! messages a replication session needs are framed here.

use aqueduct_core::{Error, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Largest accepted message body: Postgres' `MaxAllocSize`. Anything larger is a protocol error.
pub const MAX_FRAME: usize = 0x3fff_ffff;

/// Largest accepted body for every message except `CopyData`: startup replies, `DataRow` from
/// `IDENTIFY_SYSTEM` and `ErrorResponse` are tiny, and this limit applies before authentication.
pub const MAX_CONTROL_FRAME: usize = 64 * 1024;

/// Largest accepted `ErrorResponse` or `NoticeResponse` once the session is authenticated: a
/// server error with a long detail or context must not kill a running stream.
pub const MAX_NOTICE_FRAME: usize = 1 << 20;

/// Tag and length: the bytes in front of every backend message body.
const HEADER: usize = 5;

/// Most the read buffer grows by before one read. A large frame is received in steps of this
/// size, so memory follows the bytes that actually arrive (at most about twice what arrived plus
/// this), not the announced length.
const READ_CHUNK: usize = 256 * 1024;

/// Read buffer capacity above which it is replaced once the large frame that grew it has been
/// handed out, so one huge message does not pin its memory for the life of the connection.
const SHRINK_ABOVE: usize = 4 << 20;

/// Seconds between the Unix epoch and the Postgres epoch (2000-01-01).
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;

pub(crate) fn proto(msg: impl Into<String>) -> Error {
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
    /// `d`: a `CopyData` payload; see [`parse_copy_data`]. It shares the read buffer's memory,
    /// so a large payload is never copied.
    CopyData(Bytes),
    /// `c`: the server ended the copy stream.
    CopyDone,
    /// `E`: error response.
    Error { code: String, message: String },
    /// `N`: notice, ignored by callers.
    Notice,
    /// `A`: asynchronous notification, ignored by callers.
    Notification,
    /// Any other tag, with its body length.
    Other(u8, usize),
}

/// Takes one complete backend message off the front of `buf`. Returns `Ok(None)` and leaves
/// `buf` untouched while the message is incomplete. An invalid or over-cap length header is an
/// error as soon as its 5 bytes are in `buf`, before anything is reserved for the body.
pub fn decode(buf: &mut BytesMut) -> Result<Option<Backend>> {
    decode_with(buf, false)
}

/// [`decode`], with the larger error and notice limit of an authenticated session when
/// `authenticated` is set.
pub fn decode_with(buf: &mut BytesMut, authenticated: bool) -> Result<Option<Backend>> {
    let Some(need) = frame_len(buf, authenticated)? else {
        return Ok(None);
    };
    if buf.len() < need {
        return Ok(None);
    }
    let tag = buf[0];
    let mut frame = buf.split_to(need);
    frame.advance(HEADER);
    parse_backend(tag, frame.freeze()).map(Some)
}

/// Total length (header included) of the message at the front of `buf`, once its header is
/// there.
fn frame_len(buf: &[u8], authenticated: bool) -> Result<Option<usize>> {
    let Some(header) = buf.get(..HEADER) else {
        return Ok(None);
    };
    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    Ok(Some(body_len(header[0], len, authenticated)? + HEADER))
}

/// Reads backend messages from `R` through a buffer that outlives each read.
///
/// **Cancel safety:** [`FrameReader::read`] is cancel-safe. Dropping its future before it
/// completes loses no data: the bytes received so far stay in the buffer and the next call
/// resumes the same message. It is safe to use in a `select!` branch or under a timeout.
#[derive(Debug)]
pub struct FrameReader<R> {
    io: R,
    buf: BytesMut,
    broken: bool,
    authenticated: bool,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(io: R) -> Self {
        FrameReader {
            io,
            buf: BytesMut::new(),
            broken: false,
            authenticated: false,
        }
    }

    /// Raises the limit on `ErrorResponse` and `NoticeResponse` to [`MAX_NOTICE_FRAME`]. Call
    /// it once the server has accepted authentication; before that, every message except
    /// `CopyData` is held to [`MAX_CONTROL_FRAME`].
    pub fn set_authenticated(&mut self) {
        self.authenticated = true;
    }

    /// The next backend message. After an error the reader stays broken: the stream position
    /// is unknown, so every later call fails too.
    pub async fn read(&mut self) -> Result<Backend> {
        if self.broken {
            return Err(proto("the connection is unusable after an earlier error"));
        }
        let r = self.read_inner().await;
        if r.is_err() {
            self.broken = true;
        }
        r
    }

    async fn read_inner(&mut self) -> Result<Backend> {
        loop {
            // `decode` is synchronous and only removes a message once it is complete, so the
            // only await point below is `read_buf`, which tokio documents as cancel-safe.
            let before = self.buf.len();
            if let Some(msg) = decode_with(&mut self.buf, self.authenticated)? {
                self.shrink(before - self.buf.len());
                return Ok(msg);
            }
            let missing = match frame_len(&self.buf, self.authenticated)? {
                Some(need) => need - self.buf.len(),
                None => HEADER - self.buf.len(),
            };
            self.buf.reserve(missing.clamp(HEADER, READ_CHUNK));
            let n = self
                .io
                .read_buf(&mut self.buf)
                .await
                .map_err(|e| proto(format!("read: {e}")))?;
            if n == 0 {
                return Err(
                    match (self.buf.first(), frame_len(&self.buf, self.authenticated)?) {
                        (None, _) => proto("the server closed the connection"),
                        (Some(tag), Some(need)) => proto(format!(
                            "connection closed in the body of {:?}: got {} of {} bytes",
                            *tag as char,
                            self.buf.len() - HEADER,
                            need - HEADER
                        )),
                        (Some(tag), None) => proto(format!(
                            "connection closed in the header of {:?}",
                            *tag as char
                        )),
                    },
                );
            }
        }
    }

    /// After a large frame has been split off, the remaining buffer still points into its
    /// allocation, and that allocation stays alive for as long as the buffer does (up to about
    /// 2 GiB after a 1 GiB frame). Moving the few remaining bytes to a fresh buffer lets it go
    /// once the caller drops the frame.
    fn shrink(&mut self, consumed: usize) {
        let large = consumed > READ_CHUNK || self.buf.capacity() > SHRINK_ABOVE;
        if large && self.buf.len() <= READ_CHUNK {
            self.buf = BytesMut::from(&self.buf[..]);
        }
    }

    /// Bytes received but not yet returned as a message.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Heap held by the read buffer.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

/// Body length for a header, or a protocol error for a length that is too small or over the cap.
/// `CopyData` may be up to [`MAX_FRAME`]; errors and notices up to [`MAX_NOTICE_FRAME`] once
/// `authenticated`; everything else up to [`MAX_CONTROL_FRAME`].
pub fn body_len(tag: u8, len: i32, authenticated: bool) -> Result<usize> {
    let body = usize::try_from(len)
        .ok()
        .and_then(|n| n.checked_sub(4))
        .ok_or_else(|| {
            proto(format!(
                "message {:?} has invalid length {len}",
                tag as char
            ))
        })?;
    let cap = match tag {
        b'd' => MAX_FRAME,
        b'E' | b'N' if authenticated => MAX_NOTICE_FRAME,
        _ => MAX_CONTROL_FRAME,
    };
    if body > cap {
        return Err(proto(format!(
            "message {:?} announces {body} bytes, over the {cap} byte limit",
            tag as char
        )));
    }
    Ok(body)
}

/// Parses a message body already read from the wire.
pub fn parse_backend(tag: u8, body: Bytes) -> Result<Backend> {
    if tag == b'd' {
        return Ok(Backend::CopyData(body));
    }
    let mut c = Cursor::new(&body);
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
        b'c' => Backend::CopyDone,
        b'A' => Backend::Notification,
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
        /// The pgoutput message. Shares the `CopyData` payload's memory.
        data: Bytes,
    },
    /// `k`: primary keepalive.
    Keepalive {
        wal_end: u64,
        server_time_micros: i64,
        reply_requested: bool,
    },
}

/// Size of the `XLogData` header: tag, start, end, clock.
const XLOGDATA_HEADER: usize = 1 + 8 + 8 + 8;

/// Parses a `CopyData` payload received after `START_REPLICATION`. The `XLogData` body is a
/// slice of `payload`, not a copy.
pub fn parse_copy_data(payload: Bytes) -> Result<Replication> {
    let mut c = Cursor::new(&payload);
    match c.u8()? {
        b'w' => {
            let wal_start = c.u64()?;
            let wal_end = c.u64()?;
            let server_time_micros = c.i64()?;
            Ok(Replication::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data: payload.slice(XLOGDATA_HEADER..),
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

/// Rejects a NUL in an operator-supplied name: it would cut a query short or add startup
/// parameters.
pub fn reject_nul(what: &str, value: &str) -> Result<()> {
    if value.contains('\0') {
        return Err(Error::Config(format!("{what} contains a NUL byte")));
    }
    Ok(())
}

// ---- frontend messages ----------------------------------------------------------------------

/// The startup message for a logical replication connection (`replication=database`).
///
/// `client_encoding=UTF8` is always sent, as tokio-postgres does for the SQL connection:
/// pgoutput converts text values to the client encoding, so without it a LATIN1 or SQL_ASCII
/// database would stream different bytes than poll mode's peek returns. `extra` carries the
/// URL's `application_name` and `options`.
pub fn startup_message(user: &str, database: &str, extra: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes());
    let fixed = [
        ("user", user),
        ("database", database),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
    ];
    for (k, v) in fixed.iter().chain(extra) {
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

/// A standby status update (`r`) payload, to be wrapped by [`copy_data`]. One LSN is reported
/// as written, flushed and applied (ADR 0003 §4): the client has no separate stages.
pub fn status_update(lsn: u64, clock_micros: i64, reply: bool) -> Vec<u8> {
    let mut p = Vec::with_capacity(34);
    p.push(b'r');
    for _ in 0..3 {
        p.extend_from_slice(&lsn.to_be_bytes());
    }
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
}

/// Feeds `input` to [`decode`] in chunks whose size comes from the first byte of `input`, and
/// parses every `CopyData` payload. Shared by the fuzz targets and the randomized tests: it
/// must return, never panic, and the buffer must never hold more than the bytes fed to it.
#[doc(hidden)]
pub fn fuzz_frames(input: &[u8]) {
    let Some((&step, mut rest)) = input.split_first() else {
        return;
    };
    let step = usize::from(step).max(1) * 97;
    let mut buf = BytesMut::new();
    let mut fed = 0usize;
    while !rest.is_empty() {
        let n = step.min(rest.len());
        buf.extend_from_slice(&rest[..n]);
        rest = &rest[n..];
        fed += n;
        loop {
            match decode(&mut buf) {
                Ok(Some(Backend::CopyData(p))) => {
                    let _ = parse_copy_data(p);
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => return,
            }
        }
        assert!(buf.len() <= fed, "decoder holds more than it was given");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
        tagged(tag, body)
    }

    async fn read_all(bytes: &[u8]) -> Vec<Result<Backend>> {
        let mut r = FrameReader::new(bytes);
        let mut out = Vec::new();
        loop {
            let m = r.read().await;
            let stop = m.is_err();
            out.push(m);
            if stop {
                return out;
            }
        }
    }

    #[tokio::test]
    async fn reads_a_message_and_stops_at_its_end() {
        let mut bytes = wire(b'Z', b"I");
        bytes.extend(wire(b'C', b"START_REPLICATION\0"));
        let out = read_all(&bytes).await;
        assert_eq!(out[0].as_ref().unwrap(), &Backend::ReadyForQuery);
        assert_eq!(
            out[1].as_ref().unwrap(),
            &Backend::CommandComplete("START_REPLICATION".into())
        );
        let eof = out[2].as_ref().unwrap_err().to_string();
        assert!(eof.contains("closed the connection"), "{eof}");
    }

    #[tokio::test]
    async fn over_cap_length_is_rejected_before_the_body_is_read() {
        // The header claims 1 GiB + 1 and no body follows: reading must fail on the length, not
        // on the missing bytes, and must not try to reserve the announced size.
        let mut bytes = vec![b'd'];
        bytes.extend(((MAX_FRAME as i32) + 5).to_be_bytes());
        let mut r = FrameReader::new(&bytes[..]);
        let err = r.read().await.unwrap_err().to_string();
        assert!(err.contains("over the"), "{err}");
        assert!(r.capacity() < 1024, "{}", r.capacity());
        // The reader stays broken.
        assert!(r.read().await.is_err());
    }

    #[tokio::test]
    async fn announced_size_is_not_allocated_up_front() {
        // MAX_FRAME is announced, 10 bytes arrive, then EOF. The read must fail on the short body
        // and the buffer must hold only about what arrived.
        let mut bytes = vec![b'd'];
        bytes.extend(((MAX_FRAME as i32) + 4).to_be_bytes());
        bytes.extend([7u8; 10]);
        let mut r = FrameReader::new(&bytes[..]);
        let err = r.read().await.unwrap_err().to_string();
        assert!(err.contains("got 10 of"), "{err}");
        assert!(r.capacity() <= 4 * READ_CHUNK, "{}", r.capacity());
    }

    #[tokio::test]
    async fn large_frame_grows_with_the_bytes_that_arrive() {
        // A 3 MiB CopyData arrives through a pipe; the buffer stays near the frame's size and
        // the payload comes back without a copy into a second buffer.
        let body = vec![0x5au8; 3 << 20];
        let (mut tx, rx) = tokio::io::duplex(64 * 1024);
        let mut r = FrameReader::new(rx);
        let frame = wire(b'd', &body);
        let writer = tokio::spawn(async move {
            tx.write_all(&frame).await.unwrap();
            tx
        });
        match r.read().await.unwrap() {
            Backend::CopyData(p) => assert_eq!(p.len(), body.len()),
            other => panic!("{other:?}"),
        }
        assert!(r.capacity() <= 2 * (body.len() + READ_CHUNK));
        drop(writer.await.unwrap());
    }

    #[tokio::test]
    async fn the_buffer_of_a_huge_frame_is_released_after_it() {
        // 6 MiB frame then a small one: once the frame is handed out the reader must not keep
        // its allocation (which can be ~2 GiB for a 1 GiB frame) for the rest of the session.
        let mut bytes = wire(b'd', &vec![1u8; 6 << 20]);
        bytes.extend(wire(b'Z', b"I"));
        let mut r = FrameReader::new(&bytes[..]);
        let big = r.read().await.unwrap();
        assert!(r.capacity() <= READ_CHUNK, "{}", r.capacity());
        drop(big);
        assert_eq!(r.read().await.unwrap(), Backend::ReadyForQuery);
        assert!(r.capacity() <= READ_CHUNK, "{}", r.capacity());
    }

    #[tokio::test]
    async fn a_dropped_read_resumes_the_same_message() {
        // ADR 0003 T3 selects over reads: a read cancelled mid-frame must not lose the bytes it
        // already took off the socket.
        let mut first = wire(b'd', b"wfirst-payload");
        let second = wire(b'Z', b"I");
        let tail = first.split_off(7); // header + 2 body bytes
        let (mut tx, rx) = tokio::io::duplex(1024);
        let mut r = FrameReader::new(rx);

        tx.write_all(&first[..3]).await.unwrap(); // part of the header only
        let cancelled = tokio::time::timeout(Duration::from_millis(20), r.read()).await;
        assert!(cancelled.is_err(), "nothing complete yet");
        tx.write_all(&first[3..]).await.unwrap(); // rest of the header, start of the body
        let cancelled = tokio::time::timeout(Duration::from_millis(20), r.read()).await;
        assert!(cancelled.is_err(), "body incomplete");
        assert_eq!(r.buffered(), first.len());

        tx.write_all(&tail).await.unwrap();
        tx.write_all(&second).await.unwrap();
        assert_eq!(
            r.read().await.unwrap(),
            Backend::CopyData(Bytes::from_static(b"wfirst-payload"))
        );
        assert_eq!(r.read().await.unwrap(), Backend::ReadyForQuery);
    }

    #[test]
    fn decode_leaves_partial_messages_in_place() {
        let mut all = wire(b'Z', b"I");
        all.extend(wire(b'd', b"abc"));
        for cut in 0..all.len() {
            let mut buf = BytesMut::from(&all[..cut]);
            let mut got = Vec::new();
            while let Some(m) = decode(&mut buf).unwrap() {
                got.push(m);
            }
            buf.extend_from_slice(&all[cut..]);
            while let Some(m) = decode(&mut buf).unwrap() {
                got.push(m);
            }
            assert_eq!(
                got,
                vec![
                    Backend::ReadyForQuery,
                    Backend::CopyData(Bytes::from_static(b"abc"))
                ],
                "cut at {cut}"
            );
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn errors_and_notices_may_be_larger_once_authenticated() {
        let big = (MAX_CONTROL_FRAME + 5) as i32;
        for tag in *b"EN" {
            assert!(body_len(tag, big, false).is_err());
            assert!(body_len(tag, big, true).is_ok());
            assert!(body_len(tag, (MAX_NOTICE_FRAME + 4) as i32, true).is_ok());
            assert!(body_len(tag, (MAX_NOTICE_FRAME + 5) as i32, true).is_err());
        }
        // Only errors and notices.
        assert!(body_len(b'D', big, true).is_err());
        assert!(body_len(b'Z', big, true).is_err());
    }

    #[tokio::test]
    async fn a_large_error_while_streaming_is_reported_not_rejected() {
        let mut body = b"SERROR\0CXX000\0M".to_vec();
        body.extend(vec![b'x'; 200_000]);
        body.extend(b"\0\0");
        let bytes = wire(b'E', &body);
        let mut r = FrameReader::new(&bytes[..]);
        r.set_authenticated();
        assert!(matches!(r.read().await.unwrap(), Backend::Error { .. }));
    }

    #[test]
    fn only_copy_data_may_be_large() {
        let over = (MAX_CONTROL_FRAME + 5) as i32;
        assert!(body_len(b'E', over, false).is_err());
        assert!(body_len(b'D', over, false).is_err());
        assert!(body_len(b'R', over, false).is_err());
        assert!(body_len(b'd', over, false).is_ok());
        assert!(body_len(b'E', (MAX_CONTROL_FRAME + 4) as i32, false).is_ok());
    }

    #[test]
    fn nul_bytes_are_refused() {
        assert!(reject_nul("slot", "a\0b").is_err());
        assert!(reject_nul("slot", "ab").is_ok());
        assert!(reject_nul("slot", "a b").is_ok());
    }

    #[test]
    fn body_length_edges() {
        assert_eq!(body_len(b'd', 4, false).unwrap(), 0);
        assert_eq!(
            body_len(b'd', MAX_FRAME as i32 + 4, false).unwrap(),
            MAX_FRAME
        );
        assert!(body_len(b'd', MAX_FRAME as i32 + 5, false).is_err());
        assert!(body_len(b'd', 3, false).is_err());
        assert!(body_len(b'd', -1, false).is_err());
        assert!(body_len(b'd', i32::MIN, false).is_err());
    }

    #[test]
    fn xlogdata_and_keepalive_roundtrip() {
        let mut w = vec![b'w'];
        w.extend(0x10u64.to_be_bytes());
        w.extend(0x20u64.to_be_bytes());
        w.extend(7i64.to_be_bytes());
        w.extend(b"BEGIN");
        assert_eq!(
            parse_copy_data(Bytes::from(w)).unwrap(),
            Replication::XLogData {
                wal_start: 0x10,
                wal_end: 0x20,
                server_time_micros: 7,
                data: Bytes::from_static(b"BEGIN")
            }
        );
        let mut k = vec![b'k'];
        k.extend(0x30u64.to_be_bytes());
        k.extend(9i64.to_be_bytes());
        k.push(1);
        assert_eq!(
            parse_copy_data(Bytes::from(k.clone())).unwrap(),
            Replication::Keepalive {
                wal_end: 0x30,
                server_time_micros: 9,
                reply_requested: true
            }
        );
        k.truncate(10);
        assert!(parse_copy_data(Bytes::from(k)).is_err());
        assert!(parse_copy_data(Bytes::from_static(b"z")).is_err());
        assert!(parse_copy_data(Bytes::new()).is_err());
        assert!(parse_copy_data(Bytes::from_static(b"w\0\0")).is_err());
    }

    #[test]
    fn data_row_and_error_response() {
        let mut row = 2i16.to_be_bytes().to_vec();
        row.extend(3i32.to_be_bytes());
        row.extend(b"abc");
        row.extend((-1i32).to_be_bytes());
        assert_eq!(
            parse_backend(b'D', Bytes::from(row)).unwrap(),
            Backend::DataRow(vec![Some(b"abc".to_vec()), None])
        );
        // A column count the body cannot hold is an error, not a huge reservation.
        assert!(parse_backend(b'D', Bytes::copy_from_slice(&i16::MAX.to_be_bytes())).is_err());
        assert!(parse_backend(b'D', Bytes::from_static(&[0xff, 0xff])).is_err());

        let e = parse_backend(b'E', Bytes::from_static(b"SERROR\0C28000\0Mno access\0\0")).unwrap();
        assert_eq!(
            e,
            Backend::Error {
                code: "28000".into(),
                message: "no access".into()
            }
        );
        assert!(parse_backend(b'E', Bytes::from_static(b"Mno terminator")).is_err());
    }

    #[test]
    fn frontend_messages() {
        let s = startup_message("u", "d", &[]);
        assert_eq!(
            i32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize,
            s.len()
        );
        assert_eq!(&s[4..8], &196_608i32.to_be_bytes());
        assert_eq!(
            &s[8..],
            b"user\0u\0database\0d\0replication\0database\0client_encoding\0UTF8\0\0"
        );
        let s = startup_message(
            "u",
            "d",
            &[("application_name", "aq"), ("options", "-c x=1")],
        );
        assert!(s.ends_with(b"client_encoding\0UTF8\0application_name\0aq\0options\0-c x=1\0\0"));
        assert_eq!(query("X"), b"Q\0\0\0\x06X\0");
        let u = status_update(0x0102, 4, true);
        assert_eq!(u.len(), 34);
        assert_eq!(u[0], b'r');
        // write, flush and apply all carry the one LSN
        for i in 0..3 {
            assert_eq!(&u[1 + 8 * i..9 + 8 * i], &0x0102u64.to_be_bytes());
        }
        assert_eq!(&u[25..33], &4i64.to_be_bytes());
        assert_eq!(u[33], 1);
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

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// Random and truncated input must produce errors, never a panic. A stable stand-in for the
    /// cargo-fuzz targets in `fuzz/`: deterministic, so a failure reproduces.
    #[test]
    fn arbitrary_bytes_never_panic() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..20_000 {
            let n = (rng.next() % 256) as usize;
            let buf: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
            fuzz_frames(&buf);
            let _ = parse_copy_data(Bytes::from(buf.clone()));
            if let Some((tag, body)) = buf.split_first() {
                let _ = parse_backend(*tag, Bytes::copy_from_slice(body));
            }
        }
    }

    /// Structured random streams that reach the paths plain random bytes rarely hit: lengths
    /// at and just over both caps, negative lengths, large `CopyData` bodies (up to 4 MiB),
    /// truncated streams and arbitrary chunking. The reader must return messages or an error,
    /// never panic, and its buffer must stay bounded by what was received.
    #[tokio::test]
    async fn structured_random_streams_stay_bounded() {
        let mut rng = Rng(0xD1B5_4A32_D192_ED03);
        let tags = b"RSKZTDCWdcENAX?";
        for round in 0..300 {
            let mut stream = Vec::new();
            for _ in 0..(1 + rng.next() % 6) {
                let tag = tags[(rng.next() % tags.len() as u64) as usize];
                let body_len = match rng.next() % 8 {
                    0 => (rng.next() % (4 << 20)) as usize,
                    1 => MAX_CONTROL_FRAME + (rng.next() % 3) as usize,
                    _ => (rng.next() % 128) as usize,
                };
                let announced: i64 = match rng.next() % 10 {
                    0 => MAX_FRAME as i64 + 4 + (rng.next() % 3) as i64,
                    1 => i64::from(rng.next() as i32),
                    2 => (rng.next() % 4) as i64,
                    _ => body_len as i64 + 4,
                };
                stream.push(tag);
                stream.extend((announced as i32).to_be_bytes());
                let mut body: Vec<u8> = (0..body_len.min(256)).map(|_| rng.next() as u8).collect();
                body.resize(body_len, 0);
                if tag == b'd' && !body.is_empty() {
                    body[0] = b"wk?"[(rng.next() % 3) as usize];
                }
                stream.extend(body);
            }
            if rng.next().is_multiple_of(3) {
                let cut = (rng.next() as usize) % (stream.len() + 1);
                stream.truncate(cut);
            }
            fuzz_frames(&stream);
            let total = stream.len();
            // Tiny writes for small streams, so headers split everywhere; larger ones for big
            // bodies, so the test stays fast.
            let chunk = if total > 64 * 1024 {
                4096 + (rng.next() % 65_536) as usize
            } else {
                1 + (rng.next() % 64) as usize
            };
            let (mut tx, rx) = tokio::io::duplex(chunk);
            let writer = tokio::spawn(async move {
                for part in stream.chunks(chunk) {
                    if tx.write_all(part).await.is_err() {
                        return;
                    }
                }
            });
            let mut r = FrameReader::new(rx);
            let mut got = 0usize;
            loop {
                match r.read().await {
                    Ok(Backend::CopyData(p)) => {
                        got += p.len() + HEADER;
                        let _ = parse_copy_data(p);
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
                assert!(
                    r.capacity() <= 2 * (total + READ_CHUNK),
                    "round {round}: capacity {} for {total} bytes",
                    r.capacity()
                );
            }
            assert!(got <= total);
            writer.abort();
        }
    }
}
