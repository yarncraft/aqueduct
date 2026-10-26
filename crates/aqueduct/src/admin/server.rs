//! The cockpit's HTTP/1.1 layer, on hyper: the accept loop and its connection limits, the
//! timeouts, the request as the routes see it, and the headers every answer carries. The routes
//! are in `api.rs`.

use std::convert::Infallible;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::{StatusCode, request::Parts};
use http_body_util::{BodyExt, Full, Limited, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{Sleep, timeout};

use super::{
    ACCEPT_BACKOFF, Admin, HEAD_TIMEOUT, LOOPBACK_PROBE_CONNECTIONS, MAX_CONNECTIONS, MAX_REQUEST,
    PROBE_CONNECTIONS, READ_TIMEOUT, WRITE_TIMEOUT,
};

/// The host part of a `host[:port]` authority (an IPv6 address keeps its brackets).
pub(super) fn host_name(authority: &str) -> &str {
    match authority.rsplit_once(':') {
        Some((h, p))
            if !p.is_empty()
                && p.bytes().all(|b| b.is_ascii_digit())
                && (!h.contains(':') || h.ends_with(']')) =>
        {
            h
        }
        _ => authority,
    }
}

/// True when `origin` (`scheme://host[:port]`) names the authority the request was sent to.
pub(super) fn same_origin(origin: &str, host: Option<&str>) -> bool {
    let authority = ["http://", "https://"].iter().find_map(|s| {
        origin
            .get(..s.len())
            .filter(|p| p.eq_ignore_ascii_case(s))
            .map(|_| &origin[s.len()..])
    });
    matches!((authority, host), (Some(a), Some(h)) if a.eq_ignore_ascii_case(h))
}

/// `Content-Type: application/json`, with or without parameters.
pub(super) fn is_json(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("application/json"))
}

/// The routes that need the token when one is set, and the browser checks when none is.
pub(super) const PROTECTED: [&str; 9] = [
    "/metrics",
    "/api/metrics",
    "/api/stream",
    "/api/events",
    "/api/snapshot",
    "/api/tables",
    "/api/routes",
    "/api/routes/history",
    "/api/kpis/history",
];

/// Constant-time string comparison (length is not secret here).
pub(super) fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// What the routes read from a request.
pub(super) struct Request {
    pub(super) method: String,
    pub(super) path: String,
    /// The `Authorization: Bearer` token. Never taken from the query.
    pub(super) bearer: Option<String>,
    pub(super) host: Option<String>,
    pub(super) origin: Option<String>,
    pub(super) content_type: Option<String>,
    pub(super) query: String,
    pub(super) body: String,
}

impl Request {
    /// The first of each header counts; a value that is not visible ASCII counts as absent.
    pub(super) fn new(parts: &Parts, body: String) -> Self {
        let first = |name: HeaderName| {
            parts
                .headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        Self {
            method: parts.method.as_str().to_owned(),
            path: parts.uri.path().to_owned(),
            bearer: first(header::AUTHORIZATION)
                .and_then(|v| v.strip_prefix("Bearer ").map(str::to_owned)),
            host: first(header::HOST),
            origin: first(header::ORIGIN),
            content_type: first(header::CONTENT_TYPE),
            query: parts.uri.query().unwrap_or_default().to_owned(),
            body,
        }
    }
}

pub(super) type Body = BoxBody<Bytes, Infallible>;
pub(super) type Response = http::Response<Body>;

/// An answer with `body`, and the headers every answer carries.
pub(super) fn reply(status: StatusCode, ctype: &'static str, body: impl Into<Bytes>) -> Response {
    with_body(status, ctype, Full::new(body.into()).boxed())
}

/// Like [`reply`], for a body that is produced as it is sent.
pub(super) fn with_body(status: StatusCode, ctype: &'static str, body: Body) -> Response {
    let mut r = http::Response::new(body);
    *r.status_mut() = status;
    secure(r.headers_mut(), ctype);
    r
}

fn secure(h: &mut HeaderMap, ctype: &'static str) {
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ctype));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}

/// The connection is closed without an answer: the request was too large or too slow.
#[derive(Debug)]
struct Dropped;

impl std::fmt::Display for Dropped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("request too large or too slow")
    }
}

impl std::error::Error for Dropped {}

/// Reads the body (at most [`MAX_REQUEST`], within [`READ_TIMEOUT`]) and hands the request to
/// the routes. An oversized or stalled request gets no answer: the connection is closed.
async fn answer(
    admin: Arc<Admin>,
    req: hyper::Request<Incoming>,
    probes_only: bool,
) -> Result<Response, Dropped> {
    let (parts, body) = req.into_parts();
    let declared = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    if declared.is_some_and(|n| n > MAX_REQUEST as u64) {
        return Err(Dropped);
    }
    let body = match timeout(READ_TIMEOUT, Limited::new(body, MAX_REQUEST).collect()).await {
        Ok(Ok(b)) => b.to_bytes(),
        _ => return Err(Dropped),
    };
    let req = Request::new(&parts, String::from_utf8_lossy(&body).into_owned());
    Ok(super::api::route(&admin, req, probes_only).await)
}

/// Serves one connection: one request, then it closes. `probes_only`: it holds a slot of the
/// probe reserve, so it answers only `/healthz` and `/readyz`.
async fn connection(admin: Arc<Admin>, sock: TcpStream, probes_only: bool) {
    let svc = service_fn(move |req| answer(admin.clone(), req, probes_only));
    let io = TokioIo::new(WriteDeadline::new(sock, WRITE_TIMEOUT));
    let served = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEAD_TIMEOUT)
        .max_buf_size(MAX_REQUEST)
        .keep_alive(false)
        .serve_connection(io, svc)
        .await;
    if let Err(e) = served {
        tracing::debug!(error = %e, "admin connection closed");
    }
}

pub async fn serve(listener: TcpListener, admin: Arc<Admin>) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let probes = Arc::new(Semaphore::new(
        PROBE_CONNECTIONS - LOOPBACK_PROBE_CONNECTIONS,
    ));
    let loopback_probes = Arc::new(Semaphore::new(LOOPBACK_PROBE_CONNECTIONS));
    // Logged once per run of failures, not every retry.
    let mut failing = false;
    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                if !std::mem::replace(&mut failing, true) {
                    tracing::warn!(error = %e, "admin server: accept failed; retrying");
                }
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        failing = false;
        // Shed load instead of queueing: a flood cannot exhaust file descriptors or memory. Once
        // the slots are full, a small reserve still answers the health probes.
        let (permit, probes_only) = match slots.clone().try_acquire_owned() {
            Ok(p) => (p, false),
            Err(_) => {
                let reserve = peer
                    .ip()
                    .is_loopback()
                    .then(|| loopback_probes.clone().try_acquire_owned().ok())
                    .flatten()
                    .or_else(|| probes.clone().try_acquire_owned().ok());
                let Some(p) = reserve else {
                    drop(sock);
                    continue;
                };
                (p, true)
            }
        };
        let admin = admin.clone();
        tokio::spawn(async move {
            connection(admin, sock, probes_only).await;
            drop(permit);
        });
    }
}

/// A socket whose writes fail once one has made no progress for `limit`. hyper has no write
/// timeout, and a client that stops reading (a stream behind a stalled proxy) would otherwise
/// keep its connection slot forever.
struct WriteDeadline {
    sock: TcpStream,
    limit: Duration,
    /// Armed while a write is pending, cleared when one makes progress.
    stalled: Option<Pin<Box<Sleep>>>,
}

impl WriteDeadline {
    fn new(sock: TcpStream, limit: Duration) -> Self {
        Self {
            sock,
            limit,
            stalled: None,
        }
    }

    fn deadline<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if polled.is_ready() {
            self.stalled = None;
            return polled;
        }
        let limit = self.limit;
        let sleep = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        match sleep.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::ErrorKind::TimedOut.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for WriteDeadline {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().sock).poll_read(cx, buf)
    }
}

impl AsyncWrite for WriteDeadline {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.sock).poll_write(cx, buf);
        this.deadline(cx, polled)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.sock).poll_write_vectored(cx, bufs);
        this.deadline(cx, polled)
    }

    fn is_write_vectored(&self) -> bool {
        self.sock.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.sock).poll_flush(cx);
        this.deadline(cx, polled)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.sock).poll_shutdown(cx);
        this.deadline(cx, polled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// A peer that never reads: once the socket buffers are full the write stalls, and the
    /// deadline turns the stall into an error instead of a connection held forever.
    #[tokio::test]
    async fn a_write_that_makes_no_progress_fails_after_the_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (sock, _) = listener.accept().await.unwrap();
        let mut w = WriteDeadline::new(sock, Duration::from_millis(200));
        let chunk = vec![0u8; 64 * 1024];
        let began = std::time::Instant::now();
        let err = loop {
            if let Err(e) = w.write_all(&chunk).await {
                break e;
            }
            assert!(began.elapsed() < Duration::from_secs(10), "never stalled");
        };
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        drop(peer);
    }
}
