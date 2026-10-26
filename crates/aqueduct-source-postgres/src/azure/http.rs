//! A small HTTP/1.1 client for the token endpoints, on hyper: one request per connection
//! (`Connection: close`), a size cap and a timeout. hyper does the framing (lengths, chunking,
//! interim responses) and refuses a header value with CR or LF, so neither the request nor a
//! response is assembled or parsed by hand here.
//!
//! HTTPS uses the shared rustls config with `verify-full` and the bundled Mozilla roots. Plain
//! HTTP is accepted only for a loopback host (the Container Apps identity endpoint, or a test
//! server), so a credential never crosses the network unencrypted.

use std::net::SocketAddr;
use std::pin::pin;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Body;
use hyper::header::{self, HeaderName, HeaderValue};
use hyper::{Method, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;

/// Largest response accepted: the body, and separately the buffer the head must fit in. A token
/// response is a few KiB.
pub(crate) const MAX_RESPONSE: usize = 64 * 1024;

/// Connect, send and receive must all finish within this. It also bounds a server that sends
/// interim (1xx) responses without end.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(30);

/// An `http://` or `https://` URL, split for the request line and the `Host` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Url {
    pub https: bool,
    /// Without brackets for an IPv6 literal.
    pub host: String,
    pub port: u16,
    /// Path and query, starting with `/`.
    pub target: String,
}

impl Url {
    /// Parses `scheme://host[:port][/path][?query]`. `http://` is refused unless the host is
    /// loopback. User info and fragments are refused.
    pub fn parse(s: &str) -> Result<Self, String> {
        let (https, rest) = if let Some(r) = s.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = s.strip_prefix("http://") {
            (false, r)
        } else {
            return Err("the URL must start with https:// (or http:// for a loopback host)".into());
        };
        if s.contains('#') || s.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err("the URL must not contain a fragment, spaces or control characters".into());
        }
        let (authority, target) = match rest.find(['/', '?']) {
            Some(i) if rest.as_bytes()[i] == b'?' => (&rest[..i], format!("/{}", &rest[i..])),
            Some(i) => (&rest[..i], rest[i..].to_string()),
            None => (rest, "/".to_string()),
        };
        if authority.contains('@') {
            return Err("the URL must not contain user info".into());
        }
        let (host, port) = if let Some(r) = authority.strip_prefix('[') {
            let (h, after) = r
                .split_once(']')
                .ok_or("the URL has an unterminated IPv6 address")?;
            let port = match after {
                "" => None,
                p => Some(p.strip_prefix(':').ok_or("the URL has a malformed port")?),
            };
            (h, port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, Some(p)),
                None => (authority, None),
            }
        };
        if host.is_empty() {
            return Err("the URL has no host".into());
        }
        let port = match port {
            Some(p) => p.parse().map_err(|_| "the URL has a malformed port")?,
            None if https => 443,
            None => 80,
        };
        let url = Url {
            https,
            host: host.to_string(),
            port,
            target,
        };
        if !https && !url.is_loopback() {
            return Err(
                "plain http:// is only allowed for a loopback host; use https:// so the \
                 credential is not sent unencrypted"
                    .into(),
            );
        }
        Ok(url)
    }

    pub fn is_loopback(&self) -> bool {
        self.host.eq_ignore_ascii_case("localhost")
            || self
                .host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    }

    fn host_header(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        match (self.https, self.port) {
            (true, 443) | (false, 80) => host,
            (_, p) => format!("{host}:{p}"),
        }
    }

    /// Appends `query` (already encoded) to the target.
    pub fn with_query(&self, query: &str) -> Url {
        let sep = if self.target.contains('?') { '&' } else { '?' };
        Url {
            target: format!("{}{sep}{query}", self.target),
            ..self.clone()
        }
    }
}

/// Percent-encodes everything but the RFC 3986 unreserved characters.
pub(crate) fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `k=v&k=v`, each part percent-encoded: a query string or an
/// `application/x-www-form-urlencoded` body.
pub(crate) fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// One request. Not `Debug`: headers and body carry credentials.
pub(crate) struct Request {
    pub method: &'static str,
    pub url: Url,
    pub headers: Vec<(&'static str, String)>,
    /// Content type and body.
    pub body: Option<(&'static str, String)>,
}

/// A header value, refused if it holds CR, LF or another control character.
fn value(v: &str) -> Result<HeaderValue, String> {
    HeaderValue::from_str(v).map_err(|_| "a header value holds a control character".to_string())
}

impl Request {
    /// The request as hyper sends it. The caller's headers and the body carry credentials, so
    /// each extra header is marked sensitive.
    fn to_http(&self) -> Result<hyper::Request<Full<Bytes>>, String> {
        let body = self.body.as_ref().map_or("", |(_, b)| b.as_str());
        let mut r = hyper::Request::new(Full::new(Bytes::copy_from_slice(body.as_bytes())));
        *r.method_mut() =
            Method::from_bytes(self.method.as_bytes()).map_err(|_| "invalid HTTP method")?;
        *r.uri_mut() = self
            .url
            .target
            .parse()
            .map_err(|_| "the URL path is not a valid request target")?;
        let h = r.headers_mut();
        h.insert(header::HOST, value(&self.url.host_header())?);
        h.insert(header::CONNECTION, HeaderValue::from_static("close"));
        h.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        for (k, v) in &self.headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|_| "invalid header name")?;
            let mut v = value(v)?;
            v.set_sensitive(true);
            h.append(name, v);
        }
        if let Some((ct, body)) = &self.body {
            h.insert(header::CONTENT_TYPE, value(ct)?);
            h.insert(header::CONTENT_LENGTH, HeaderValue::from(body.len()));
        }
        Ok(r)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Sends `req` and reads the response, within [`TIMEOUT`].
pub(crate) async fn send(req: &Request) -> Result<Response, String> {
    tokio::time::timeout(TIMEOUT, send_inner(req))
        .await
        .map_err(|_| format!("no response within {}s", TIMEOUT.as_secs()))?
}

/// The addresses of a plain-HTTP host, which must all be loopback: a name is trusted only for
/// what it resolves to, so plain HTTP never reaches another machine.
async fn loopback_addrs(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("resolving {host}: {e}"))?
        .collect();
    check_loopback(host, &addrs)?;
    Ok(addrs)
}

fn check_loopback(host: &str, addrs: &[SocketAddr]) -> Result<(), String> {
    if addrs.is_empty() || addrs.iter().any(|a| !a.ip().is_loopback()) {
        return Err(format!(
            "{host} does not resolve to loopback addresses only; plain http:// is only allowed \
             for a loopback host"
        ));
    }
    Ok(())
}
async fn send_inner(req: &Request) -> Result<Response, String> {
    // Build it first: a malformed header fails before anything is sent.
    let request = req.to_http()?;
    let url = &req.url;
    let connected = if url.https {
        TcpStream::connect((url.host.as_str(), url.port)).await
    } else {
        // Connect to the checked addresses, not to the name again.
        TcpStream::connect(&loopback_addrs(&url.host, url.port).await?[..]).await
    };
    let tcp = connected.map_err(|e| format!("connecting to {}:{}: {e}", url.host, url.port))?;
    tcp.set_nodelay(true).ok();
    if url.https {
        let config = aqueduct_tls::client_config(None, aqueduct_tls::Verify::Full)?;
        let name = ServerName::try_from(url.host.clone())
            .map_err(|e| format!("invalid TLS server name {:?}: {e}", url.host))?;
        let tls = TlsConnector::from(config)
            .connect(name, tcp)
            .await
            .map_err(|e| format!("TLS handshake with {}: {e}", url.host))?;
        exchange(tls, request).await
    } else {
        exchange(tcp, request).await
    }
}

/// Sends `request` over `io` and reads the response. The connection is driven here rather than
/// spawned, so it ends with the call (and with the [`TIMEOUT`] around it).
async fn exchange<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
    io: S,
    request: hyper::Request<Full<Bytes>>,
) -> Result<Response, String> {
    let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(MAX_RESPONSE)
        .handshake(TokioIo::new(io))
        .await
        .map_err(|e| format!("sending the request: {e}"))?;
    let reply = async {
        let resp = sender
            .send_request(request)
            .await
            .map_err(|e| format!("reading the response: {e}"))?;
        let status = resp.status();
        if status == StatusCode::SWITCHING_PROTOCOLS {
            return Err("the server switched protocols (HTTP 101)".to_string());
        }
        let too_large = || format!("the response is larger than {MAX_RESPONSE} bytes");
        if resp.body().size_hint().lower() > MAX_RESPONSE as u64 {
            return Err(too_large());
        }
        let body = Limited::new(resp.into_body(), MAX_RESPONSE)
            .collect()
            .await
            .map_err(|e| {
                if e.is::<LengthLimitError>() {
                    too_large()
                } else {
                    format!("reading the response: {e}")
                }
            })?
            .to_bytes();
        Ok(Response {
            status: status.as_u16(),
            body: body.to_vec(),
        })
    };
    let (mut reply, mut conn) = (pin!(reply), pin!(conn));
    let mut closed = false;
    loop {
        tokio::select! {
            r = &mut reply => return r,
            // Once the connection ends, a reply still waiting on it fails with the reason.
            _ = &mut conn, if !closed => closed = true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn parses_urls() {
        let u = Url::parse("https://login.microsoftonline.com/").unwrap();
        assert_eq!(
            (u.https, u.host.as_str(), u.port, u.target.as_str()),
            (true, "login.microsoftonline.com", 443, "/")
        );
        let u = Url::parse("http://localhost:42356/msi/token?x=1").unwrap();
        assert_eq!((u.port, u.target.as_str()), (42356, "/msi/token?x=1"));
        let u = Url::parse("http://127.0.0.1:8080?a=b").unwrap();
        assert_eq!(u.target, "/?a=b");
        let u = Url::parse("http://[::1]:9/t").unwrap();
        assert_eq!((u.host.as_str(), u.port), ("::1", 9));
        assert_eq!(u.host_header(), "[::1]:9");
        assert_eq!(
            Url::parse("https://h.example").unwrap().host_header(),
            "h.example"
        );
        assert_eq!(
            Url::parse("https://h.example:8443/").unwrap().host_header(),
            "h.example:8443"
        );
    }

    #[test]
    fn plain_http_only_to_loopback() {
        for bad in [
            "http://login.microsoftonline.com/",
            "http://10.0.0.5/",
            "http://169.254.169.254/metadata",
        ] {
            assert!(Url::parse(bad).unwrap_err().contains("loopback"), "{bad}");
        }
        for bad in [
            "ftp://x/",
            "login.microsoftonline.com",
            "https://u:p@h/",
            "https://h/#f",
            "https:///x",
            "https://h:port/",
            "https://h/a b",
        ] {
            assert!(Url::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn query_is_appended() {
        let u = Url::parse("http://localhost/t").unwrap();
        assert_eq!(u.with_query("a=1").target, "/t?a=1");
        assert_eq!(u.with_query("a=1").with_query("b=2").target, "/t?a=1&b=2");
    }

    #[test]
    fn form_encoding() {
        assert_eq!(encode("aZ09-._~"), "aZ09-._~");
        assert_eq!(
            encode("https://ossrdbms-aad.database.windows.net/.default"),
            "https%3A%2F%2Fossrdbms-aad.database.windows.net%2F.default"
        );
        assert_eq!(encode("a b+c&d=é"), "a%20b%2Bc%26d%3D%C3%A9");
        assert_eq!(form(&[("a", "1"), ("b c", "x&y")]), "a=1&b%20c=x%26y");
    }

    /// Answers one request on an in-memory pipe with `reply`, then hangs up. Returns the outcome
    /// and what the client sent.
    async fn answer(
        req: &Request,
        reply: impl Into<Vec<u8>>,
    ) -> (Result<Response, String>, String) {
        let reply = reply.into();
        let (mut server, client) = tokio::io::duplex(1 << 20);
        let peer = tokio::spawn(async move {
            let mut got = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let text = String::from_utf8_lossy(&got).to_string();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().to_string())
                        })
                        .map_or(0, |v| v.parse::<usize>().unwrap());
                    if body.len() >= length {
                        break;
                    }
                }
                match server.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => got.extend_from_slice(&chunk[..n]),
                }
            }
            let _ = server.write_all(&reply).await;
            String::from_utf8(got).unwrap()
        });
        let r = exchange(client, req.to_http().unwrap()).await;
        (r, peer.await.unwrap())
    }

    fn get() -> Request {
        Request {
            method: "GET",
            url: Url::parse("http://localhost/").unwrap(),
            headers: vec![],
            body: None,
        }
    }

    #[tokio::test]
    async fn request_on_the_wire() {
        let r = Request {
            method: "POST",
            url: Url::parse("https://h.example/t/oauth2").unwrap(),
            headers: vec![("X-A", "b".into())],
            body: Some(("application/x-www-form-urlencoded", "a=1".into())),
        };
        let (resp, sent) = answer(&r, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await;
        assert_eq!(resp.unwrap().body, b"{}");
        let sent = sent.to_ascii_lowercase();
        assert!(sent.starts_with("post /t/oauth2 http/1.1\r\n"), "{sent}");
        for line in [
            "host: h.example",
            "connection: close",
            "accept: application/json",
            "x-a: b",
            "content-type: application/x-www-form-urlencoded",
            "content-length: 3",
        ] {
            assert!(
                sent.contains(&format!("\r\n{line}\r\n")),
                "{line} in {sent}"
            );
        }
        assert!(sent.ends_with("\r\n\r\na=1"), "{sent}");
    }

    #[test]
    fn header_injection_is_refused_before_sending() {
        let mut r = get();
        r.headers = vec![("X-IDENTITY-HEADER", "s\r\nX-Evil: 1".into())];
        assert!(r.to_http().unwrap_err().contains("control character"));
        r.headers = vec![("Bad Name", "v".into())];
        assert!(r.to_http().is_err());
    }

    #[test]
    fn credential_headers_are_sensitive() {
        let mut r = get();
        r.headers = vec![("X-IDENTITY-HEADER", "secret".into())];
        let h = r.to_http().unwrap();
        assert!(h.headers()["x-identity-header"].is_sensitive());
        assert!(!format!("{:?}", h.headers()).contains("secret"));
    }

    #[tokio::test]
    async fn content_length_and_chunked_responses() {
        let (r, _) = answer(
            &get(),
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX: y\r\n\r\nhello",
        )
        .await;
        assert_eq!(
            r.unwrap(),
            Response {
                status: 200,
                body: b"hello".to_vec()
            }
        );
        let (r, _) = answer(
            &get(),
            "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n\
             4;ext=1\r\nwiki\r\n5\r\npedia\r\n0\r\n\r\n",
        )
        .await;
        let r = r.unwrap();
        assert_eq!(
            (r.status, r.body.as_slice()),
            (400, b"wikipedia".as_slice())
        );
    }

    #[tokio::test]
    async fn body_until_close_and_interim_responses() {
        let (r, _) = answer(&get(), "HTTP/1.0 200 OK\r\n\r\n{}").await;
        assert_eq!(r.unwrap().body, b"{}");
        let (r, _) = answer(
            &get(),
            "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert_eq!(r.unwrap().status, 204);
    }

    #[tokio::test]
    async fn malformed_or_truncated_responses_are_errors() {
        for bad in [
            "SSH-2.0\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhell",
            "HTTP/1.1 200 OK\r\n",
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nwi",
            "HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\nok",
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nokk",
            "HTTP/1.1 +200 OK\r\nContent-Length: 0\r\n\r\n",
        ] {
            let (r, _) = answer(&get(), bad).await;
            assert!(r.is_err(), "{bad:?} gave {r:?}");
        }
    }

    #[tokio::test]
    async fn switching_protocols_is_refused() {
        let (r, _) = answer(
            &get(),
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\n\r\n",
        )
        .await;
        assert!(r.unwrap_err().contains("101"));
    }

    #[test]
    fn plain_http_needs_every_resolved_address_to_be_loopback() {
        let lo: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let lo6: SocketAddr = "[::1]:80".parse().unwrap();
        let remote: SocketAddr = "10.0.0.5:80".parse().unwrap();
        assert!(check_loopback("localhost", &[lo, lo6]).is_ok());
        assert!(check_loopback("localhost", &[lo, remote]).is_err());
        assert!(check_loopback("localhost", &[]).is_err());
    }

    #[tokio::test]
    async fn localhost_resolves_to_loopback() {
        let addrs = loopback_addrs("localhost", 80).await.unwrap();
        assert!(addrs.iter().all(|a| a.ip().is_loopback()));
    }

    #[tokio::test]
    async fn oversized_responses_are_refused() {
        let big = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_RESPONSE + 1
        );
        let (r, _) = answer(&get(), big).await;
        assert!(r.unwrap_err().contains("larger than"));
        let chunk = format!("{:x}\r\n{}\r\n", 1000, "a".repeat(1000));
        let stream = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{}0\r\n\r\n",
            chunk.repeat(100)
        );
        let (r, _) = answer(&get(), stream).await;
        assert!(r.unwrap_err().contains("larger than"));
        // A head that does not fit the buffer.
        let head = format!(
            "HTTP/1.1 200 OK\r\nX: {}\r\n\r\n",
            "a".repeat(4 * MAX_RESPONSE)
        );
        let (r, _) = answer(&get(), head).await;
        assert!(r.is_err(), "{r:?}");
    }

    #[tokio::test]
    async fn sends_over_loopback() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
        });
        let mut r = get();
        r.url = Url::parse(&format!("http://127.0.0.1:{port}/msi/token")).unwrap();
        assert_eq!(send(&r).await.unwrap().body, b"ok");
    }
}
