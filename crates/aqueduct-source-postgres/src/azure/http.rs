//! A small HTTP/1.1 client for the token endpoints: one request per connection
//! (`Connection: close`), `Content-Length` or chunked responses, a size cap and a timeout.
//!
//! HTTPS uses the shared rustls config with `verify-full` and the bundled Mozilla roots. Plain
//! HTTP is accepted only for a loopback host (the Container Apps identity endpoint, or a test
//! server), so a credential never crosses the network unencrypted.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;

/// Largest response accepted, headers included. A token response is a few KiB.
pub(crate) const MAX_RESPONSE: usize = 64 * 1024;

/// Connect, send and receive must all finish within this.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(30);

/// Interim (1xx) responses accepted before the final one.
const MAX_INTERIM: usize = 5;

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

impl Request {
    /// The bytes on the wire.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut head = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: application/json\r\n",
            self.method,
            self.url.target,
            self.url.host_header()
        );
        for (k, v) in &self.headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        match &self.body {
            Some((ct, body)) => {
                head.push_str(&format!(
                    "Content-Type: {ct}\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                ));
                head.push_str(body);
            }
            None => head.push_str("\r\n"),
        }
        head.into_bytes()
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
        exchange(tls, req).await
    } else {
        exchange(tcp, req).await
    }
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    mut io: S,
    req: &Request,
) -> Result<Response, String> {
    io.write_all(&req.to_bytes())
        .await
        .map_err(|e| format!("sending the request: {e}"))?;
    io.flush()
        .await
        .map_err(|e| format!("sending the request: {e}"))?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = match io.read(&mut chunk).await {
            Ok(n) => n,
            // A TLS peer that closes without close_notify: whatever arrived is judged below.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => 0,
            Err(e) => return Err(format!("reading the response: {e}")),
        };
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_RESPONSE {
            return Err(format!("the response is larger than {MAX_RESPONSE} bytes"));
        }
        if let Some(r) = parse_response(&buf, n == 0)? {
            return Ok(r);
        }
        if n == 0 {
            return Err("the connection closed before the response was complete".into());
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// A non-empty run of digits in `radix`, without the sign `from_str_radix` would accept.
fn number(v: &str, radix: u32) -> Option<usize> {
    if v.is_empty() || !v.chars().all(|c| c.is_digit(radix)) {
        return None;
    }
    usize::from_str_radix(v, radix).ok()
}

/// The response in `buf`, or `None` while more bytes are needed. `eof`: nothing more will come.
pub(crate) fn parse_response(mut buf: &[u8], eof: bool) -> Result<Option<Response>, String> {
    let mut interim = 0;
    let (end, status, head) = loop {
        let Some(end) = find(buf, b"\r\n\r\n") else {
            return if eof {
                Err("the connection closed inside the response headers".into())
            } else {
                Ok(None)
            };
        };
        let head = std::str::from_utf8(&buf[..end]).map_err(|_| "non-UTF-8 response headers")?;
        let status_line = head.split("\r\n").next().unwrap_or_default();
        let mut parts = status_line.splitn(3, ' ');
        let version = parts.next().unwrap_or_default();
        if !version.starts_with("HTTP/1.") {
            return Err("the server did not answer with HTTP/1.x".into());
        }
        let status: u16 = parts
            .next()
            .and_then(|s| number(s, 10))
            .and_then(|s| u16::try_from(s).ok())
            .ok_or("malformed HTTP status line")?;
        if status == 101 {
            return Err("the server switched protocols (HTTP 101)".into());
        }
        if !(100..200).contains(&status) {
            break (end, status, head);
        }
        // An interim response (100 Continue): the real one follows.
        interim += 1;
        if interim > MAX_INTERIM {
            return Err(format!("more than {MAX_INTERIM} interim (1xx) responses"));
        }
        buf = &buf[end + 4..];
    };
    let rest = &buf[end + 4..];
    let lines = head.split("\r\n").skip(1);
    let (mut chunked, mut length) = (false, None);
    for line in lines {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let v = v.trim();
        if k.eq_ignore_ascii_case("transfer-encoding") {
            chunked = v
                .rsplit(',')
                .next()
                .is_some_and(|t| t.trim().eq_ignore_ascii_case("chunked"));
        } else if k.eq_ignore_ascii_case("content-length") {
            let n = number(v, 10).ok_or("malformed Content-Length header")?;
            if length.is_some_and(|l| l != n) {
                return Err("conflicting Content-Length headers".into());
            }
            length = Some(n);
        }
    }
    let body = if chunked {
        match dechunk(rest)? {
            Some(b) => b,
            None if eof => return Err("the connection closed inside a chunked body".into()),
            None => return Ok(None),
        }
    } else if let Some(n) = length {
        if n > MAX_RESPONSE {
            return Err(format!("the response is larger than {MAX_RESPONSE} bytes"));
        }
        match rest.get(..n) {
            Some(b) => b.to_vec(),
            None if eof => return Err("the connection closed before Content-Length bytes".into()),
            None => return Ok(None),
        }
    } else if eof {
        rest.to_vec()
    } else {
        return Ok(None);
    };
    Ok(Some(Response { status, body }))
}

/// Decodes a chunked body; `None` while incomplete. Trailers are ignored.
fn dechunk(mut rest: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut out = Vec::new();
    loop {
        let Some(eol) = find(rest, b"\r\n") else {
            return Ok(None);
        };
        let line = std::str::from_utf8(&rest[..eol]).map_err(|_| "malformed chunk size")?;
        let size = line.split(';').next().unwrap_or_default().trim();
        let size = number(size, 16).ok_or("malformed chunk size")?;
        if size > MAX_RESPONSE {
            return Err(format!("the response is larger than {MAX_RESPONSE} bytes"));
        }
        rest = &rest[eol + 2..];
        if size == 0 {
            return Ok(Some(out));
        }
        if rest.len() < size + 2 {
            return Ok(None);
        }
        if &rest[size..size + 2] != b"\r\n" {
            return Err("malformed chunk".into());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn request_bytes() {
        let r = Request {
            method: "POST",
            url: Url::parse("https://h.example/t/oauth2").unwrap(),
            headers: vec![("X-A", "b".into())],
            body: Some(("application/x-www-form-urlencoded", "a=1".into())),
        };
        assert_eq!(
            String::from_utf8(r.to_bytes()).unwrap(),
            "POST /t/oauth2 HTTP/1.1\r\nHost: h.example\r\nConnection: close\r\n\
             Accept: application/json\r\nX-A: b\r\n\
             Content-Type: application/x-www-form-urlencoded\r\nContent-Length: 3\r\n\r\na=1"
        );
    }

    #[test]
    fn content_length_response() {
        let full = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX: y\r\n\r\nhello";
        assert_eq!(
            parse_response(full, false).unwrap(),
            Some(Response {
                status: 200,
                body: b"hello".to_vec()
            })
        );
        assert_eq!(
            parse_response(&full[..full.len() - 1], false).unwrap(),
            None
        );
        assert!(parse_response(&full[..full.len() - 1], true).is_err());
        assert_eq!(parse_response(b"HTTP/1.1 200 OK\r\n", false).unwrap(), None);
        assert!(parse_response(b"HTTP/1.1 200 OK\r\n", true).is_err());
    }

    #[test]
    fn chunked_response() {
        let full = b"HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n\
                     4;ext=1\r\nwiki\r\n5\r\npedia\r\n0\r\n\r\n";
        let r = parse_response(full, false).unwrap().unwrap();
        assert_eq!(
            (r.status, r.body.as_slice()),
            (400, b"wikipedia".as_slice())
        );
        assert_eq!(parse_response(&full[..60], false).unwrap(), None);
        assert!(parse_response(&full[..60], true).is_err());
        assert!(parse_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
            false
        )
        .is_err());
    }

    #[test]
    fn body_until_close_and_interim_responses() {
        let r = parse_response(b"HTTP/1.0 200 OK\r\n\r\n{}", true)
            .unwrap()
            .unwrap();
        assert_eq!(r.body, b"{}");
        assert_eq!(
            parse_response(b"HTTP/1.0 200 OK\r\n\r\n{}", false).unwrap(),
            None
        );
        let r = parse_response(
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(r.status, 204);
        assert!(parse_response(b"SSH-2.0\r\n\r\n", true).is_err());
    }

    #[test]
    fn interim_responses_are_capped_and_101_is_refused() {
        let five = "HTTP/1.1 100 Continue\r\n\r\n".repeat(5);
        let ok = format!("{five}HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        assert_eq!(
            parse_response(ok.as_bytes(), false).unwrap().unwrap().body,
            b"ok"
        );
        let six = format!("{five}HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\n\r\n");
        assert!(parse_response(six.as_bytes(), true)
            .unwrap_err()
            .contains("interim"));
        // A long run of interim heads fails cleanly instead of recursing.
        let many = "HTTP/1.1 100 Continue\r\n\r\n".repeat(5000);
        assert!(parse_response(many.as_bytes(), false).is_err());
        assert!(parse_response(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\n\r\n",
            false
        )
        .unwrap_err()
        .contains("101"));
    }

    #[test]
    fn signed_or_conflicting_lengths_are_refused() {
        for bad in [
            &b"HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\nok"[..],
            b"HTTP/1.1 200 OK\r\nContent-Length: -0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nokk",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2, 2\r\n\r\nok",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n+2\r\nok\r\n0\r\n\r\n",
            b"HTTP/1.1 +200 OK\r\nContent-Length: 0\r\n\r\n",
        ] {
            assert!(
                parse_response(bad, true).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
        // The same length twice is harmless.
        let r = parse_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nok",
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(r.body, b"ok");
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

    #[test]
    fn oversized_responses_are_refused() {
        let big = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_RESPONSE + 1
        );
        assert!(parse_response(big.as_bytes(), false).is_err());
    }

    #[tokio::test]
    async fn oversized_stream_is_cut_off() {
        let (mut server, client) = tokio::io::duplex(1 << 20);
        tokio::spawn(async move {
            let mut sink = [0u8; 1024];
            let _ = server.read(&mut sink).await;
            let _ = server
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await;
            let chunk = format!("{:x}\r\n{}\r\n", 1000, "a".repeat(1000));
            for _ in 0..100 {
                if server.write_all(chunk.as_bytes()).await.is_err() {
                    return;
                }
            }
        });
        let req = Request {
            method: "GET",
            url: Url::parse("http://localhost/").unwrap(),
            headers: vec![],
            body: None,
        };
        let e = exchange(client, &req).await.unwrap_err();
        assert!(e.contains("larger than"), "{e}");
    }
}
