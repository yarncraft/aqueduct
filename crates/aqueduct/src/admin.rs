//! Admin cockpit: a tiny HTTP server (no framework) on `health_addr`.
//!
//! - `GET /`            dashboard (single embedded HTML page)
//! - `GET /healthz`     `200 ok` (container HEALTHCHECK)
//! - `GET /api/metrics` JSON snapshot
//! - `GET /api/stream`  Server-Sent Events, one snapshot per second
//!
//! If `admin_token` is configured, the `/api/*` routes require it as
//! `Authorization: Bearer <token>` or `?token=<token>`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aqueduct_core::Metrics;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DASHBOARD: &str = include_str!("admin.html");
const HISTORY_LEN: usize = 120;

pub struct Admin {
    metrics: Arc<Metrics>,
    token: Option<String>,
    /// Events/second, one sample per second, oldest first.
    history: Mutex<VecDeque<f64>>,
}

impl Admin {
    pub fn new(metrics: Arc<Metrics>, token: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            metrics,
            token: token.filter(|t| !t.is_empty()),
            history: Mutex::new(VecDeque::with_capacity(HISTORY_LEN)),
        })
    }

    /// Sample the throughput once a second.
    pub async fn sample_forever(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last = self.metrics.events_total();
        loop {
            tick.tick().await;
            let now = self.metrics.events_total();
            let mut h = self.history.lock().unwrap();
            if h.len() == HISTORY_LEN {
                h.pop_front();
            }
            h.push_back(now.saturating_sub(last) as f64);
            last = now;
        }
    }

    fn payload(&self) -> String {
        let history: Vec<f64> = self.history.lock().unwrap().iter().copied().collect();
        let rate = history.last().copied().unwrap_or(0.0);
        let mut v = serde_json::to_value(self.metrics.snapshot()).unwrap_or_default();
        v["events_per_sec"] = json!(rate);
        v["history"] = json!(history);
        v.to_string()
    }

    fn authorized(&self, req: &Request) -> bool {
        match &self.token {
            None => true,
            Some(t) => req.bearer.as_deref() == Some(t) || req.query_token.as_deref() == Some(t),
        }
    }
}

struct Request {
    path: String,
    bearer: Option<String>,
    query_token: Option<String>,
}

fn parse(raw: &str) -> Option<Request> {
    let mut lines = raw.lines();
    let mut first = lines.next()?.split_whitespace();
    if first.next()? != "GET" {
        return None;
    }
    let target = first.next()?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query_token = query
        .split('&')
        .find_map(|kv| kv.strip_prefix("token="))
        .map(str::to_string);
    let bearer = lines.find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("authorization")
            .then(|| v.trim().strip_prefix("Bearer ").map(str::to_string))
            .flatten()
    });
    Some(Request {
        path: path.to_string(),
        bearer,
        query_token,
    })
}

async fn respond(sock: &mut TcpStream, status: &str, ctype: &str, body: &str) {
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = sock.write_all(head.as_bytes()).await;
    let _ = sock.write_all(body.as_bytes()).await;
}

async fn handle(admin: Arc<Admin>, mut sock: TcpStream) {
    let mut buf = [0u8; 2048];
    let n = match sock.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let Some(req) = parse(&String::from_utf8_lossy(&buf[..n])) else {
        return respond(
            &mut sock,
            "405 Method Not Allowed",
            "text/plain",
            "GET only",
        )
        .await;
    };
    match req.path.as_str() {
        "/" | "/index.html" => {
            respond(&mut sock, "200 OK", "text/html; charset=utf-8", DASHBOARD).await
        }
        "/healthz" => respond(&mut sock, "200 OK", "text/plain", "ok").await,
        "/api/metrics" | "/api/stream" if !admin.authorized(&req) => {
            respond(
                &mut sock,
                "401 Unauthorized",
                "text/plain",
                "token required",
            )
            .await
        }
        "/api/metrics" => respond(&mut sock, "200 OK", "application/json", &admin.payload()).await,
        "/api/stream" => {
            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store\r\nconnection: keep-alive\r\n\r\n";
            if sock.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let frame = format!("data: {}\n\n", admin.payload());
                if sock.write_all(frame.as_bytes()).await.is_err() {
                    return;
                }
            }
        }
        _ => respond(&mut sock, "404 Not Found", "text/plain", "not found").await,
    }
}

pub async fn serve(listener: TcpListener, admin: Arc<Admin>) {
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(handle(admin.clone(), sock));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_token_from_query_and_header() {
        let r = parse("GET /api/stream?x=1&token=abc HTTP/1.1\r\nhost: a\r\n\r\n").unwrap();
        assert_eq!(
            (r.path.as_str(), r.query_token.as_deref()),
            ("/api/stream", Some("abc"))
        );
        let r = parse("GET /api/metrics HTTP/1.1\r\nAuthorization: Bearer s3\r\n\r\n").unwrap();
        assert_eq!(r.bearer.as_deref(), Some("s3"));
        assert!(parse("POST / HTTP/1.1\r\n\r\n").is_none());
    }

    #[test]
    fn token_gate() {
        let m = Arc::new(Metrics::new());
        let open = Admin::new(m.clone(), None);
        let locked = Admin::new(m, Some("k".into()));
        let anon = parse("GET /api/metrics HTTP/1.1\r\n\r\n").unwrap();
        let ok = parse("GET /api/metrics?token=k HTTP/1.1\r\n\r\n").unwrap();
        assert!(open.authorized(&anon));
        assert!(!locked.authorized(&anon));
        assert!(locked.authorized(&ok));
    }

    async fn get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn serves_dashboard_health_and_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let admin = Admin::new(Arc::new(Metrics::new()), Some("k".into()));
        tokio::spawn(serve(listener, admin));
        assert!(get(addr, "/").await.contains("Aqueduct cockpit"));
        assert!(get(addr, "/healthz").await.ends_with("ok"));
        assert!(get(addr, "/api/metrics").await.starts_with("HTTP/1.1 401"));
        let ok = get(addr, "/api/metrics?token=k").await;
        assert!(ok.starts_with("HTTP/1.1 200") && ok.contains("\"events_total\":0"));
    }
}
