//! The cockpit's routes: what each path answers, behind the token and browser checks.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use aqueduct_core::{SnapshotCommand, SnapshotRequest, State};
use bytes::Bytes;
use http::StatusCode;
use http::header::{self, HeaderValue};
use http_body_util::BodyExt;
use hyper::body::Frame;
use serde_json::json;
use tokio::time::{Interval, timeout};

use super::server::{PROTECTED, Request, Response, reply, with_body};
use super::{Admin, DASHBOARD, MAX_STREAMS, SAMPLE_MS, SCRIPT, STYLES, lock};

/// The dashboard only needs same-origin resources plus inline styles (bar widths and colours are
/// set from data). No inline script: the page loads `/admin.js`.
pub(super) const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
                   connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; \
                   frame-ancestors 'none'";

/// How long `/api/snapshot` waits for the source, which answers between batches.
const SNAPSHOT_REPLY: Duration = Duration::from_secs(30);

const NOT_DURABLE: &str = "the request survives a restart only once the first chunk is \
    checkpointed; if Aqueduct restarts before that, send it again";

/// `GET` the running or last snapshot, `POST {"tables": ["schema.table", ...]}` to start one
/// (or add tables to the running one), `DELETE` to cancel after the chunk in flight.
pub(super) async fn snapshot_api(admin: &Admin, req: &Request) -> (StatusCode, String) {
    let err = |status, msg: &str| (status, json!({ "error": msg }).to_string());
    if req.method == "GET" {
        let body = match admin.metrics.snapshot().snapshot {
            Some(st) => serde_json::to_string(&st).unwrap_or_default(),
            None => json!({ "state": "idle" }).to_string(),
        };
        return (StatusCode::OK, body);
    }
    let Some(tx) = admin.snapshots.get() else {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "this source does not support snapshots",
        );
    };
    let command = match req.method.as_str() {
        "POST" => {
            let tables = serde_json::from_str::<serde_json::Value>(&req.body)
                .ok()
                .and_then(|v| {
                    v.get("tables")?
                        .as_array()?
                        .iter()
                        .map(|t| t.as_str().map(str::to_owned))
                        .collect::<Option<Vec<String>>>()
                });
            match tables {
                Some(t) if !t.is_empty() && t.iter().all(|t| t.contains('.')) => {
                    SnapshotCommand::Start(t)
                }
                _ => {
                    return err(
                        StatusCode::BAD_REQUEST,
                        r#"expected {"tables": ["schema.table", ...]}"#,
                    );
                }
            }
        }
        _ => SnapshotCommand::Cancel,
    };
    let start = matches!(command, SnapshotCommand::Start(_));
    let (reply, answer) = tokio::sync::oneshot::channel();
    if tx.try_send(SnapshotRequest { command, reply }).is_err() {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "the source is not taking requests",
        );
    }
    match timeout(SNAPSHOT_REPLY, answer).await {
        Ok(Ok(Ok(id))) if start => (
            StatusCode::ACCEPTED,
            json!({ "id": id, "state": "running", "durable": false, "note": NOT_DURABLE })
                .to_string(),
        ),
        Ok(Ok(Ok(id))) => (
            StatusCode::ACCEPTED,
            json!({ "id": id, "state": "cancelling" }).to_string(),
        ),
        Ok(Ok(Err(msg))) => err(StatusCode::CONFLICT, &msg),
        Ok(Err(_)) => err(StatusCode::SERVICE_UNAVAILABLE, "the source stopped"),
        Err(_) => err(
            StatusCode::GATEWAY_TIMEOUT,
            "the source did not answer in time",
        ),
    }
}

/// Answers one request. `probes_only`: the connection holds a slot of the probe reserve, so only
/// `/healthz` and `/readyz` are served.
pub(super) async fn route(admin: &Arc<Admin>, req: Request, probes_only: bool) -> Response {
    const TEXT: &str = "text/plain";
    const JSON: &str = "application/json";
    if !matches!(req.method.as_str(), "GET" | "POST" | "DELETE") {
        return reply(
            StatusCode::METHOD_NOT_ALLOWED,
            TEXT,
            "GET, POST or DELETE only",
        );
    }
    if probes_only && !(req.method == "GET" && matches!(req.path.as_str(), "/healthz" | "/readyz"))
    {
        return reply(
            StatusCode::SERVICE_UNAVAILABLE,
            TEXT,
            "too many connections",
        );
    }
    if req.method != "GET" && req.path != "/api/snapshot" {
        return reply(StatusCode::METHOD_NOT_ALLOWED, TEXT, "GET only");
    }
    if PROTECTED.contains(&req.path.as_str())
        && let Err((status, why)) = admin.browser_check(&req)
    {
        return reply(status, TEXT, why);
    }
    match req.path.as_str() {
        "/" | "/index.html" => {
            let mut r = reply(StatusCode::OK, "text/html; charset=utf-8", DASHBOARD);
            r.headers_mut().insert(
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static(CSP),
            );
            r
        }
        "/admin.js" => reply(StatusCode::OK, "text/javascript; charset=utf-8", SCRIPT),
        "/admin.css" => reply(StatusCode::OK, "text/css; charset=utf-8", STYLES),
        "/healthz" => reply(StatusCode::OK, TEXT, "ok"),
        "/readyz" => {
            if admin.metrics.snapshot().state == State::Streaming {
                reply(StatusCode::OK, TEXT, "ready")
            } else {
                reply(StatusCode::SERVICE_UNAVAILABLE, TEXT, "not streaming")
            }
        }
        path if PROTECTED.contains(&path) && !admin.authorized(&req) => {
            let mut r = reply(StatusCode::UNAUTHORIZED, TEXT, "token required");
            r.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            r
        }
        "/metrics" => reply(
            StatusCode::OK,
            "text/plain; version=0.0.4; charset=utf-8",
            admin.exposition(),
        ),
        "/api/metrics" => reply(StatusCode::OK, JSON, admin.payload()),
        "/api/tables" => reply(StatusCode::OK, JSON, admin.tables_payload().await),
        "/api/routes" => match admin.router.get() {
            Some(plan) => reply(
                StatusCode::OK,
                JSON,
                serde_json::to_string(&plan.describe()).unwrap_or_default(),
            ),
            None => reply(StatusCode::NOT_FOUND, JSON, r#"{"error":"no route plan"}"#),
        },
        "/api/routes/history" => reply(
            StatusCode::OK,
            JSON,
            lock(&admin.rates).window(SAMPLE_MS).to_string(),
        ),
        "/api/kpis/history" => reply(StatusCode::OK, JSON, lock(&admin.kpis).window().to_string()),
        "/api/snapshot" => {
            let (status, body) = snapshot_api(admin, &req).await;
            reply(status, JSON, body)
        }
        "/api/events" => {
            let Some(inspector) = &admin.inspector else {
                return reply(
                    StatusCode::FORBIDDEN,
                    JSON,
                    r#"{"error":"inspector disabled; set admin_token and inspector = true"}"#,
                );
            };
            let param = |k: &str| {
                req.query
                    .split('&')
                    .find_map(|kv| kv.strip_prefix(k)?.strip_prefix('='))
            };
            let topic = param("topic").filter(|t| !t.is_empty());
            let limit = param("limit")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(100);
            let (status, body) = admin.inspect_events(inspector, topic, limit).await;
            reply(status, JSON, body)
        }
        "/api/stream" => match Stream::open(admin) {
            Some(s) => with_body(StatusCode::OK, "text/event-stream", s.boxed()),
            None => reply(StatusCode::SERVICE_UNAVAILABLE, TEXT, "too many streams"),
        },
        _ => reply(StatusCode::NOT_FOUND, TEXT, "not found"),
    }
}

/// `/api/stream`: a `data:` frame with the full snapshot every second until the client goes.
/// Holds one of the [`MAX_STREAMS`] slots while it lives.
struct Stream {
    admin: Arc<Admin>,
    tick: Interval,
}

impl Stream {
    fn open(admin: &Arc<Admin>) -> Option<Self> {
        if admin.streams.fetch_add(1, Ordering::SeqCst) >= MAX_STREAMS {
            admin.streams.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Self {
            admin: admin.clone(),
            tick: tokio::time::interval(Duration::from_secs(1)),
        })
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.admin.streams.fetch_sub(1, Ordering::SeqCst);
    }
}

impl hyper::body::Body for Stream {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        ready!(self.tick.poll_tick(cx));
        let frame = format!("data: {}\n\n", self.admin.payload());
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(frame)))))
    }
}
