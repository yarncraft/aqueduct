//! Opt-in OpenTelemetry export: the `/metrics` series and the logs, pushed over OTLP/HTTP
//! (protobuf) to a Collector. Traces are not exported.
//!
//! - **Off by default.** It turns on when an endpoint is set: `OTEL_EXPORTER_OTLP_ENDPOINT`
//!   (or the per-signal `OTEL_EXPORTER_OTLP_{METRICS,LOGS}_ENDPOINT`), else
//!   `[telemetry] otlp_endpoint`. The standard `OTEL_*` variables win over the TOML.
//! - **Metrics** are not counted twice: every collection renders the same text `/metrics` serves
//!   ([`crate::admin::Admin::exposition`]) and reports each sample as an observation. Names,
//!   labels (as attributes) and meaning stay the same; a Prometheus counter becomes a monotonic
//!   cumulative sum, a gauge a gauge.
//! - **Logs** go through `opentelemetry-appender-tracing`, a second layer next to the JSON one;
//!   stdout is unchanged.
//! - **Never in the way.** The SDK exports from its own threads; log records go into a bounded
//!   queue that drops when full; every request has a timeout. A failing collector is logged once
//!   (and once more when it answers again); the data in between is dropped.
//! - Transport: hyper's HTTP/1.1 client on a one-worker tokio runtime of its own, with TLS from
//!   `aqueduct-tls` (rustls, ring) for `https://` endpoints; no gRPC stack comes in. `grpc` is
//!   refused with a clear error; the Collector's OTLP/HTTP receiver listens on 4318.

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{self, HeaderValue};
use hyper_util::rt::TokioIo;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{Protocol, WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_rustls::TlsConnector;

use crate::config::TelemetryConfig;

/// The only protocol spoken (OTLP/HTTP with protobuf bodies).
const HTTP_PROTOBUF: &str = "http/protobuf";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SERVICE_NAME: &str = "aqueduct";
/// A collector's reply is a small protobuf message; anything bigger is refused.
const MAX_RESPONSE: usize = 4 * 1024 * 1024;
/// One collection renders the exposition once, however many instruments it reads.
const EXPOSITION_TTL: Duration = Duration::from_millis(500);
/// How often new series families (a snapshot starting, say) get an instrument.
const DISCOVER_EVERY: Duration = Duration::from_secs(10);
/// Longest a clean shutdown waits for the last export.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(3);

/// What [`resolve`] decided: where each signal goes, if anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Full URL of the metrics endpoint (`…/v1/metrics`), or `None` for no metrics.
    pub metrics_endpoint: Option<String>,
    /// Full URL of the logs endpoint (`…/v1/logs`), or `None` for no logs.
    pub logs_endpoint: Option<String>,
    /// `service.name` to set, or `None` when `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES`
    /// already name it (the SDK reads those itself).
    pub service_name: Option<String>,
    pub interval: Duration,
    pub timeout: Duration,
    /// PEM CA bundle for an `https://` endpoint (`OTEL_EXPORTER_OTLP_CERTIFICATE`).
    pub ca_file: Option<String>,
}

/// Works out the settings from `[telemetry]` and the `OTEL_*` variables (read through `env`,
/// which returns `None` for unset or empty). `Ok(None)` means export stays off.
pub fn resolve(
    cfg: &TelemetryConfig,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<Settings>, String> {
    if env("OTEL_SDK_DISABLED").is_some_and(|v| v.trim().eq_ignore_ascii_case("true")) {
        return Ok(None);
    }
    let base = env("OTEL_EXPORTER_OTLP_ENDPOINT").or_else(|| cfg.otlp_endpoint.clone());
    let signal = |on: bool, exporter_var: &str, endpoint_var: &str, path: &str| {
        let off = env(exporter_var).is_some_and(|v| v.trim().eq_ignore_ascii_case("none"));
        if !on || off {
            return None;
        }
        // A per-signal URL is used as given; the base URL gets the signal's path (OTLP spec).
        env(endpoint_var).or_else(|| {
            base.as_deref()
                .map(|b| format!("{}/{path}", b.trim_end_matches('/')))
        })
    };
    let metrics_endpoint = signal(
        cfg.metrics,
        "OTEL_METRICS_EXPORTER",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "v1/metrics",
    );
    let logs_endpoint = signal(
        cfg.logs,
        "OTEL_LOGS_EXPORTER",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "v1/logs",
    );
    if metrics_endpoint.is_none() && logs_endpoint.is_none() {
        return Ok(None);
    }
    for var in [
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
    ] {
        let protocol = env(var).or_else(|| {
            (var == "OTEL_EXPORTER_OTLP_PROTOCOL")
                .then(|| cfg.otlp_protocol.clone())
                .flatten()
        });
        if let Some(p) = protocol.filter(|p| p.trim() != HTTP_PROTOBUF) {
            return Err(format!(
                "OTLP protocol {p:?} ({var} or telemetry.otlp_protocol) is not supported; use \
                 {HTTP_PROTOBUF} (the Collector's OTLP/HTTP receiver, port 4318)"
            ));
        }
    }
    for url in metrics_endpoint.iter().chain(&logs_endpoint) {
        let uri = url.parse::<http::Uri>().ok();
        let ok = uri.as_ref().is_some_and(|u| {
            matches!(u.scheme_str(), Some("http" | "https")) && u.host().is_some()
        });
        if !ok {
            return Err(format!(
                "OTLP endpoint {url:?} must be an http:// or https:// URL with a host"
            ));
        }
    }
    let millis = |var: &str| -> Result<Option<Duration>, String> {
        env(var)
            .map(|v| match v.trim().parse::<u64>() {
                Ok(ms) if ms > 0 => Ok(Duration::from_millis(ms)),
                _ => Err(format!(
                    "{var}={v:?}: expected a positive number of milliseconds"
                )),
            })
            .transpose()
    };
    let interval = match millis("OTEL_METRIC_EXPORT_INTERVAL")? {
        Some(d) => d,
        None => match cfg.metrics_interval_secs {
            Some(0) => return Err("telemetry.metrics_interval_secs must be at least 1".into()),
            Some(s) => Duration::from_secs(s),
            None => DEFAULT_INTERVAL,
        },
    };
    let timeout = millis("OTEL_EXPORTER_OTLP_TIMEOUT")?.unwrap_or(DEFAULT_TIMEOUT);
    let named_by_env = env("OTEL_SERVICE_NAME").is_some()
        || env("OTEL_RESOURCE_ATTRIBUTES").is_some_and(|attrs| {
            attrs
                .split(',')
                .any(|kv| kv.split('=').next().map(str::trim) == Some("service.name"))
        });
    let service_name = (!named_by_env).then(|| {
        cfg.service_name
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.into())
    });
    Ok(Some(Settings {
        metrics_endpoint,
        logs_endpoint,
        service_name,
        interval,
        timeout,
        ca_file: env("OTEL_EXPORTER_OTLP_CERTIFICATE"),
    }))
}

/// The running exporters. Kept in a global so [`shutdown`] can flush from any exit path.
struct Exporters {
    meters: Option<SdkMeterProvider>,
    loggers: Option<SdkLoggerProvider>,
}

static EXPORTERS: OnceLock<Exporters> = OnceLock::new();

/// The log layer to add next to the JSON one, if logs are exported.
pub type LogLayer = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge<
    SdkLoggerProvider,
    opentelemetry_sdk::logs::SdkLogger,
>;

/// Builds the exporters. Runs before the tracing subscriber exists, so it returns errors
/// instead of logging them. Metrics start flowing once [`export_metrics`] names their source.
pub fn start(s: &Settings) -> Result<Option<LogLayer>, String> {
    let https = [&s.metrics_endpoint, &s.logs_endpoint]
        .into_iter()
        .flatten()
        .any(|u| u.starts_with("https://"));
    let tls = if https {
        Some(aqueduct_tls::client_config(
            s.ca_file.as_deref(),
            aqueduct_tls::Verify::Full,
        )?)
    } else {
        None
    };
    let mut resource = Resource::builder()
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")));
    if let Some(name) = &s.service_name {
        resource = resource.with_service_name(name.clone());
    }
    let resource = resource.build();
    let client = |signal: &'static str| OtlpHttp {
        signal,
        timeout: s.timeout,
        tls: tls.clone(),
        failing: AtomicBool::new(false),
    };

    let meters = match &s.metrics_endpoint {
        Some(url) => {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_http_client(client("metrics"))
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(url.clone())
                .with_timeout(s.timeout)
                .build()
                .map_err(|e| format!("OTLP metrics exporter: {e}"))?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(s.interval)
                .build();
            Some(
                SdkMeterProvider::builder()
                    .with_resource(resource.clone())
                    .with_reader(reader)
                    .build(),
            )
        }
        None => None,
    };
    let loggers = match &s.logs_endpoint {
        Some(url) => {
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_http_client(client("logs"))
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(url.clone())
                .with_timeout(s.timeout)
                .build()
                .map_err(|e| format!("OTLP logs exporter: {e}"))?;
            Some(
                SdkLoggerProvider::builder()
                    .with_resource(resource)
                    .with_batch_exporter(exporter)
                    .build(),
            )
        }
        None => None,
    };
    let layer = loggers.as_ref().map(LogLayer::new);
    let _ = EXPORTERS.set(Exporters { meters, loggers });
    Ok(layer)
}

/// `scheme://host:port` of an endpoint, for logs: no user info, path or query.
pub fn origin(url: &str) -> String {
    match url.parse::<http::Uri>() {
        Ok(uri) => format!(
            "{}://{}{}",
            uri.scheme_str().unwrap_or("http"),
            uri.host().unwrap_or_default(),
            uri.port().map(|p| format!(":{p}")).unwrap_or_default()
        ),
        Err(_) => "<invalid endpoint>".into(),
    }
}

/// Whether a log event goes to the OTLP layer. Not the SDK's own events, nor this module's
/// export warnings: those are about the export itself and would only queue behind it.
pub fn exported_target(target: &str) -> bool {
    !(target.starts_with("opentelemetry") || target.starts_with(module_path!()))
}

/// Starts exporting the series that `exposition` renders (the `/metrics` text). A no-op when
/// metrics are not exported. Needs a Tokio runtime: new families are picked up on a timer.
pub fn export_metrics(exposition: impl Fn() -> String + Send + Sync + 'static) {
    let Some(provider) = EXPORTERS.get().and_then(|e| e.meters.as_ref()) else {
        return;
    };
    let mut bridge = Bridge::new(provider.meter("aqueduct"), exposition);
    bridge.discover();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(DISCOVER_EVERY);
        loop {
            tick.tick().await;
            bridge.discover();
        }
    });
}

/// Flushes and stops the exporters, waiting at most a few seconds. Safe to call when export
/// is off, and more than once.
pub fn shutdown() {
    let Some(e) = EXPORTERS.get() else { return };
    let (meters, loggers) = (e.meters.clone(), e.loggers.clone());
    if meters.is_none() && loggers.is_none() {
        return;
    }
    let (done, wait) = std::sync::mpsc::channel();
    // On its own thread: the final export blocks, and a dead collector must not hold up exit.
    let spawned = std::thread::Builder::new()
        .name("otlp-shutdown".into())
        .spawn(move || {
            if let Some(m) = meters {
                let _ = m.shutdown_with_timeout(SHUTDOWN_BUDGET);
            }
            if let Some(l) = loggers {
                let _ = l.shutdown_with_timeout(SHUTDOWN_BUDGET);
            }
            let _ = done.send(());
        });
    if spawned.is_ok() {
        let _ = wait.recv_timeout(SHUTDOWN_BUDGET);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
}

/// One metric family of the exposition: `# HELP`, `# TYPE` and its samples.
#[derive(Debug, Clone, PartialEq)]
pub struct Family {
    pub name: String,
    pub kind: Kind,
    pub help: String,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

/// Parses the text format `/metrics` writes (0.0.4: `# HELP`, `# TYPE`, `name{labels} value`,
/// no timestamps). Lines it does not understand are skipped, and so are non-finite values.
pub fn parse_exposition(text: &str) -> Vec<Family> {
    let mut families: Vec<Family> = Vec::new();
    let family = |families: &mut Vec<Family>, name: &str| -> usize {
        match families.iter().position(|f| f.name == name) {
            Some(i) => i,
            None => {
                families.push(Family {
                    name: name.to_string(),
                    kind: Kind::Gauge,
                    help: String::new(),
                    samples: Vec::new(),
                });
                families.len() - 1
            }
        }
    };
    for line in text.lines() {
        let line = line.trim_end();
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest.split_once(' ').unwrap_or((rest, ""));
            let i = family(&mut families, name);
            families[i].help = help.replace("\\n", "\n").replace("\\\\", "\\");
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').unwrap_or((rest, ""));
            let i = family(&mut families, name);
            families[i].kind = if kind.trim() == "counter" {
                Kind::Counter
            } else {
                Kind::Gauge
            };
        } else if !line.is_empty()
            && !line.starts_with('#')
            && let Some((name, sample)) = parse_sample(line)
        {
            let i = family(&mut families, name);
            families[i].samples.push(sample);
        }
    }
    families
}

/// `name{a="x",b="y\"z"} 1.5` → name, labels, value.
fn parse_sample(line: &str) -> Option<(&str, Sample)> {
    let name_end = line.find(['{', ' '])?;
    let name = &line[..name_end];
    let mut labels = Vec::new();
    let mut rest = &line[name_end..];
    if let Some(body) = rest.strip_prefix('{') {
        let mut chars = body.char_indices();
        loop {
            // label name up to `=`, then a quoted value with `\\`, `\"` and `\n` escapes.
            let start = chars.clone().next()?.0;
            let (eq, _) = chars.by_ref().find(|&(_, c)| c == '=' || c == '}')?;
            if body[start..].starts_with('}') {
                rest = &body[start + 1..];
                break;
            }
            let key = body[start..eq].trim().trim_start_matches(',').trim();
            if chars.next()?.1 != '"' {
                return None;
            }
            let mut value = String::new();
            loop {
                match chars.next()?.1 {
                    '"' => break,
                    '\\' => match chars.next()?.1 {
                        'n' => value.push('\n'),
                        c => value.push(c),
                    },
                    c => value.push(c),
                }
            }
            labels.push((key.to_string(), value));
            match chars.clone().next()? {
                (i, '}') => {
                    rest = &body[i + 1..];
                    break;
                }
                (_, ',') => {
                    chars.next();
                }
                _ => return None,
            }
        }
    }
    let value: f64 = rest.split_whitespace().next()?.parse().ok()?;
    value
        .is_finite()
        .then_some((name, Sample { labels, value }))
}

/// The last parse of the exposition, shared by every instrument's callback.
struct Latest {
    exposition: Box<dyn Fn() -> String + Send + Sync>,
    cache: Mutex<Option<(Instant, Arc<Vec<Family>>)>>,
}

impl Latest {
    fn families(&self) -> Arc<Vec<Family>> {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((at, families)) = cache.as_ref()
            && at.elapsed() < EXPOSITION_TTL
        {
            return families.clone();
        }
        let families = Arc::new(parse_exposition(&(self.exposition)()));
        *cache = Some((Instant::now(), families.clone()));
        families
    }

    fn samples(&self, name: &str) -> Vec<(Vec<KeyValue>, f64)> {
        let families = self.families();
        let Some(f) = families.iter().find(|f| f.name == name) else {
            return Vec::new();
        };
        f.samples
            .iter()
            .map(|s| {
                let attrs = s
                    .labels
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect();
                (attrs, s.value)
            })
            .collect()
    }
}

/// One observable instrument per family name, created the first time the family shows up.
struct Bridge {
    meter: Meter,
    latest: Arc<Latest>,
    known: HashSet<String>,
    /// The instrument handles, kept for the life of the process.
    instruments: Vec<Box<dyn Any + Send>>,
}

impl Bridge {
    fn new(meter: Meter, exposition: impl Fn() -> String + Send + Sync + 'static) -> Self {
        Self {
            meter,
            latest: Arc::new(Latest {
                exposition: Box::new(exposition),
                cache: Mutex::new(None),
            }),
            known: HashSet::new(),
            instruments: Vec::new(),
        }
    }

    fn discover(&mut self) {
        for f in self.latest.families().iter() {
            if !self.known.insert(f.name.clone()) {
                continue;
            }
            let (latest, name) = (self.latest.clone(), f.name.clone());
            let instrument: Box<dyn Any + Send> = match f.kind {
                Kind::Counter => Box::new(
                    self.meter
                        .u64_observable_counter(f.name.clone())
                        .with_description(f.help.clone())
                        .with_callback(move |o| {
                            for (attrs, v) in latest.samples(&name) {
                                o.observe(v.max(0.0).round() as u64, &attrs);
                            }
                        })
                        .build(),
                ),
                Kind::Gauge => Box::new(
                    self.meter
                        .f64_observable_gauge(f.name.clone())
                        .with_description(f.help.clone())
                        .with_callback(move |o| {
                            for (attrs, v) in latest.samples(&name) {
                                o.observe(v, &attrs);
                            }
                        })
                        .build(),
                ),
            };
            self.instruments.push(instrument);
        }
    }
}

/// OTLP/HTTP transport: one short-lived HTTP/1.1 connection per export, run on [`runtime`].
/// Logs the first failure of an outage and the recovery, nothing in between.
#[derive(Debug)]
struct OtlpHttp {
    signal: &'static str,
    timeout: Duration,
    tls: Option<Arc<rustls::ClientConfig>>,
    failing: AtomicBool,
}

#[async_trait::async_trait]
impl HttpClient for OtlpHttp {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let at = origin(&request.uri().to_string());
        let res = send(request, self.timeout, self.tls.clone()).await;
        let problem = match &res {
            Ok(r) if r.status().is_success() => None,
            Ok(r) => Some(format!("HTTP {}", r.status())),
            Err(e) => Some(e.to_string()),
        };
        match problem {
            Some(error) if !self.failing.swap(true, Ordering::Relaxed) => tracing::warn!(
                signal = self.signal,
                endpoint = %at,
                %error,
                "OTLP export failing; dropping {} until the collector answers",
                self.signal
            ),
            None if self.failing.swap(false, Ordering::Relaxed) => {
                tracing::info!(signal = self.signal, endpoint = %at, "OTLP export recovered")
            }
            _ => {}
        }
        res
    }
}

/// The SDK calls the client from its own threads, outside any tokio runtime, so the requests
/// run here: one worker, kept apart from the pipeline's runtime. `None` if it could not start.
fn runtime() -> Option<&'static Runtime> {
    static RUNTIME: OnceLock<Option<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("otlp-http")
                .enable_all()
                .build()
                .ok()
        })
        .as_ref()
}

/// [`post`] on [`runtime`], awaitable from any executor.
async fn send(
    req: Request<Bytes>,
    timeout: Duration,
    tls: Option<Arc<rustls::ClientConfig>>,
) -> Result<Response<Bytes>, HttpError> {
    let rt = runtime().ok_or("the OTLP client runtime did not start")?;
    rt.spawn(async move {
        tokio::time::timeout(timeout, post(req, tls))
            .await
            .map_err(|_| format!("no response within {}s", timeout.as_secs_f32()))?
    })
    .await?
}

async fn post(
    req: Request<Bytes>,
    tls: Option<Arc<rustls::ClientConfig>>,
) -> Result<Response<Bytes>, HttpError> {
    let (parts, body) = req.into_parts();
    let uri = &parts.uri;
    let host = uri.host().ok_or("endpoint has no host")?;
    let https = uri.scheme_str() == Some("https");
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    // Built from host and port, never `uri.authority()`, which would carry userinfo.
    let authority = match uri.port_u16() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    };

    let mut out = hyper::Request::new(Full::new(body));
    *out.method_mut() = parts.method.clone();
    *out.uri_mut() = uri.path_and_query().map_or("/", |p| p.as_str()).parse()?;
    let headers = out.headers_mut();
    for (name, value) in &parts.headers {
        if matches!(
            name.as_str(),
            "host" | "content-length" | "connection" | "transfer-encoding"
        ) {
            continue;
        }
        // The exporter's headers may carry the collector's credentials (OTEL_EXPORTER_OTLP_HEADERS).
        let mut value = value.clone();
        value.set_sensitive(true);
        headers.append(name.clone(), value);
    }
    headers.insert(header::HOST, HeaderValue::from_str(&authority)?);
    headers.insert(header::CONNECTION, HeaderValue::from_static("close"));

    let tcp = TcpStream::connect((bare, port)).await?;
    let _ = tcp.set_nodelay(true);
    if https {
        let cfg = tls.ok_or("https endpoint without a TLS config")?;
        let name = rustls::pki_types::ServerName::try_from(bare.to_string())?;
        let stream = TlsConnector::from(cfg).connect(name, tcp).await?;
        exchange(stream, out).await
    } else {
        exchange(tcp, out).await
    }
}

/// Sends `request` over `io` and reads the reply, its body capped at [`MAX_RESPONSE`]. The
/// connection is driven here rather than spawned, so it ends with the call.
async fn exchange<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
    io: S,
    request: hyper::Request<Full<Bytes>>,
) -> Result<Response<Bytes>, HttpError> {
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let reply = async {
        let (parts, body) = sender.send_request(request).await?.into_parts();
        let body = Limited::new(body, MAX_RESPONSE).collect().await?.to_bytes();
        Ok::<_, HttpError>(Response::from_parts(parts, body))
    };
    let (mut reply, mut conn) = (std::pin::pin!(reply), std::pin::pin!(conn));
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
    use std::collections::HashMap;

    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| vars.get(k).cloned().filter(|v| !v.is_empty())
    }

    fn with_endpoint(url: &str) -> TelemetryConfig {
        TelemetryConfig {
            otlp_endpoint: Some(url.into()),
            ..TelemetryConfig::default()
        }
    }

    #[test]
    fn off_without_an_endpoint() {
        let off = resolve(&TelemetryConfig::default(), &env_of(&[])).unwrap();
        assert_eq!(off, None);
        // Not even a protocol the exporter cannot speak is an error while export is off.
        let env = env_of(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")]);
        assert_eq!(resolve(&TelemetryConfig::default(), &env).unwrap(), None);
    }

    #[test]
    fn the_config_endpoint_turns_both_signals_on_with_defaults() {
        let s = resolve(&with_endpoint("http://collector:4318/"), &env_of(&[]))
            .unwrap()
            .unwrap();
        assert_eq!(
            s,
            Settings {
                metrics_endpoint: Some("http://collector:4318/v1/metrics".into()),
                logs_endpoint: Some("http://collector:4318/v1/logs".into()),
                service_name: Some("aqueduct".into()),
                interval: Duration::from_secs(60),
                timeout: Duration::from_secs(10),
                ca_file: None,
            }
        );
    }

    #[test]
    fn otel_variables_win_over_the_config() {
        let cfg = TelemetryConfig {
            otlp_endpoint: Some("http://from-toml:4318".into()),
            service_name: Some("toml-name".into()),
            metrics_interval_secs: Some(30),
            ..TelemetryConfig::default()
        };
        let env = env_of(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://from-env:4318"),
            (
                "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
                "https://logs.example/ingest",
            ),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
            ("OTEL_SERVICE_NAME", "env-name"),
            ("OTEL_METRIC_EXPORT_INTERVAL", "5000"),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2500"),
            ("OTEL_EXPORTER_OTLP_CERTIFICATE", "/etc/ca.pem"),
        ]);
        let s = resolve(&cfg, &env).unwrap().unwrap();
        assert_eq!(
            s.metrics_endpoint.as_deref(),
            Some("https://from-env:4318/v1/metrics")
        );
        // A per-signal URL is taken as it is.
        assert_eq!(
            s.logs_endpoint.as_deref(),
            Some("https://logs.example/ingest")
        );
        // The SDK reads OTEL_SERVICE_NAME itself; setting it again would override it.
        assert_eq!(s.service_name, None);
        assert_eq!(s.interval, Duration::from_secs(5));
        assert_eq!(s.timeout, Duration::from_millis(2500));
        assert_eq!(s.ca_file.as_deref(), Some("/etc/ca.pem"));

        let env = env_of(&[("OTEL_RESOURCE_ATTRIBUTES", "env=prod, service.name=cdc")]);
        let s = resolve(&cfg, &env).unwrap().unwrap();
        assert_eq!(s.service_name, None);
        assert_eq!(s.interval, Duration::from_secs(30));
        let s = resolve(&cfg, &env_of(&[])).unwrap().unwrap();
        assert_eq!(s.service_name.as_deref(), Some("toml-name"));
    }

    #[test]
    fn each_signal_can_be_turned_off() {
        let cfg = TelemetryConfig {
            metrics: false,
            ..with_endpoint("http://c:4318")
        };
        let s = resolve(&cfg, &env_of(&[])).unwrap().unwrap();
        assert_eq!(s.metrics_endpoint, None);
        assert!(s.logs_endpoint.is_some());

        let env = env_of(&[("OTEL_LOGS_EXPORTER", "none")]);
        let s = resolve(&with_endpoint("http://c:4318"), &env)
            .unwrap()
            .unwrap();
        assert!(s.metrics_endpoint.is_some());
        assert_eq!(s.logs_endpoint, None);

        let env = env_of(&[
            ("OTEL_LOGS_EXPORTER", "none"),
            ("OTEL_METRICS_EXPORTER", "none"),
        ]);
        assert_eq!(
            resolve(&with_endpoint("http://c:4318"), &env).unwrap(),
            None
        );
        let env = env_of(&[("OTEL_SDK_DISABLED", "TRUE")]);
        assert_eq!(
            resolve(&with_endpoint("http://c:4318"), &env).unwrap(),
            None
        );
        // A per-signal endpoint alone turns that signal on.
        let env = env_of(&[(
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "http://m:4318/v1/metrics",
        )]);
        let s = resolve(&TelemetryConfig::default(), &env).unwrap().unwrap();
        assert!(s.metrics_endpoint.is_some());
        assert_eq!(s.logs_endpoint, None);
    }

    #[test]
    fn unsupported_or_malformed_settings_are_errors() {
        let bad =
            |cfg: TelemetryConfig, vars: &[(&str, &str)]| resolve(&cfg, &env_of(vars)).unwrap_err();
        let e = bad(
            with_endpoint("http://c:4317"),
            &[("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")],
        );
        assert!(e.contains("\"grpc\"") && e.contains("http/protobuf"), "{e}");
        let cfg = TelemetryConfig {
            otlp_protocol: Some("http/json".into()),
            ..with_endpoint("http://c:4318")
        };
        assert!(bad(cfg, &[]).contains("http/json"));
        let e = bad(
            with_endpoint("http://c:4318"),
            &[("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "grpc")],
        );
        assert!(e.contains("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"), "{e}");
        assert!(bad(with_endpoint("collector:4318"), &[]).contains("http://"));
        assert!(bad(with_endpoint("ftp://c/"), &[]).contains("http://"));
        let e = bad(
            with_endpoint("http://c:4318"),
            &[("OTEL_METRIC_EXPORT_INTERVAL", "soon")],
        );
        assert!(e.contains("OTEL_METRIC_EXPORT_INTERVAL"), "{e}");
        let cfg = TelemetryConfig {
            metrics_interval_secs: Some(0),
            ..with_endpoint("http://c:4318")
        };
        assert!(bad(cfg, &[]).contains("metrics_interval_secs"));
    }

    #[test]
    fn the_log_layer_skips_export_noise() {
        assert!(exported_target("aqueduct"));
        assert!(exported_target("aqueduct_source_postgres::source"));
        assert!(!exported_target("aqueduct::telemetry"));
        assert!(!exported_target("opentelemetry_sdk"));
    }

    #[test]
    fn parses_names_kinds_help_and_escaped_labels() {
        let text = "# HELP aqueduct_errors_total Errors seen.\n\
                    # TYPE aqueduct_errors_total counter\n\
                    aqueduct_errors_total 3\n\
                    # HELP aqueduct_route_field_unseen A field \\\\ never seen.\n\
                    # TYPE aqueduct_route_field_unseen gauge\n\
                    aqueduct_route_field_unseen{route=\"a\\\\b\",field=\"row.st\\\"at}us, x\"} 1\n\
                    aqueduct_route_field_unseen{route=\"multi\\nline\"} 1\n\
                    # TYPE aqueduct_lag_seconds gauge\n\
                    aqueduct_lag_seconds 0.25\n\
                    aqueduct_untyped NaN\n";
        let f = parse_exposition(text);
        assert_eq!(f.len(), 3);
        assert_eq!(f[0].name, "aqueduct_errors_total");
        assert_eq!(f[0].kind, Kind::Counter);
        assert_eq!(f[0].help, "Errors seen.");
        assert_eq!(
            f[0].samples,
            vec![Sample {
                labels: vec![],
                value: 3.0
            }]
        );
        assert_eq!(f[1].kind, Kind::Gauge);
        assert_eq!(f[1].help, "A field \\ never seen.");
        assert_eq!(
            f[1].samples[0].labels,
            vec![
                ("route".to_string(), "a\\b".to_string()),
                ("field".to_string(), "row.st\"at}us, x".to_string()),
            ]
        );
        assert_eq!(f[1].samples[1].labels[0].1, "multi\nline");
        assert_eq!(f[2].samples[0].value, 0.25);
        // A non-finite sample is dropped.
        assert!(f.iter().all(|f| f.name != "aqueduct_untyped"));
    }

    /// Every sample line `/metrics` serves today comes through the parser, with its labels.
    #[test]
    fn every_sample_of_the_real_exposition_parses() {
        let metrics = Arc::new(aqueduct_core::Metrics::new());
        metrics.record_error("boom");
        let admin = crate::admin::Admin::new(metrics, None, None);
        let text = admin.exposition();
        let lines: Vec<&str> = text
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        let families = parse_exposition(&text);
        let samples: usize = families.iter().map(|f| f.samples.len()).sum();
        assert_eq!(samples, lines.len(), "{text}");
        assert_eq!(
            families.len(),
            text.lines().filter(|l| l.starts_with("# TYPE ")).count()
        );
        let errors = families
            .iter()
            .find(|f| f.name == "aqueduct_errors_total")
            .unwrap();
        assert_eq!((errors.kind, errors.samples[0].value), (Kind::Counter, 1.0));
        let state = families
            .iter()
            .find(|f| f.name == "aqueduct_state")
            .unwrap();
        assert!(state.samples.iter().any(|s| s.labels
            == vec![("state".to_string(), "starting".to_string())]
            && s.value == 1.0));
    }

    /// The SDK side of the mapping: counters become monotonic sums, gauges gauges, labels
    /// attributes, under the exact Prometheus names; families that appear later are picked up.
    #[test]
    fn the_bridge_reports_the_exposition_through_the_sdk() {
        use opentelemetry_sdk::metrics::InMemoryMetricExporter;
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

        let text = Arc::new(Mutex::new(String::from(
            "# HELP aqueduct_events_total Events.\n\
             # TYPE aqueduct_events_total counter\n\
             aqueduct_events_total{op=\"insert\"} 7\n\
             aqueduct_events_total{op=\"delete\"} 2\n\
             # HELP aqueduct_lag_seconds Lag.\n\
             # TYPE aqueduct_lag_seconds gauge\n\
             aqueduct_lag_seconds 1.5\n",
        )));
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let source = text.clone();
        let mut bridge = Bridge::new(provider.meter("aqueduct"), move || {
            source.lock().unwrap().clone()
        });
        bridge.discover();
        text.lock().unwrap().push_str(
            "# HELP aqueduct_snapshot_running Snapshot.\n\
             # TYPE aqueduct_snapshot_running gauge\n\
             aqueduct_snapshot_running 1\n",
        );
        std::thread::sleep(EXPOSITION_TTL);
        bridge.discover();
        provider.force_flush().unwrap();

        let finished = exporter.get_finished_metrics().unwrap();
        let mut seen = std::collections::BTreeMap::new();
        for rm in &finished {
            for sm in rm.scope_metrics() {
                for m in sm.metrics() {
                    let points: Vec<(Vec<String>, String)> = match m.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            assert!(sum.is_monotonic(), "{}", m.name());
                            sum.data_points()
                                .map(|p| {
                                    let a =
                                        p.attributes().map(|kv| format!("{}={}", kv.key, kv.value));
                                    (a.collect(), format!("sum {}", p.value()))
                                })
                                .collect()
                        }
                        AggregatedMetrics::F64(MetricData::Gauge(g)) => g
                            .data_points()
                            .map(|p| {
                                let a = p.attributes().map(|kv| format!("{}={}", kv.key, kv.value));
                                (a.collect(), format!("gauge {}", p.value()))
                            })
                            .collect(),
                        other => panic!("{}: unexpected {other:?}", m.name()),
                    };
                    let mut points = points;
                    points.sort();
                    seen.insert(m.name().to_string(), (m.description().to_string(), points));
                }
            }
        }
        assert_eq!(
            seen["aqueduct_events_total"],
            (
                "Events.".to_string(),
                vec![
                    (vec!["op=delete".to_string()], "sum 2".to_string()),
                    (vec!["op=insert".to_string()], "sum 7".to_string()),
                ]
            )
        );
        assert_eq!(
            seen["aqueduct_lag_seconds"].1,
            vec![(vec![], "gauge 1.5".to_string())]
        );
        assert_eq!(
            seen["aqueduct_snapshot_running"].1,
            vec![(vec![], "gauge 1".to_string())]
        );
        assert_eq!(seen.len(), 3);
    }

    /// A one-shot HTTP server that answers `reply` and hands back the request it got.
    fn serve_once(reply: &'static [u8]) -> (String, std::thread::JoinHandle<Vec<u8>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/v1/metrics", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            while !got.windows(4).any(|w| w == b"\r\n\r\n") || !got.ends_with(b"payload") {
                let n = s.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            s.write_all(reply).unwrap();
            got
        });
        (url, handle)
    }

    fn request(url: &str) -> Request<Bytes> {
        Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/x-protobuf")
            .header("authorization", "Bearer s3cret")
            .body(Bytes::from_static(b"payload"))
            .unwrap()
    }

    #[test]
    fn posts_the_body_with_headers_and_reads_the_reply() {
        let (url, server) =
            serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: x\r\n\r\nokEXTRA");
        let res = block_on(send(request(&url), Duration::from_secs(5), None)).unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(res.body().as_ref(), b"ok");
        let got = String::from_utf8(server.join().unwrap()).unwrap();
        assert!(got.starts_with("POST /v1/metrics HTTP/1.1\r\n"), "{got}");
        assert!(got.contains("content-length: 7\r\n"), "{got}");
        assert!(
            got.contains("content-type: application/x-protobuf\r\n"),
            "{got}"
        );
        assert!(got.contains("authorization: Bearer s3cret\r\n"), "{got}");
        assert!(got.ends_with("\r\n\r\npayload"), "{got}");
    }

    #[test]
    fn reads_chunked_and_error_replies() {
        let (url, server) = serve_once(
            b"HTTP/1.1 503 Service Unavailable\r\nTransfer-Encoding: chunked\r\nRetry-After: 3\r\n\r\n4\r\nbusy\r\n0\r\n\r\n",
        );
        let res = block_on(send(request(&url), Duration::from_secs(5), None)).unwrap();
        assert_eq!(res.status(), 503);
        assert_eq!(res.headers()["retry-after"], "3");
        assert_eq!(res.body().as_ref(), b"busy");
        server.join().unwrap();
        // No length: the body runs to the close.
        let (url, server) = serve_once(b"HTTP/1.1 200 OK\r\n\r\nall");
        let res = block_on(send(request(&url), Duration::from_secs(5), None)).unwrap();
        assert_eq!(res.body().as_ref(), b"all");
        server.join().unwrap();
        // Cut short of its length: an error, not a short body.
        let (url, server) = serve_once(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort");
        assert!(block_on(send(request(&url), Duration::from_secs(5), None)).is_err());
        server.join().unwrap();
    }

    #[test]
    fn a_silent_collector_times_out() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/v1/metrics", listener.local_addr().unwrap());
        let started = Instant::now();
        let res = block_on(send(request(&url), Duration::from_millis(300), None));
        assert!(res.unwrap_err().to_string().contains("no response"));
        assert!(started.elapsed() < Duration::from_secs(3));
        drop(listener);
    }

    #[test]
    fn oversized_replies_are_refused() {
        static BIG: OnceLock<Vec<u8>> = OnceLock::new();
        let big = BIG.get_or_init(|| {
            let mut r = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                MAX_RESPONSE + 1
            )
            .into_bytes();
            r.resize(r.len() + MAX_RESPONSE + 1, b'a');
            r
        });
        let (url, server) = serve_once(big);
        assert!(block_on(send(request(&url), Duration::from_secs(5), None)).is_err());
        let _ = server.join();
    }

    #[test]
    fn a_dead_collector_is_an_error_not_a_hang() {
        // Bind and drop: nothing listens on the port any more.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let client = OtlpHttp {
            signal: "metrics",
            timeout: Duration::from_secs(2),
            tls: None,
            failing: AtomicBool::new(false),
        };
        let started = Instant::now();
        let url = format!("http://127.0.0.1:{port}/v1/metrics");
        let res = block_on(client.send_bytes(request(&url)));
        assert!(res.is_err());
        assert!(client.failing.load(Ordering::Relaxed));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// The SDK drives the client from a plain thread with a minimal executor; so does this.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }
}
