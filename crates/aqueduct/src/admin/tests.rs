use super::api::CSP;
use super::prometheus::escape_label;
use super::*;
use aqueduct_core::SnapshotCommand;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A raw request head and body as the routes see it, through hyper's types. The first of each
/// header counts, as on the wire.
fn parse(raw: &str) -> Option<Request> {
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw, ""));
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split_whitespace();
    let mut b = http::Request::builder()
        .method(first.next()?)
        .uri(first.next()?);
    for (k, v) in lines.filter_map(|l| l.split_once(':')) {
        b = b.header(k.trim(), v.trim());
    }
    let (parts, ()) = b.body(()).ok()?.into_parts();
    Some(Request::new(&parts, body.to_string()))
}

#[test]
fn history_carries_every_op_slot_including_reads() {
    let admin = Admin::new(Arc::new(Metrics::new()), None, None);
    let mut ops = BTreeMap::new();
    ops.insert("public.t".to_string(), [3, 2, 1, 4, 5]);
    admin.record_sample(15.0, &ops, &BTreeMap::new());
    // The second tick sees only the increase since the first.
    let mut next = ops.clone();
    next.insert("public.t".to_string(), [4, 2, 1, 4, 9]);
    admin.record_sample(5.0, &next, &ops);
    let d = lock(&admin.detail);
    let series: Vec<_> = d["public.t"].iter().copied().collect();
    assert_eq!(series, vec![[3, 2, 1, 4, 5], [1, 0, 0, 0, 4]]);
}

/// A tick 3 s late reads as the true rate, not a spike of 3 s of events in one second.
#[test]
fn a_late_sample_is_divided_by_the_time_that_passed() {
    assert_eq!(per_sec(500, Duration::from_millis(SAMPLE_MS)), 500.0);
    assert_eq!(per_sec(2000, Duration::from_secs(4)), 500.0);
    assert_eq!(per_sec(0, Duration::from_secs(4)), 0.0);
}

/// An inline script or handler would need `'unsafe-inline'` in the CSP (#218).
#[test]
fn dashboard_has_no_inline_script_and_csp_forbids_it() {
    assert!(!CSP.contains("script-src 'unsafe-inline'"), "{CSP}");
    assert!(CSP.contains("script-src 'self';"), "{CSP}");
    let lower = DASHBOARD.to_ascii_lowercase();
    assert!(lower.contains(r#"<script src="/admin.js""#));
    for tag in lower.split("<script").skip(1) {
        let head = tag.split('>').next().unwrap();
        assert!(
            head.contains("src=") || head.contains("application/json"),
            "inline <script{head}> in admin.html"
        );
    }
    for src in [DASHBOARD, SCRIPT] {
        if let Some(i) = inline_handler(src) {
            let b = src.as_bytes();
            let end = (i + 24).min(b.len());
            panic!("inline handler: {}", String::from_utf8_lossy(&b[i..end]));
        }
        assert!(
            !src.to_ascii_lowercase().contains("javascript:"),
            "javascript: URL"
        );
    }
}

/// Byte offset of the first inline event-handler attribute in `src`: whitespace or `/`, then
/// `on` and letters, then optional whitespace and `=` (`onclick=`, ` onclick =`, `<a/onclick=`).
fn inline_handler(src: &str) -> Option<usize> {
    let b = src.to_ascii_lowercase().into_bytes();
    (1..b.len()).find(|&i| {
        if !(b[i - 1].is_ascii_whitespace() || b[i - 1] == b'/') || !b[i..].starts_with(b"on") {
            return false;
        }
        let letters = b[i + 2..]
            .iter()
            .take_while(|c| c.is_ascii_lowercase())
            .count();
        let after = &b[i + 2 + letters..];
        let after = &after[after.iter().take_while(|c| c.is_ascii_whitespace()).count()..];
        letters > 0 && after.first() == Some(&b'=')
    })
}

#[test]
fn inline_handler_scan_catches_every_spelling_and_spares_lookalikes() {
    for bad in [
        "<a onmouseover=\"x\">",
        "<a onclick =\"x\">",
        "<a/onclick=x>",
        "<a
onkeydown
=x>",
        "<A ONCLICK=x>",
    ] {
        assert!(inline_handler(bad).is_some(), "missed {bad:?}");
    }
    for ok in [
        "addEventListener('x', f, { once: true })",
        "<button aria-pressed=\"true\">",
        "let on = 1; once: true",
        "text · on – ≤ done",
    ] {
        assert_eq!(inline_handler(ok), None, "false positive on {ok:?}");
    }
}

/// Every element the dashboard script looks up by id must exist in the HTML,
/// otherwise the script throws on load and every handler after it is dead.
#[test]
fn dashboard_script_ids_exist_in_html() {
    let ids = |prefix: &str, suffix: &str| -> Vec<&str> {
        [DASHBOARD, SCRIPT]
            .into_iter()
            .flat_map(|src| src.split(prefix).skip(1))
            .filter_map(|r| r.split(suffix).next())
            .collect()
    };
    let defined = ids("id=\"", "\"");
    let mut missing: Vec<&str> = ids("$('", "')")
        .into_iter()
        .chain(ids("getElementById('", "')"))
        // Skip ids built at runtime (`$('op-' + k)`); only literal ids can be checked.
        .filter(|id| !id.contains(['\'', ' ', '(', ')']))
        .filter(|id| !defined.contains(id))
        .collect();
    missing.sort_unstable();
    missing.dedup();
    assert!(
        missing.is_empty(),
        "ids used by script but absent from admin.html: {missing:?}"
    );
    // The detail chart's op buttons index the server's per-op rows, so they follow its slot order.
    let labels = SCRIPT
        .split("const OP_LABEL = [")
        .nth(1)
        .and_then(|r| r.split(']').next())
        .expect("OP_LABEL in admin.js");
    assert_eq!(labels.split(',').count(), OP_SLOTS, "one button per op");
    assert_eq!(
        labels, "'insert', 'update', 'delete', 'truncate', 'read'",
        "the order of the op slots"
    );
}

/// The routing graph reimplements `glob_match` in JS to draw which route claims a table. The page
/// asserts its version against the cases in `#globcases` on load; the Rust function must agree.
#[test]
fn dashboard_glob_cases_agree_with_glob_match() {
    let cases = DASHBOARD
        .split("id=\"globcases\">")
        .nth(1)
        .and_then(|r| r.split("</script>").next())
        .expect("#globcases in admin.html");
    let cases: Vec<(String, String, bool)> = serde_json::from_str(cases).unwrap();
    assert!(cases.len() >= 9, "{cases:?}");
    for (pattern, text, want) in cases {
        assert_eq!(
            aqueduct_sinks::glob_match(&pattern, &text),
            want,
            "{pattern:?} vs {text:?}"
        );
    }
}

/// Two top-level `function f(` with the same name in the page: the later one silently replaces the
/// earlier, which once broke the cockpit when two cards each brought their own `spark`.
#[test]
fn dashboard_declares_each_function_once() {
    let mut seen = std::collections::HashSet::new();
    for line in SCRIPT.lines() {
        let Some(rest) = line.strip_prefix("function ") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        assert!(
            seen.insert(name.clone()),
            "function {name} is declared twice in admin.js"
        );
    }
    assert!(seen.len() > 10, "{seen:?}");
}

#[test]
fn parses_token_from_query_and_header() {
    let r = parse("GET /api/stream?x=1&token=abc HTTP/1.1\r\nhost: a\r\n\r\n").unwrap();
    assert_eq!(
        (r.path.as_str(), r.bearer.as_deref()),
        ("/api/stream", None)
    );
    let r = parse("GET /api/metrics HTTP/1.1\r\nAuthorization: Bearer s3\r\n\r\n").unwrap();
    assert_eq!(r.bearer.as_deref(), Some("s3"));
    let r = parse("POST /api/snapshot HTTP/1.1\r\ncontent-length: 2\r\n\r\n{}").unwrap();
    assert_eq!((r.method.as_str(), r.body.as_str()), ("POST", "{}"));
}

#[test]
fn token_gate() {
    let m = Arc::new(Metrics::new());
    let open = Admin::new(m.clone(), None, None);
    let locked = Admin::new(m, Some("k".into()), None);
    let anon = parse("GET /api/metrics HTTP/1.1\r\n\r\n").unwrap();
    let ok = parse("GET /api/metrics HTTP/1.1\r\nAuthorization: Bearer k\r\n\r\n").unwrap();
    let bad = parse("GET /api/metrics HTTP/1.1\r\nAuthorization: Bearer kk\r\n\r\n").unwrap();
    let query = parse("GET /api/metrics?token=k HTTP/1.1\r\n\r\n").unwrap();
    assert!(!locked.authorized(&query));
    assert!(open.authorized(&anon));
    assert!(!locked.authorized(&anon));
    assert!(!locked.authorized(&bad));
    assert!(locked.authorized(&ok));
}

#[test]
fn constant_time_eq() {
    assert!(ct_eq("abc", "abc"));
    assert!(!ct_eq("abc", "abd"));
    assert!(!ct_eq("abc", "abcd"));
    assert!(ct_eq("", ""));
}

#[test]
fn prometheus_output() {
    let m = Metrics::new();
    let s = m.snapshot();
    let out = prometheus(&s);
    assert!(out.contains("aqueduct_events_total{op=\"insert\"} 0"));
    assert!(out.contains("aqueduct_state{state=\"starting\"} 1"));
    assert!(out.contains("aqueduct_errors_total 0"));
    assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
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

async fn get_bearer(addr: std::net::SocketAddr, path: &str, token: &str) -> String {
    let mut c = TcpStream::connect(addr).await.unwrap();
    c.write_all(
        format!(
            "GET {path} HTTP/1.1
host: x
Authorization: Bearer {token}

"
        )
        .as_bytes(),
    )
    .await
    .unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).await.unwrap();
    out
}

async fn start(token: Option<&str>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(Metrics::new());
    let admin = Admin::new(metrics, token.map(str::to_string), None);
    tokio::spawn(serve(listener, admin));
    addr
}

#[tokio::test]
async fn serves_dashboard_health_and_metrics() {
    let addr = start(Some("k")).await;
    let page = get(addr, "/").await;
    assert!(page.contains("Stream operations"));
    assert!(page.contains("content-security-policy"));
    assert!(page.contains("x-frame-options: DENY"));
    assert!(get(addr, "/admin.css").await.contains("text/css"));
    assert!(get(addr, "/admin.js").await.contains("text/javascript"));
    assert!(get(addr, "/healthz").await.ends_with("ok"));
    assert!(get(addr, "/readyz").await.starts_with("HTTP/1.1 503"));
    assert!(get(addr, "/api/metrics").await.starts_with("HTTP/1.1 401"));
    assert!(get(addr, "/metrics").await.starts_with("HTTP/1.1 401"));
    let ok = get_bearer(addr, "/api/metrics", "k").await;
    assert!(ok.starts_with("HTTP/1.1 200") && ok.contains("\"events_total\":0"));
    assert!(ok.contains("\"inspector\":false"));
    let prom = get_bearer(addr, "/metrics", "k").await;
    assert!(prom.contains("aqueduct_events_total{op=\"insert\"} 0"));
    assert!(get(addr, "/api/events").await.starts_with("HTTP/1.1 401"));
}

/// A plan with a filtered route, a plain route and the default, after a few events.
fn routed_plan() -> RoutePlan {
    use aqueduct_core::{ChangeEvent, Op, Position};
    let route = |name: &str, pattern: &str, filter: Option<&str>| aqueduct_sinks::KafkaRoute {
        name: Some(name.into()),
        pattern: pattern.into(),
        topic: format!(
            "{}.t",
            name.replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        ),
        filter: filter.map(Into::into),
        ..Default::default()
    };
    let plan = RoutePlan::with_dead_letter(
        vec![
            route(
                "paid \"orders\"",
                "public.orders",
                Some("op == 'insert' and row.status == 'paid'"),
            ),
            route("rest", "public.*", None),
        ],
        Some("cdc.{table}".into()),
        Some("aqueduct.dlq".into()),
    )
    .unwrap();
    let ev = |table: &str, row: &str| ChangeEvent {
        position: Position::Lsn(1),
        xid: 1,
        commit_ts_micros: 0,
        schema: "public".into(),
        table: table.into(),
        op: Op::Insert,
        before: None,
        after: serde_json::value::RawValue::from_string(row.into()).ok(),
        key: None,
        checkpoint: None,
        unchanged: Vec::new(),
        after_is_key: false,
    };
    for (table, row) in [
        ("orders", r#"{"status":"paid","card":"4111-secret"}"#),
        ("orders", r#"{"status":"open"}"#),
        ("orders", r#"{"status":7}"#),
        ("users", r#"{"id":1}"#),
    ] {
        plan.render(&ev(table, row), 0).unwrap();
    }
    plan
}

#[tokio::test]
async fn routes_api_needs_the_token_and_describes_the_plan() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let admin = Admin::new(Arc::new(Metrics::new()), Some("k".into()), None);
    tokio::spawn(serve(listener, admin.clone()));
    // Before the plan is set, the route exists but has nothing to show.
    assert!(
        get_bearer(addr, "/api/routes", "k")
            .await
            .starts_with("HTTP/1.1 404")
    );
    admin.set_router(routed_plan());

    let r = get(addr, "/api/routes").await;
    assert!(r.starts_with("HTTP/1.1 401"), "{r}");
    assert!(
        get_bearer(addr, "/api/routes", "wrong")
            .await
            .starts_with("HTTP/1.1 401")
    );
    let r = get_bearer(addr, "/api/routes", "k").await;
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
    assert!(!r.contains("4111-secret"), "no row data: {r}");
    let body = r.split("\r\n\r\n").nth(1).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["default_topic"], "cdc.{table}");
    assert_eq!(v["dead_letter_topic"], "aqueduct.dlq");
    assert_eq!(v["partition_by"], "key");
    let routes = v["routes"].as_array().unwrap();
    assert_eq!(routes.len(), 3);
    assert_eq!(
        routes[0],
        json!({"route": "paid \"orders\"", "match": "public.orders",
               "where": "op == 'insert' and row.status == 'paid'", "key": null, "topic": "paid--orders-.t", "topic_fallback": null, "topic_values": null,
               "dead_letter_topic": "aqueduct.dlq",
               "counts": {"matched": 3, "produced": 1, "filtered": 1, "fallback": 0, "errors": 1, "tombstones_skipped": 0,
                          "ops": {"insert": 1, "update": 0, "delete": 0, "truncate": 0, "read": 0}},
               "unseen_fields": []})
    );
    assert_eq!(routes[1]["counts"]["produced"], 1);
    assert_eq!(routes[2]["route"], "default");
    assert_eq!(routes[2]["match"], serde_json::Value::Null);

    // /api/metrics carries the flat per-route counters.
    let r = get_bearer(addr, "/api/metrics", "k").await;
    let v: serde_json::Value = serde_json::from_str(r.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(
        v["routes"][0],
        json!({"route": "paid \"orders\"", "matched": 3, "produced": 1, "filtered": 1,
               "fallback": 0, "errors": 1, "tombstones_skipped": 0,
               "ops": {"insert": 1, "update": 0, "delete": 0, "truncate": 0, "read": 0},
               "unseen_fields": []})
    );

    // Prometheus: one series per route and outcome, label values escaped.
    let prom = get_bearer(addr, "/metrics", "k").await;
    for line in [
        r#"aqueduct_route_records_total{route="paid \"orders\"",outcome="produced"} 1"#,
        r#"aqueduct_route_records_total{route="paid \"orders\"",outcome="filtered"} 1"#,
        r#"aqueduct_route_records_total{route="paid \"orders\"",outcome="errors"} 1"#,
        r#"aqueduct_route_records_total{route="paid \"orders\"",outcome="fallback"} 0"#,
        r#"aqueduct_route_records_total{route="default",outcome="produced"} 0"#,
        r#"aqueduct_route_tombstones_skipped_total{route="paid \"orders\""} 0"#,
        "# TYPE aqueduct_route_field_unseen gauge",
    ] {
        assert!(prom.contains(line), "missing {line}");
    }
}

#[test]
fn unseen_fields_are_exported_with_escaped_labels() {
    let mut o = String::new();
    route_metrics(
        &mut o,
        &[RouteCount {
            route: "a\\b".into(),
            counts: Default::default(),
            unseen_fields: vec!["row.st\"atus".into()],
        }],
    );
    assert!(o.contains(r#"aqueduct_route_field_unseen{route="a\\b",field="row.st\"atus"} 1"#));
    // No routes (no plan): no route series at all.
    let mut o = String::new();
    route_metrics(&mut o, &[]);
    assert!(o.is_empty());
}

#[tokio::test]
async fn kpi_history_needs_the_token_and_carries_the_seconds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let admin = Admin::new(Arc::new(Metrics::new()), Some("k".into()), None);
    tokio::spawn(serve(listener, admin.clone()));
    let usage = Some(kpis::Usage {
        cpu_secs: 1.0,
        rss_bytes: 48_000_000,
    });
    let snap = admin.metrics.snapshot();
    lock(&admin.kpis).record(unix_ms(), 7.0, 2_000_000.0, &snap, usage);

    for bad in [
        get(addr, "/api/kpis/history").await,
        get_bearer(addr, "/api/kpis/history", "kk").await,
    ] {
        assert!(bad.starts_with("HTTP/1.1 401"), "{bad}");
    }
    let r = get_bearer(addr, "/api/kpis/history", "k").await;
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
    let v: serde_json::Value = serde_json::from_str(r.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(v["len"], HISTORY_LEN);
    assert_eq!(v["interval_ms"], 1_000);
    assert_eq!(v["throughput"], json!([7.0]));
    assert_eq!(v["kafka_mb_per_sec"], json!([2.0]));
    assert_eq!(v["lag_ms"], json!([null]));
    assert_eq!(v["cpu_pct"], json!([null]));
    assert_eq!(v["mem_mb"], json!([48.0]));
}

#[tokio::test]
async fn route_history_needs_the_token_and_carries_route_and_topic_rates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let admin = Admin::new(Arc::new(Metrics::new()), Some("k".into()), None);
    tokio::spawn(serve(listener, admin.clone()));
    let plan = routed_plan();
    let stats = Arc::new(TopicStats::new());
    admin.set_router(plan.clone());
    admin.set_topic_stats(stats.clone());
    // Two ticks of the sampler, the sink's counters moving in between.
    let ops = |code: &str, n: u64| {
        let mut o = OpCounts::default();
        o.add(code, n);
        o
    };
    stats.acked("paid--orders-.t", &ops("c", 1));
    lock(&admin.rates).record(
        &admin.route_counts(),
        &admin.topic_counts(),
        &admin.topic_bytes(),
    );
    stats.acked("paid--orders-.t", &ops("u", 2));
    stats.acked("cdc.users", &ops("d", 4));
    stats.acked_bytes("cdc.users", 640);
    lock(&admin.rates).record(
        &admin.route_counts(),
        &admin.topic_counts(),
        &admin.topic_bytes(),
    );

    for bad in [
        get(addr, "/api/routes/history").await,
        get_bearer(addr, "/api/routes/history", "kk").await,
    ] {
        assert!(bad.starts_with("HTTP/1.1 401"), "{bad}");
    }
    let r = get_bearer(addr, "/api/routes/history", "k").await;
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
    assert!(!r.contains("4111-secret"), "no row data: {r}");
    let v: serde_json::Value = serde_json::from_str(r.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(
        (v["seq"].as_u64(), v["len"].as_u64()),
        (Some(2), Some(HISTORY_LEN as u64))
    );
    assert_eq!(v["interval_ms"], 1000);
    let routes = v["routes"].as_array().unwrap();
    assert_eq!(routes.len(), 3);
    // The plan's counters were already there at the first tick; nothing moved after it.
    assert_eq!(
        routes[0],
        json!({"route": "paid \"orders\"", "produced": [1, 0], "filtered": [1, 0],
               "fallback": [0, 0], "errors": [1, 0],
               "ops": [[1, 0, 0, 0, 0], [0, 0, 0, 0, 0]],
               "filtered_ops": [[1, 0, 0, 0, 0], [0, 0, 0, 0, 0]],
               "fallback_ops": [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0]],
               "errors_ops": [[1, 0, 0, 0, 0], [0, 0, 0, 0, 0]]})
    );
    assert_eq!(
        v["topics"],
        json!({"cdc.users": [0, 4], "paid--orders-.t": [1, 2]})
    );
    // Per op: rows of [insert, update, delete, truncate, read], and the totals since start.
    assert_eq!(
        v["topic_ops"],
        json!({"cdc.users": [[0, 0, 0, 0, 0], [0, 0, 4, 0, 0]],
               "paid--orders-.t": [[1, 0, 0, 0, 0], [0, 2, 0, 0, 0]]})
    );
    assert_eq!(v["topic_bytes"], json!({"cdc.users": [0, 640]}));
    assert_eq!(
        v["topic_totals"]["paid--orders-.t"],
        json!({"insert": 1, "update": 2, "delete": 0, "truncate": 0, "read": 0})
    );

    // Each stream frame carries only the newest second.
    let r = get_bearer(addr, "/api/metrics", "k").await;
    let m: serde_json::Value = serde_json::from_str(r.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(
        m["rates"],
        json!({"seq": 2, "routes": [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]],
               "route_ops": [[0, 0, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]],
               "route_outcome_ops": ([[[0u64; 5]; 3]; 3]),
               "topics": {"cdc.users": 4, "paid--orders-.t": 2},
               "topic_ops": {"cdc.users": [0, 0, 4, 0, 0], "paid--orders-.t": [0, 2, 0, 0, 0]},
               "topic_bytes": {"cdc.users": 640}})
    );
    // The stream's per-route totals carry the produced breakdown too.
    assert_eq!(m["routes"][0]["ops"]["insert"], 1);

    // Prometheus: one counter per topic, label values escaped.
    let prom = get_bearer(addr, "/metrics", "k").await;
    for line in [
        "# TYPE aqueduct_topic_records_total counter",
        r#"aqueduct_topic_records_total{topic="cdc.users"} 4"#,
        r#"aqueduct_topic_records_total{topic="paid--orders-.t"} 3"#,
    ] {
        assert!(prom.contains(line), "missing {line}");
    }
}

#[test]
fn topic_counters_are_exported_with_escaped_labels() {
    let mut o = String::new();
    topic_metrics(
        &mut o,
        &BTreeMap::from([
            (
                "(other)".to_string(),
                OpCounts {
                    insert: 1,
                    delete: 1,
                    ..Default::default()
                },
            ),
            (
                "a\"b".to_string(),
                OpCounts {
                    read: 1,
                    ..Default::default()
                },
            ),
        ]),
    );
    assert!(
        o.contains(r#"aqueduct_topic_records_total{topic="(other)"} 2"#),
        "{o}"
    );
    assert!(
        o.contains(r#"aqueduct_topic_records_total{topic="a\"b"} 1"#),
        "{o}"
    );
    // No topic produced yet: no series.
    let mut o = String::new();
    topic_metrics(&mut o, &BTreeMap::new());
    assert!(o.is_empty());
}

#[tokio::test]
async fn inspector_is_disabled_without_kafka_inspector() {
    let addr = start(Some("k")).await;
    let r = get_bearer(addr, "/api/events", "k").await;
    assert!(r.starts_with("HTTP/1.1 403"));
}

// #11: the token must never be accepted from the URL (proxy logs, history, Referer).
#[tokio::test]
async fn other_methods_are_refused() {
    let addr = start(Some("k")).await;
    for m in ["PUT", "PATCH", "OPTIONS"] {
        let r = raw(
            addr,
            &format!(
                "{m} /api/metrics HTTP/1.1
host: x"
            ),
            "",
        )
        .await;
        assert!(r.starts_with("HTTP/1.1 405"), "{m}: {r}");
    }
}

#[tokio::test]
async fn query_string_token_is_rejected() {
    let addr = start(Some("k")).await;
    for path in [
        "/api/metrics?token=k",
        "/metrics?token=k",
        "/api/stream?token=k",
        "/api/events?token=k",
        "/api/tables?token=k",
        "/api/routes?token=k",
        "/api/routes/history?token=k",
    ] {
        assert!(
            get(addr, path).await.starts_with("HTTP/1.1 401"),
            "{path} accepted ?token="
        );
    }
    assert!(
        get_bearer(addr, "/api/metrics", "k")
            .await
            .starts_with("HTTP/1.1 200")
    );
}

// Contract the dashboard's fetch-based SSE client parses: Bearer-authenticated stream, a
// 200 text/event-stream head, then `data: <json>` frames (one full snapshot each).
#[tokio::test]
async fn stream_with_bearer_yields_json_data_frames() {
    let addr = start(Some("k")).await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    c.write_all(b"GET /api/stream HTTP/1.1\r\nhost: x\r\nAuthorization: Bearer k\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let n = c.read(&mut chunk).await.unwrap();
            assert!(n > 0, "stream closed before first frame");
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf);
            // The body is chunked: a chunk per frame, each frame ending in a blank line.
            if text.contains("data: ") && text.contains("\n\n\r\n") {
                return text.into_owned();
            }
        }
    })
    .await
    .expect("no SSE frame within 5s");
    assert!(got.starts_with("HTTP/1.1 200") && got.contains("text/event-stream"));
    let data = got.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    let v: serde_json::Value = serde_json::from_str(data).unwrap();
    assert!(v.get("events_per_sec").is_some());
    // Unauthenticated stream is refused.
    assert!(get(addr, "/api/stream").await.starts_with("HTTP/1.1 401"));
}

async fn request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
) -> String {
    let mut c = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: x\r\nAuthorization: Bearer {token}\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    );
    // Split the request in two writes, as a client may: the server must read it whole.
    let (a, b) = req.split_at(req.len() / 2);
    c.write_all(a.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    c.write_all(b.as_bytes()).await.unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).await.unwrap();
    out
}

/// A stand-in source: starts accept `public.*` tables, a cancel works only after a start.
fn fake_source(admin: &Admin) {
    let (tx, mut rx) = aqueduct_core::snapshot_channel();
    admin.set_snapshots(tx);
    tokio::spawn(async move {
        let mut running = false;
        while let Some(req) = rx.recv().await {
            let answer = match req.command {
                SnapshotCommand::Start(t) if t.iter().all(|t| t.starts_with("public.")) => {
                    running = true;
                    Ok("s1".to_string())
                }
                SnapshotCommand::Start(t) => Err(format!("not in publication: {}", t.join(", "))),
                SnapshotCommand::Cancel if running => {
                    running = false;
                    Ok("s1".to_string())
                }
                SnapshotCommand::Cancel => Err("no snapshot is running".to_string()),
            };
            let _ = req.reply.send(answer);
        }
    });
}

#[tokio::test]
async fn snapshot_api_needs_the_token_and_validates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(Metrics::new());
    let admin = Admin::new(metrics.clone(), Some("k".into()), None);
    tokio::spawn(serve(listener, admin.clone()));

    // No source with snapshots yet: refused, but only after the token check.
    for m in ["GET", "POST", "DELETE"] {
        let r = request(addr, m, "/api/snapshot", "wrong", "").await;
        assert!(r.starts_with("HTTP/1.1 401"), "{m}: {r}");
    }
    let r = request(
        addr,
        "POST",
        "/api/snapshot",
        "k",
        r#"{"tables":["public.a"]}"#,
    )
    .await;
    assert!(r.starts_with("HTTP/1.1 501"), "{r}");
    // Other paths stay GET only.
    assert!(
        request(addr, "POST", "/api/metrics", "k", "")
            .await
            .starts_with("HTTP/1.1 405")
    );

    fake_source(&admin);
    let r = request(addr, "GET", "/api/snapshot", "k", "").await;
    assert!(
        r.starts_with("HTTP/1.1 200") && r.contains(r#""state":"idle""#),
        "{r}"
    );
    for bad in [
        "",
        "{}",
        r#"{"tables":[]}"#,
        r#"{"tables":["nodot"]}"#,
        r#"{"tables":[1]}"#,
    ] {
        let r = request(addr, "POST", "/api/snapshot", "k", bad).await;
        assert!(r.starts_with("HTTP/1.1 400"), "{bad}: {r}");
    }
    let r = request(
        addr,
        "POST",
        "/api/snapshot",
        "k",
        r#"{"tables":["other.x"]}"#,
    )
    .await;
    assert!(
        r.starts_with("HTTP/1.1 409") && r.contains("not in publication"),
        "{r}"
    );
    let r = request(addr, "DELETE", "/api/snapshot", "k", "").await;
    assert!(
        r.starts_with("HTTP/1.1 409") && r.contains("no snapshot is running"),
        "{r}"
    );
    let r = request(
        addr,
        "POST",
        "/api/snapshot",
        "k",
        r#"{"tables":["public.a","public.b"]}"#,
    )
    .await;
    assert!(
        r.starts_with("HTTP/1.1 202") && r.contains(r#""id":"s1""#),
        "{r}"
    );
    assert!(
        r.contains(r#""durable":false"#),
        "the API says a request is not durable yet: {r}"
    );
    let r = request(addr, "DELETE", "/api/snapshot", "k", "").await;
    assert!(
        r.starts_with("HTTP/1.1 202") && r.contains("cancelling"),
        "{r}"
    );

    // The status the source reports is what GET returns, and what Prometheus exports.
    metrics.set_snapshot(Some(aqueduct_core::SnapshotStatus {
        id: "s1".into(),
        state: "running".into(),
        tables: vec!["public.a".into(), "public.b".into()],
        tables_done: 1,
        table: Some("public.b".into()),
        rows: 42,
        rows_estimated: Some(100),
        skipped: vec![],
    }));
    let r = request(addr, "GET", "/api/snapshot", "k", "").await;
    assert!(
        r.contains(r#""rows":42"#) && r.contains(r#""table":"public.b""#),
        "{r}"
    );
    let prom = prometheus(&metrics.snapshot());
    assert!(prom.contains("aqueduct_snapshot_running 1"));
    assert!(prom.contains("aqueduct_snapshot_rows_total 42"));
    assert!(prom.contains("aqueduct_snapshot_tables_done 1"));
    assert!(prom.contains("aqueduct_snapshot_rows_estimated 100"));
}

fn change(
    schema: &str,
    table: &str,
    op: aqueduct_core::Op,
    lsn: u64,
) -> aqueduct_core::ChangeEvent {
    aqueduct_core::ChangeEvent {
        position: aqueduct_core::Position::Lsn(lsn),
        xid: 1,
        commit_ts_micros: 1_700_000_000_250_000,
        schema: schema.into(),
        table: table.into(),
        op,
        before: None,
        after: None,
        key: None,
        checkpoint: None,
        unchanged: Vec::new(),
        after_is_key: false,
    }
}

fn json_body(resp: &str) -> serde_json::Value {
    serde_json::from_str(resp.split("\r\n\r\n").nth(1).unwrap()).unwrap()
}

/// Serves an admin whose sink routes `public.*` to one shared topic and everything else to
/// a topic per table, with three tables' events delivered and a checkpoint taken.
async fn start_tables(inspector: Option<KafkaInspector>) -> std::net::SocketAddr {
    use aqueduct_core::{Op, Position};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(Metrics::new());
    let admin = Admin::new(metrics.clone(), Some("k".into()), inspector);
    admin.set_router(router());
    let d = Duration::from_millis(1);
    metrics.record_event(&change("public", "a", Op::Insert, 0x10), d);
    metrics.record_event(&change("public", "a", Op::Update, 0x1_0000_0020), d);
    metrics.record_event(&change("public", "b", Op::Read, 0x30), d);
    metrics.record_event(&change("hr", "staff", Op::Delete, 0x40), d);
    metrics.record_checkpoint(&Position::Lsn(0x40));
    tokio::spawn(serve(listener, admin));
    addr
}

fn router() -> RoutePlan {
    let route = aqueduct_sinks::KafkaRoute {
        name: None,
        pattern: "public.*".into(),
        topic: "cdc.public".into(),
        ..Default::default()
    };
    RoutePlan::new(vec![route], Some("cdc.{schema}.{table}".into())).unwrap()
}

#[tokio::test]
async fn the_snapshot_cursor_is_never_shown_as_the_checkpoint() {
    use aqueduct_core::{Checkpoint, Position, SnapState};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(Metrics::new());
    let admin = Admin::new(metrics.clone(), Some("k".into()), None);
    admin.set_router(router());
    let ckpt = Checkpoint {
        pos: "16/B374D848@16/B374D8E0#2;db=5".into(),
        snap: Some(SnapState {
            id: "s1".into(),
            tables: vec!["public.users".into(), "public.orders".into()],
            table: 1,
            after: Some(vec!["ada@example.com".into()]),
        }),
    };
    metrics.record_checkpoint(&Position::Text(ckpt.encode()));
    tokio::spawn(serve(listener, admin));
    let want = "16/B374D848@16/B374D8E0#2;db=5 (snapshot: public.orders 2/2)";
    for path in ["/api/tables", "/api/metrics"] {
        let r = get_bearer(addr, path, "k").await;
        assert!(r.starts_with("HTTP/1.1 200"), "{path}: {r}");
        assert!(
            !r.contains("ada@example.com"),
            "{path} leaks the cursor: {r}"
        );
        assert_eq!(json_body(&r)["checkpoint"], want, "{path}");
    }
}

#[tokio::test]
async fn tables_api_needs_the_token_and_lists_marks_without_kafka() {
    let addr = start_tables(None).await;
    assert!(get(addr, "/api/tables").await.starts_with("HTTP/1.1 401"));
    assert!(
        get_bearer(addr, "/api/tables", "kk")
            .await
            .starts_with("HTTP/1.1 401")
    );
    let r = get_bearer(addr, "/api/tables", "k").await;
    assert!(
        r.starts_with("HTTP/1.1 200") && r.contains("application/json"),
        "{r}"
    );
    let v = json_body(&r);
    assert_eq!(v["checkpoint"], "0/40");
    assert_eq!(v["inspector"], false);
    assert!(v["kafka_read_ms"].is_null());
    let tables = v["tables"].as_array().unwrap();
    let names: Vec<_> = tables
        .iter()
        .map(|t| t["table"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["hr.staff", "public.a", "public.b"]);
    let a = &tables[1];
    assert_eq!(a["topic"], "cdc.public");
    assert_eq!(a["events"], 2);
    assert_eq!(a["ops"], json!({ "c": 1, "u": 1, "d": 0, "t": 0, "r": 0 }));
    assert_eq!(a["last_lsn"], "1/20");
    assert_eq!(a["last_commit_ms"], 1_700_000_000_250i64);
    assert!(a["last_delivered_ms"].as_u64().unwrap() > 0);
    assert!(a["kafka"].is_null());
    assert!(a["error"].as_str().unwrap().contains("inspector"), "{a}");
    assert_eq!(tables[0]["topic"], "cdc.hr.staff");
    assert_eq!(tables[2]["ops"]["r"], 1);
}

/// Kafka down: `/api/tables` still answers within the timeout with the pipeline's numbers,
/// says why the offsets are missing, and the next call reuses the failed read.
#[tokio::test]
async fn tables_api_answers_without_offsets_while_kafka_is_down() {
    let dead = KafkaInspector::new(
        vec!["127.0.0.1:1".into()],
        &router(),
        aqueduct_sinks::KafkaSecurity::default(),
    );
    let addr = start_tables(Some(dead)).await;
    let began = Instant::now();
    let first = tokio::spawn(get_bearer(addr, "/api/tables", "k"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Other routes are not held up while Kafka is read.
    let other = Instant::now();
    assert!(
        get_bearer(addr, "/api/metrics", "k")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert!(other.elapsed() < Duration::from_secs(1));
    let r = first.await.unwrap();
    assert!(
        began.elapsed() < HWM_TIMEOUT + Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
    let v = json_body(&r);
    assert_eq!(v["inspector"], true);
    assert!(v["kafka_read_ms"].as_u64().is_some());
    for t in v["tables"].as_array().unwrap() {
        assert!(t["kafka"].is_null(), "{t}");
        assert!(!t["error"].as_str().unwrap().is_empty(), "{t}");
        assert!(t["events"].as_u64().unwrap() > 0);
    }
    // Within the cache window the failed read is reused rather than retried.
    let began = Instant::now();
    let again = get_bearer(addr, "/api/tables", "k").await;
    assert!(again.starts_with("HTTP/1.1 200") && again.contains("\"kafka\":null"));
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "{:?}",
        began.elapsed()
    );
}

#[test]
fn kafka_entry_sums_partitions_and_counts_sharing_tables() {
    let mut found = Offsets::new();
    found.insert("cdc.public".into(), vec![(0, 5), (1, 7)]);
    let hwms = Some((Instant::now(), Ok(found)));
    let mut topic_of = BTreeMap::new();
    topic_of.insert("public.a", Some("cdc.public".to_string()));
    topic_of.insert("public.b", Some("cdc.public".to_string()));
    topic_of.insert("hr.x", Some("cdc.hr.x".to_string()));
    let (k, e) = kafka_entry(&hwms, Some("cdc.public"), &topic_of);
    assert_eq!(e, None);
    assert_eq!(
        k.unwrap(),
        json!({
            "hwm_total": 12,
            "partitions": [{ "partition": 0, "hwm": 5 }, { "partition": 1, "hwm": 7 }],
            "tables_on_topic": 2,
        })
    );
    let (k, e) = kafka_entry(&hwms, Some("cdc.hr.x"), &topic_of);
    assert!(k.is_none() && e.unwrap().contains("not found"));
    let (k, e) = kafka_entry(&hwms, None, &topic_of);
    assert!(k.is_none() && e.unwrap().contains("no route"));
    let failed = Some((Instant::now(), Err("boom".to_string())));
    assert_eq!(
        kafka_entry(&failed, Some("cdc.public"), &topic_of)
            .1
            .as_deref(),
        Some("boom")
    );
    // A content-routed table has no single topic: the card points at the Routing card,
    // whether or not offsets could be read.
    for h in [&hwms, &failed, &None] {
        assert_eq!(
            kafka_entry(h, Some("orders.{row.region}"), &topic_of),
            (None, Some(CONTENT_ROUTED.to_string()))
        );
    }
}

#[test]
fn prometheus_exports_last_commit_per_table() {
    let m = Metrics::new();
    assert!(!prometheus(&m.snapshot()).contains("aqueduct_table_last_commit"));
    m.record_event(
        &change("public", "we\"ird", aqueduct_core::Op::Insert, 1),
        Duration::from_millis(1),
    );
    let out = prometheus(&m.snapshot());
    assert!(
        out.contains(
            "aqueduct_table_last_commit_timestamp_seconds{table=\"public.we\\\"ird\"} 1700000000.25"
        ),
        "{out}"
    );
}

#[tokio::test]
async fn oversized_requests_are_dropped() {
    let addr = start(Some("k")).await;
    let mut c = TcpStream::connect(addr).await.unwrap();
    let big = "x".repeat(MAX_REQUEST + 1);
    let _ = c
        .write_all(
            format!(
                "POST /api/snapshot HTTP/1.1\r\ncontent-length: {}\r\n\r\n{big}",
                big.len()
            )
            .as_bytes(),
        )
        .await;
    let mut out = String::new();
    let _ = c.read_to_string(&mut out).await;
    assert!(out.is_empty(), "{out}");
}

/// `source_lag` (#150) is in the JSON of `/api/metrics` behind the token, with every key the
/// cockpit reads, and `unknown` until the source reports both positions.
#[tokio::test]
async fn source_lag_is_served_behind_the_token() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(Metrics::new());
    let admin = Admin::new(metrics.clone(), Some("k".into()), None);
    tokio::spawn(serve(listener, admin));
    let lag = json_body(&get_bearer(addr, "/api/metrics", "k").await)["source_lag"].clone();
    assert_eq!(lag["state"], "unknown");
    for key in [
        "backlog_bytes",
        "behind_secs",
        "ingest_rate",
        "process_rate",
        "eta_secs",
        "snapshot",
    ] {
        assert!(lag[key].is_null(), "{key}: {lag}");
    }
    assert_eq!(lag["threshold"]["backlog_bytes"], 1024 * 1024);
    assert_eq!(lag["threshold"]["stalled_after_secs"], 30);

    // 8 MiB of WAL behind the head.
    metrics.report_source_head_lsn(0x100_0000 + 8 * 1024 * 1024);
    metrics.report_source_delivered_lsn(0x100_0000);
    metrics.sample_source_lag();
    let v = json_body(&get_bearer(addr, "/api/metrics", "k").await);
    let lag = &v["source_lag"];
    assert_eq!(lag["backlog_bytes"], 8 * 1024 * 1024);
    assert_eq!(lag["rate_unit"], "bytes/s");
    // One sample: a backlog, but no rates yet, so the direction is not known.
    assert_eq!(lag["state"], "unknown", "{lag}");
    assert!(lag["head_age_secs"].as_f64().unwrap() < 3.0);
    assert_eq!(lag["history"].as_array().unwrap().len(), 2);
    assert_eq!(v["source_head_lsn"], "0/1800000");
    assert!(get(addr, "/api/metrics").await.starts_with("HTTP/1.1 401"));
}

#[test]
fn prometheus_exports_source_lag_only_when_known() {
    let m = Metrics::new();
    let out = prometheus(&m.snapshot());
    for name in [
        "aqueduct_source_backlog_bytes",
        "aqueduct_source_behind_seconds",
        "aqueduct_source_catch_up_eta_seconds",
        "aqueduct_snapshot_rows_per_second",
        "aqueduct_snapshot_eta_seconds",
    ] {
        assert!(!out.contains(name), "{name} without a value");
    }
    // Caught up: a few kB of heartbeat WAL.
    m.report_source_head_lsn(0x2000);
    m.report_source_delivered_lsn(0x1000);
    let out = prometheus(&m.snapshot());
    assert!(out.contains("# TYPE aqueduct_source_backlog_bytes gauge"));
    assert!(
        out.contains(
            "aqueduct_source_backlog_bytes 4096
"
        ),
        "{out}"
    );
    assert!(out.contains(
        "aqueduct_source_behind_seconds 0
"
    ));
    assert!(out.contains(
        "aqueduct_source_catch_up_eta_seconds 0
"
    ));
    assert!(!out.contains("aqueduct_snapshot_rows_per_second"));
    // A running snapshot without two seconds of samples has an estimate but no rate yet.
    m.set_snapshot(Some(aqueduct_core::SnapshotStatus {
        id: "s1".into(),
        state: "running".into(),
        tables: vec!["public.a".into()],
        tables_done: 0,
        table: Some("public.a".into()),
        rows: 10,
        rows_estimated: Some(100),
        skipped: vec![],
    }));
    let s = m.snapshot();
    assert_eq!(s.source_lag.snapshot.as_ref().unwrap().rows_left, Some(90));
    assert!(!prometheus(&s).contains("aqueduct_snapshot_rows_per_second"));
}

/// Sends `head` (request line and headers, without the blank line) and `body`, returns the
/// answer.
async fn raw(addr: std::net::SocketAddr, head: &str, body: &str) -> String {
    let mut c = TcpStream::connect(addr).await.unwrap();
    let req = format!("{head}\r\ncontent-length: {}\r\n\r\n{body}", body.len());
    c.write_all(req.as_bytes()).await.unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).await.unwrap();
    out
}

#[test]
fn parses_host_origin_and_content_type() {
    let r = parse(
        "POST /api/snapshot HTTP/1.1\r\nHost: 127.0.0.1:8080\r\nhost: evil\r\n\
         Origin: http://127.0.0.1:8080\r\nContent-Type: application/json; charset=utf-8\r\n\r\n{}",
    )
    .unwrap();
    assert_eq!(r.host.as_deref(), Some("127.0.0.1:8080"), "the first Host");
    assert_eq!(r.origin.as_deref(), Some("http://127.0.0.1:8080"));
    assert!(r.content_type.as_deref().is_some_and(is_json));
    assert!(!is_json("text/plain"));
    assert!(!is_json("application/jsonp"));
    for (authority, host) in [
        ("localhost:8080", "localhost"),
        ("localhost", "localhost"),
        ("[::1]:8080", "[::1]"),
        ("[::1]", "[::1]"),
        ("::1", "::1"),
        ("0.0.0.0:9000", "0.0.0.0"),
    ] {
        assert_eq!(host_name(authority), host, "{authority}");
    }
    assert!(same_origin("http://localhost:8080", Some("localhost:8080")));
    assert!(same_origin(
        "HTTPS://LocalHost:8080",
        Some("localhost:8080")
    ));
    assert!(!same_origin("http://evil.example", Some("localhost:8080")));
    assert!(!same_origin("null", Some("localhost:8080")));
    assert!(!same_origin("http://localhost:8080", None));
}

#[test]
fn without_a_token_only_ips_localhost_and_the_bind_host_are_accepted_as_host() {
    let admin = Admin::new(Arc::new(Metrics::new()), None, None);
    admin.set_health_addr("aqueduct.internal:8080");
    for ok in [
        "127.0.0.1:8080",
        "localhost:8080",
        "LOCALHOST",
        "[::1]:8080",
        "10.1.2.3:8080",
        "[fd00::1]:8080",
        "aqueduct.internal:8080",
    ] {
        assert!(admin.allowed_host(ok), "{ok}");
    }
    for bad in [
        "evil.example",
        "evil.example:8080",
        "localhost.evil.example:8080",
        "127.0.0.1.nip.io:8080",
        "",
    ] {
        assert!(!admin.allowed_host(bad), "{bad}");
    }
}

/// No token: a web page visited by the operator cannot use the API through the browser,
/// while the cockpit (same origin) and scripts (no `Origin`) still can.
#[tokio::test]
async fn without_a_token_cross_site_requests_are_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let admin = Admin::new(Arc::new(Metrics::new()), None, None);
    admin.set_health_addr(&addr.to_string());
    fake_source(&admin);
    tokio::spawn(serve(listener, admin));
    let me = format!("http://{addr}");
    let status = |r: &str| r.split("\r\n").next().unwrap_or_default().to_string();

    // DNS rebinding: the page's own name as Host.
    for path in ["/api/metrics", "/metrics", "/api/tables", "/api/snapshot"] {
        let r = raw(
            addr,
            &format!("GET {path} HTTP/1.1\r\nhost: evil.example:{}", addr.port()),
            "",
        )
        .await;
        assert!(r.starts_with("HTTP/1.1 403"), "{path}: {r}");
    }
    for host in [addr.to_string(), format!("localhost:{}", addr.port())] {
        let r = raw(
            addr,
            &format!("GET /api/metrics HTTP/1.1\r\nhost: {host}"),
            "",
        )
        .await;
        assert!(r.starts_with("HTTP/1.1 200"), "{host}: {r}");
    }
    // HTTP/1.0 without Host cannot come from a browser. The answer is in the request's version.
    assert!(
        raw(addr, "GET /api/metrics HTTP/1.0", "")
            .await
            .starts_with("HTTP/1.0 200")
    );
    // The page and the probes are not row data: served whatever the Host.
    for path in ["/", "/healthz"] {
        let r = raw(
            addr,
            &format!("GET {path} HTTP/1.1\r\nhost: evil.example"),
            "",
        )
        .await;
        assert!(r.starts_with("HTTP/1.1 200"), "{path}: {r}");
    }

    let host = format!("host: {addr}");
    // A cross-site read and a cross-site state change are refused.
    let r = raw(
        addr,
        &format!("GET /api/metrics HTTP/1.1\r\n{host}\r\norigin: http://evil.example"),
        "",
    )
    .await;
    assert!(r.starts_with("HTTP/1.1 403"), "{r}");
    for origin in ["http://evil.example", "null"] {
        let r = raw(
            addr,
            &format!("DELETE /api/snapshot HTTP/1.1\r\n{host}\r\norigin: {origin}"),
            "",
        )
        .await;
        assert!(r.starts_with("HTTP/1.1 403"), "{origin}: {r}");
    }
    // A no-cors POST can only be text/plain, form-encoded or multipart.
    let body = r#"{"tables":["public.a"]}"#;
    for ctype in [
        None,
        Some("text/plain"),
        Some("application/x-www-form-urlencoded"),
    ] {
        let head = match ctype {
            Some(c) => format!("POST /api/snapshot HTTP/1.1\r\n{host}\r\ncontent-type: {c}"),
            None => format!("POST /api/snapshot HTTP/1.1\r\n{host}"),
        };
        let r = raw(addr, &head, body).await;
        assert!(r.starts_with("HTTP/1.1 415"), "{ctype:?}: {r}");
    }
    // What the cockpit and a script send is accepted.
    let r = raw(
        addr,
        &format!("POST /api/snapshot HTTP/1.1\r\n{host}\r\norigin: {me}\r\ncontent-type: application/json; charset=utf-8"),
        body,
    )
    .await;
    assert!(r.starts_with("HTTP/1.1 202"), "{}", status(&r));
    let r = raw(
        addr,
        &format!("DELETE /api/snapshot HTTP/1.1\r\n{host}\r\norigin: {me}"),
        "",
    )
    .await;
    assert!(r.starts_with("HTTP/1.1 202"), "{r}");
    let r = raw(addr, &format!("GET /api/snapshot HTTP/1.1\r\n{host}"), "").await;
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
}

/// With a token the browser checks are not needed (a page cannot attach the token) and
/// change nothing: a reverse proxy may rewrite Host, and scripts may post without a type.
#[tokio::test]
async fn with_a_token_host_origin_and_content_type_are_not_checked() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let admin = Admin::new(Arc::new(Metrics::new()), Some("k".into()), None);
    fake_source(&admin);
    tokio::spawn(serve(listener, admin));
    let auth = "authorization: Bearer k";
    let r = raw(addr, &format!("GET /api/metrics HTTP/1.1\r\nhost: proxy.example\r\norigin: https://cockpit.example\r\n{auth}"), "").await;
    assert!(r.starts_with("HTTP/1.1 200"), "{r}");
    let r = raw(
        addr,
        &format!("POST /api/snapshot HTTP/1.1\r\nhost: proxy.example\r\n{auth}"),
        r#"{"tables":["public.a"]}"#,
    )
    .await;
    assert!(r.starts_with("HTTP/1.1 202"), "{r}");
    let r = raw(addr, "GET /api/metrics HTTP/1.1\r\nhost: proxy.example", "").await;
    assert!(r.starts_with("HTTP/1.1 401"), "{r}");
}

/// Every slot held by an idle connection: `/healthz` and `/readyz` still answer from the
/// reserve, other routes get `503`, and the idle connections are closed after the head
/// timeout.
#[tokio::test]
async fn health_probes_answer_while_every_slot_is_taken() {
    let addr = start(Some("k")).await;
    let mut idle = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        idle.push(TcpStream::connect(addr).await.unwrap());
    }
    // Let the server accept them all.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let began = Instant::now();
    assert!(get(addr, "/healthz").await.ends_with("ok"));
    assert!(get(addr, "/readyz").await.ends_with("not streaming"));
    let r = get_bearer(addr, "/api/metrics", "k").await;
    assert!(
        r.starts_with("HTTP/1.1 503") && r.ends_with("too many connections"),
        "{r}"
    );
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "{:?}",
        began.elapsed()
    );
    // The idle connections are dropped once the head timeout passes.
    let mut c = idle.pop().unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(HEAD_TIMEOUT + Duration::from_secs(2), c.read(&mut buf))
        .await
        .expect("idle connection still open")
        .unwrap_or(0);
    assert_eq!(n, 0);
    drop(idle);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        get_bearer(addr, "/api/metrics", "k")
            .await
            .starts_with("HTTP/1.1 200")
    );
}

/// A broker that accepts connections and never answers: `/api/events` gives up after the
/// timeout with `504`, a second request is not queued behind the first, and other routes
/// keep answering.
#[tokio::test]
async fn inspector_reads_time_out_on_a_stalled_broker() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_addr = broker.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = broker.accept().await {
            held.push(s);
        }
    });
    let stalled = KafkaInspector::new(
        vec![broker_addr.to_string()],
        &router(),
        aqueduct_sinks::KafkaSecurity::default(),
    );
    let addr = start_tables(Some(stalled)).await;
    let began = Instant::now();
    let first = tokio::spawn(get_bearer(addr, "/api/events?limit=5", "k"));
    let second = tokio::spawn(get_bearer(addr, "/api/events?limit=5", "k"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        get_bearer(addr, "/api/metrics", "k")
            .await
            .starts_with("HTTP/1.1 200")
    );
    for r in [first.await.unwrap(), second.await.unwrap()] {
        assert!(r.starts_with("HTTP/1.1 504"), "{r}");
        assert!(r.contains("Kafka did not answer within"), "{r}");
    }
    assert!(
        began.elapsed() < INSPECT_TIMEOUT + Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
}
