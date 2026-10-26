//! `/metrics`: the Prometheus text exposition (also pushed over OTLP, see `telemetry.rs`).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use aqueduct_core::{Snapshot, State};
use aqueduct_sinks::{OpCounts, RouteCount};

pub(super) fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(super) fn metric(o: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(o, "# HELP {name} {help}");
    let _ = writeln!(o, "# TYPE {name} {kind}");
}

/// Prometheus text exposition format (version 0.0.4).
pub(super) fn prometheus(s: &Snapshot) -> String {
    let mut o = String::new();
    metric(
        &mut o,
        "aqueduct_events_total",
        "counter",
        "Events delivered to the sink, by operation.",
    );
    for (op, n) in [
        ("insert", s.inserts),
        ("update", s.updates),
        ("delete", s.deletes),
        ("truncate", s.truncates),
        ("read", s.reads),
    ] {
        let _ = writeln!(o, "aqueduct_events_total{{op=\"{op}\"}} {n}");
    }
    metric(
        &mut o,
        "aqueduct_table_events_total",
        "counter",
        "Events delivered, by table.",
    );
    for (t, n) in &s.tables {
        let _ = writeln!(
            o,
            "aqueduct_table_events_total{{table=\"{}\"}} {n}",
            escape_label(t)
        );
    }
    if s.table_marks.values().any(|m| m.last_commit_ms.is_some()) {
        metric(
            &mut o,
            "aqueduct_table_last_commit_timestamp_seconds",
            "gauge",
            "Commit time of the newest change delivered, by table (Unix time).",
        );
        for (t, m) in &s.table_marks {
            if let Some(ms) = m.last_commit_ms {
                let _ = writeln!(
                    o,
                    "aqueduct_table_last_commit_timestamp_seconds{{table=\"{}\"}} {}",
                    escape_label(t),
                    ms as f64 / 1000.0
                );
            }
        }
    }
    metric(
        &mut o,
        "aqueduct_errors_total",
        "counter",
        "Pipeline errors.",
    );
    let _ = writeln!(o, "aqueduct_errors_total {}", s.errors);
    metric(
        &mut o,
        "aqueduct_state",
        "gauge",
        "1 for the current pipeline state.",
    );
    for st in [
        State::Starting,
        State::Streaming,
        State::Stopped,
        State::Failed,
    ] {
        let name = format!("{st:?}").to_lowercase();
        let _ = writeln!(
            o,
            "aqueduct_state{{state=\"{name}\"}} {}",
            u8::from(st == s.state)
        );
    }
    metric(
        &mut o,
        "aqueduct_uptime_seconds",
        "gauge",
        "Process uptime.",
    );
    let _ = writeln!(o, "aqueduct_uptime_seconds {}", s.uptime_secs);
    if let Some(lag) = s.lag_ms {
        metric(
            &mut o,
            "aqueduct_lag_seconds",
            "gauge",
            "Commit time to delivery time of the newest event.",
        );
        let _ = writeln!(o, "aqueduct_lag_seconds {}", lag.max(0) as f64 / 1000.0);
    }
    if let Some(lag) = s.slot_lag_bytes {
        metric(
            &mut o,
            "aqueduct_slot_lag_bytes",
            "gauge",
            "WAL between the server head and the slot's confirmed_flush_lsn.",
        );
        let _ = writeln!(o, "aqueduct_slot_lag_bytes {lag}");
    }
    source_lag_metrics(&mut o, s);
    if let Some(st) = &s.snapshot {
        for (name, kind, help, value) in [
            (
                "aqueduct_snapshot_running",
                "gauge",
                "1 while a table snapshot runs.",
                u64::from(st.state == "running"),
            ),
            (
                "aqueduct_snapshot_rows_total",
                "counter",
                "Rows copied by the current or last snapshot in this process.",
                st.rows,
            ),
            (
                "aqueduct_snapshot_tables_total",
                "gauge",
                "Tables in the current or last snapshot.",
                st.tables.len() as u64,
            ),
            (
                "aqueduct_snapshot_tables_done",
                "gauge",
                "Tables of the current or last snapshot fully copied.",
                st.tables_done as u64,
            ),
        ] {
            metric(&mut o, name, kind, help);
            let _ = writeln!(o, "{name} {value}");
        }
        if let Some(n) = st.rows_estimated {
            metric(
                &mut o,
                "aqueduct_snapshot_rows_estimated",
                "gauge",
                "Planner estimate of the rows the snapshot copies.",
            );
            let _ = writeln!(o, "aqueduct_snapshot_rows_estimated {n}");
        }
    }
    if let Some(hb) = &s.heartbeat {
        metric(
            &mut o,
            "aqueduct_heartbeat_skipped_total",
            "counter",
            "Idle heartbeats skipped because a query failed.",
        );
        let _ = writeln!(o, "aqueduct_heartbeat_skipped_total {}", hb.skipped_total);
        if let Some(ms) = hb.last_ms {
            metric(
                &mut o,
                "aqueduct_heartbeat_last_timestamp_seconds",
                "gauge",
                "When the last idle heartbeat completed (Unix time).",
            );
            let _ = writeln!(
                o,
                "aqueduct_heartbeat_last_timestamp_seconds {}",
                ms as f64 / 1000.0
            );
        }
    }
    if let Some(ms) = s.last_event_unix_ms {
        metric(
            &mut o,
            "aqueduct_last_event_timestamp_seconds",
            "gauge",
            "When the newest event was delivered (Unix time).",
        );
        let _ = writeln!(
            o,
            "aqueduct_last_event_timestamp_seconds {}",
            ms as f64 / 1000.0
        );
    }
    metric(
        &mut o,
        "aqueduct_sink_latency_seconds",
        "gauge",
        "Sink delivery latency per event (mean, last, max).",
    );
    for (k, v) in [
        ("avg", s.sink_avg_ms),
        ("last", s.sink_last_ms),
        ("max", s.sink_max_ms),
    ] {
        let _ = writeln!(
            o,
            "aqueduct_sink_latency_seconds{{stat=\"{k}\"}} {}",
            v / 1000.0
        );
    }
    o
}

/// "Behind source" (#150): one gauge per number that is known; an unknown one is left out.
pub(super) fn source_lag_metrics(o: &mut String, s: &Snapshot) {
    let lag = &s.source_lag;
    let snap = lag.snapshot.as_ref();
    for (name, help, value) in [
        (
            "aqueduct_source_backlog_bytes",
            "WAL bytes between the source's head and the position delivered (Postgres).",
            lag.backlog_bytes.map(|b| b as f64),
        ),
        (
            "aqueduct_source_behind_seconds",
            "How far delivery trails the source, in seconds; 0 when caught up.",
            lag.behind_secs,
        ),
        (
            "aqueduct_source_catch_up_eta_seconds",
            "Seconds until the backlog is gone at the current rates; 0 when caught up.",
            lag.eta_secs,
        ),
        (
            "aqueduct_snapshot_rows_per_second",
            "Rows the running snapshot copies per second, over the last minute.",
            snap.and_then(|r| r.rows_per_sec),
        ),
        (
            "aqueduct_snapshot_eta_seconds",
            "Seconds until the running snapshot has copied its estimated rows.",
            snap.and_then(|r| r.eta_secs),
        ),
    ] {
        if let Some(v) = value {
            metric(o, name, "gauge", help);
            let _ = writeln!(o, "{name} {v}");
        }
    }
}

/// Records the sink got acknowledged per topic (`(other)` past the tracked-topic cap), all
/// operations together.
pub(super) fn topic_metrics(o: &mut String, topics: &BTreeMap<String, OpCounts>) {
    if topics.is_empty() {
        return;
    }
    metric(
        o,
        "aqueduct_topic_records_total",
        "counter",
        "Records Kafka acknowledged, by topic.",
    );
    for (t, n) in topics {
        let _ = writeln!(
            o,
            "aqueduct_topic_records_total{{topic=\"{}\"}} {}",
            escape_label(t),
            n.total()
        );
    }
}

/// Per-route Prometheus series. `matched` is not exported: it is the sum of the four outcomes.
pub(super) fn route_metrics(o: &mut String, routes: &[RouteCount]) {
    if routes.is_empty() {
        return;
    }
    metric(
        o,
        "aqueduct_route_records_total",
        "counter",
        "Events a route claimed, by outcome; the sum over outcomes is the route's matched count.",
    );
    for r in routes {
        let route = escape_label(&r.route);
        let c = r.counts;
        for (outcome, n) in [
            ("produced", c.produced),
            ("filtered", c.filtered),
            ("fallback", c.fallback),
            ("errors", c.errors),
        ] {
            let _ = writeln!(
                o,
                "aqueduct_route_records_total{{route=\"{route}\",outcome=\"{outcome}\"}} {n}"
            );
        }
    }
    metric(
        o,
        "aqueduct_route_tombstones_skipped_total",
        "counter",
        "Deletes a route with tombstones = true produced without a tombstone, because the row has no key.",
    );
    for r in routes {
        let _ = writeln!(
            o,
            "aqueduct_route_tombstones_skipped_total{{route=\"{}\"}} {}",
            escape_label(&r.route),
            r.counts.tombstones_skipped
        );
    }
    metric(
        o,
        "aqueduct_route_passed_on_total",
        "counter",
        "Events a route's where rejected and passed on to its otherwise route; part of its filtered count.",
    );
    for r in routes {
        let _ = writeln!(
            o,
            "aqueduct_route_passed_on_total{{route=\"{}\"}} {}",
            escape_label(&r.route),
            r.counts.passed_on
        );
    }
    metric(
        o,
        "aqueduct_route_field_unseen",
        "gauge",
        "1 for a field path a route references that no row has had yet (a typo?).",
    );
    for r in routes {
        for f in &r.unseen_fields {
            let _ = writeln!(
                o,
                "aqueduct_route_field_unseen{{route=\"{}\",field=\"{}\"}} 1",
                escape_label(&r.route),
                escape_label(f)
            );
        }
    }
}
