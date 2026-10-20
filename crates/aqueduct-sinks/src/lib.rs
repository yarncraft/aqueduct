//! The Kafka [`Sink`](aqueduct_core::Sink) (the only sink) and the Kafka-backed
//! event inspector. Events are encoded as one JSON object per record, in the shape of
//! Debezium's change event envelope ([`encode`]).
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod inspect;
mod kafka;
pub mod route;

pub use inspect::{InspectedEvent, Inspection, KafkaInspector};
pub use kafka::{
    glob_match, KafkaRoute, KafkaSasl, KafkaSecurity, KafkaSink, PartitionBy, SaslMechanism,
    TopicRouter,
};

use aqueduct_core::{ChangeEvent, Error, Op, Result};
use serde::Serialize;
use serde_json::value::RawValue;

/// Where events come from, for the envelope's `source` block and the record key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceInfo {
    /// `db` is the database the source is connected to.
    Postgres { db: String },
    /// Events carry the database in `schema` and the collection in `table`.
    Mongodb,
}

impl Default for SourceInfo {
    fn default() -> Self {
        Self::Postgres { db: String::new() }
    }
}

/// Debezium's single-letter operation codes.
pub fn op_code(op: Op) -> &'static str {
    match op {
        Op::Insert => "c",
        Op::Update => "u",
        Op::Delete => "d",
        Op::Truncate => "t",
        Op::Read => "r",
        // Heartbeats never reach the sink.
        Op::Heartbeat => "h",
    }
}

#[derive(Serialize)]
struct Envelope<'a> {
    before: Option<&'a RawValue>,
    after: Option<&'a RawValue>,
    source: Source<'a>,
    op: &'static str,
    ts_ms: i64,
    ts_us: i64,
}

#[derive(Serialize)]
struct Source<'a> {
    version: &'static str,
    connector: &'static str,
    ts_ms: i64,
    ts_us: i64,
    /// `incremental` for rows a snapshot read, `false` for changes (Debezium's values).
    snapshot: &'static str,
    db: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection: Option<&'a str>,
    #[serde(rename = "txId", skip_serializing_if = "Option::is_none")]
    tx_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lsn: Option<u64>,
}

/// Encodes `event` as a Debezium-style change event, without the schema part (as with
/// `value.converter.schemas.enable=false`):
///
/// ```json
/// {"before": {...}, "after": {...},
///  "source": {"version": "...", "connector": "postgresql", "ts_ms": ..., "ts_us": ...,
///             "snapshot": "false", "db": "app", "schema": "public", "table": "orders",
///             "txId": 512, "lsn": 24023128},
///  "op": "u", "ts_ms": ..., "ts_us": ...}
/// ```
///
/// `source.ts_*` is the commit time, the top-level `ts_*` when Aqueduct produced the record.
/// MongoDB events name `db` and `collection` and carry no `txId` or `lsn`.
pub fn encode(event: &ChangeEvent, info: &SourceInfo, now_us: i64) -> Result<String> {
    let commit_us = event.commit_ts_micros;
    let snapshot = if event.op == Op::Read {
        "incremental"
    } else {
        "false"
    };
    let source = match info {
        SourceInfo::Postgres { db } => Source {
            version: env!("CARGO_PKG_VERSION"),
            connector: "postgresql",
            ts_ms: commit_us.div_euclid(1000),
            ts_us: commit_us,
            snapshot,
            db,
            schema: Some(&event.schema),
            table: Some(&event.table),
            collection: None,
            tx_id: Some(event.xid),
            lsn: match &event.position {
                aqueduct_core::Position::Lsn(l) => Some(*l),
                aqueduct_core::Position::Text(_) => None,
            },
        },
        SourceInfo::Mongodb => Source {
            version: env!("CARGO_PKG_VERSION"),
            connector: "mongodb",
            ts_ms: commit_us.div_euclid(1000),
            ts_us: commit_us,
            snapshot,
            db: &event.schema,
            schema: None,
            table: None,
            collection: Some(&event.table),
            tx_id: None,
            lsn: None,
        },
    };
    serde_json::to_string(&Envelope {
        before: event.before.as_deref(),
        after: event.after.as_deref(),
        source,
        op: op_code(event.op),
        ts_ms: now_us.div_euclid(1000),
        ts_us: now_us,
    })
    .map_err(|e| Error::Sink(format!("encode: {e}")))
}

/// The record key: the row's primary key as a JSON object (Debezium's key), plus where the row
/// lives, so one topic can carry several tables:
///
/// `{"db": "app", "schema": "public", "table": "orders", "key": {"id": 42}}`, or for MongoDB
/// `{"db": "shop", "collection": "orders", "key": {"_id": ...}}`. `key` is `null` for a table
/// without a primary key and for a `TRUNCATE`.
pub fn record_key(event: &ChangeEvent, info: &SourceInfo) -> String {
    let key = event
        .key
        .as_deref()
        .filter(|k| k.trim_start().starts_with('{'))
        .unwrap_or("null");
    let s = |v: &str| serde_json::to_string(v).unwrap_or_default();
    match info {
        SourceInfo::Postgres { db } => format!(
            r#"{{"db":{},"schema":{},"table":{},"key":{key}}}"#,
            s(db),
            s(&event.schema),
            s(&event.table)
        ),
        SourceInfo::Mongodb => format!(
            r#"{{"db":{},"collection":{},"key":{key}}}"#,
            s(&event.schema),
            s(&event.table)
        ),
    }
}

#[cfg(test)]
pub(crate) fn sample() -> ChangeEvent {
    use aqueduct_core::{Op, Position};
    ChangeEvent {
        position: Position::Lsn(0x16_B374_D848),
        xid: 7,
        commit_ts_micros: 1_700_000_000_000_000,
        schema: "public".into(),
        table: "users".into(),
        op: Op::Insert,
        before: None,
        after: serde_json::value::RawValue::from_string(r#"{"id":1,"name":"ada"}"#.into()).ok(),
        key: Some(r#"{"id":1}"#.into()),
        checkpoint: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueduct_core::Position;

    #[test]
    fn postgres_events_have_the_debezium_envelope() {
        let pg = SourceInfo::Postgres { db: "app".into() };
        let v: serde_json::Value =
            serde_json::from_str(&encode(&sample(), &pg, 1_700_000_000_500_000).unwrap()).unwrap();
        assert_eq!(v["op"], "c");
        assert_eq!(v["before"], serde_json::Value::Null);
        assert_eq!(v["after"]["name"], "ada");
        assert_eq!(v["ts_ms"], 1_700_000_000_500i64);
        let src = &v["source"];
        assert_eq!(src["connector"], "postgresql");
        assert_eq!(
            (
                src["db"].as_str(),
                src["schema"].as_str(),
                src["table"].as_str()
            ),
            (Some("app"), Some("public"), Some("users"))
        );
        assert_eq!(src["txId"], 7);
        assert_eq!(src["lsn"], 0x16_B374_D848u64);
        assert_eq!(src["ts_us"], 1_700_000_000_000_000i64);
        assert_eq!(src["ts_ms"], 1_700_000_000_000i64);
        assert_eq!(src["snapshot"], "false");
        assert!(src.get("collection").is_none());
        // Every op maps to Debezium's letter, and snapshot rows say so.
        for (op, code) in [
            (Op::Update, "u"),
            (Op::Delete, "d"),
            (Op::Truncate, "t"),
            (Op::Read, "r"),
        ] {
            let mut e = sample();
            e.op = op;
            let v: serde_json::Value = serde_json::from_str(&encode(&e, &pg, 0).unwrap()).unwrap();
            assert_eq!(v["op"], code);
            assert_eq!(
                v["source"]["snapshot"],
                if op == Op::Read {
                    "incremental"
                } else {
                    "false"
                }
            );
        }
    }

    #[test]
    fn mongodb_events_name_db_and_collection() {
        let mut e = sample();
        e.schema = "shop".into();
        e.table = "orders".into();
        e.position = Position::Text("{\"_data\":\"82\"}".into());
        let v: serde_json::Value =
            serde_json::from_str(&encode(&e, &SourceInfo::Mongodb, 0).unwrap()).unwrap();
        let src = &v["source"];
        assert_eq!(
            (
                src["connector"].as_str(),
                src["db"].as_str(),
                src["collection"].as_str()
            ),
            (Some("mongodb"), Some("shop"), Some("orders"))
        );
        assert!(
            src.get("txId").is_none() && src.get("lsn").is_none() && src.get("table").is_none()
        );
    }

    #[test]
    fn record_keys_carry_the_primary_key_and_the_table() {
        let pg = SourceInfo::Postgres { db: "app".into() };
        assert_eq!(
            record_key(&sample(), &pg),
            r#"{"db":"app","schema":"public","table":"users","key":{"id":1}}"#
        );
        let mut e = sample();
        e.key = None;
        assert_eq!(
            record_key(&e, &pg),
            r#"{"db":"app","schema":"public","table":"users","key":null}"#
        );
        e.schema = "shop".into();
        e.table = "or\"ders".into();
        e.key = Some(r#"{"_id":{"$oid":"6530"}}"#.into());
        assert_eq!(
            record_key(&e, &SourceInfo::Mongodb),
            r#"{"db":"shop","collection":"or\"ders","key":{"_id":{"$oid":"6530"}}}"#
        );
    }
}
