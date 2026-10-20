//! End-to-end Kafka sink test. Needs a broker, so it is skipped unless
//! `AQUEDUCT_KAFKA_BROKERS` is set (e.g. `localhost:9092`).

use aqueduct_core::{ChangeEvent, Op, Position, Sink};
use aqueduct_sinks::{
    FieldSpec, HashKey, KafkaInspector, KafkaRoute, KafkaSecurity, KafkaSink, RoutePlan,
};
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use serde_json::value::to_raw_value;

fn ev(n: u64, table: &str) -> ChangeEvent {
    ChangeEvent {
        position: Position::Lsn(n),
        xid: 1,
        commit_ts_micros: 1_700_000_000_000_000 + n as i64,
        schema: "public".into(),
        table: table.into(),
        op: Op::Insert,
        before: None,
        after: Some(to_raw_value(&serde_json::json!({ "n": n })).unwrap()),
        key: None,
        checkpoint: None,
    }
}

/// A topic name no earlier run used: the process id alone repeats in containers (often 1).
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

#[tokio::test]
async fn produces_all_events_in_per_table_order() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let topic = unique("aqueduct-it");

    let router = RoutePlan::new(vec![], Some(topic.clone())).unwrap();
    let inspector = KafkaInspector::new(brokers.clone(), &router, KafkaSecurity::default());
    let mut sink = KafkaSink::connect(brokers.clone(), router, 3, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let tables = ["users", "orders", "items"];
    let total = 300u64;
    for n in 1..=total {
        sink.send(&ev(n, tables[(n % 3) as usize])).await.unwrap();
    }
    sink.flush().await.unwrap();

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let mut seen: Vec<(String, u64, String)> = Vec::new(); // (table, lsn, partition)
    for p in 0..3 {
        let pc = client
            .partition_client(&topic, p, UnknownTopicHandling::Error)
            .await
            .unwrap();
        let end = pc.get_offset(OffsetAt::Latest).await.unwrap();
        let mut offset = 0;
        while offset < end {
            let (recs, _hw) = pc.fetch_records(offset, 1..1_000_000, 1_000).await.unwrap();
            if recs.is_empty() {
                break;
            }
            for r in recs {
                offset = r.offset + 1;
                let key = String::from_utf8(r.record.key.unwrap()).unwrap();
                let lsn: u64 = String::from_utf8(r.record.headers["lsn"].clone())
                    .unwrap()
                    .parse()
                    .unwrap();
                let back: serde_json::Value =
                    serde_json::from_slice(&r.record.value.unwrap()).unwrap();
                assert_eq!(back["source"]["lsn"], lsn, "Debezium-style envelope");
                assert_eq!(back["op"], "c");
                let k: serde_json::Value = serde_json::from_str(&key).unwrap();
                let table = format!(
                    "{}.{}",
                    k["schema"].as_str().unwrap(),
                    k["table"].as_str().unwrap()
                );
                assert_eq!(
                    k["key"],
                    serde_json::Value::Null,
                    "no primary key in these events"
                );
                seen.push((table, lsn, p.to_string()));
            }
        }
    }

    assert_eq!(seen.len() as u64, total, "every event must be consumable");
    for t in tables {
        let key = format!("public.{t}");
        let rows: Vec<_> = seen.iter().filter(|s| s.0 == key).collect();
        assert_eq!(rows.len() as u64, total / 3);
        assert!(
            rows.windows(2).all(|w| w[0].1 < w[1].1),
            "{key} out of order"
        );
        assert!(
            rows.iter().all(|r| r.2 == rows[0].2),
            "{key} split across partitions"
        );
    }

    // The cockpit's high-water marks: one per partition, summing to every record produced.
    // A topic the sink cannot write to is never read.
    let hwms = inspector
        .high_water_marks(&[topic.clone(), "__consumer_offsets".into()])
        .await
        .unwrap();
    assert_eq!(hwms.keys().collect::<Vec<_>>(), [&topic]);
    let parts = &hwms[&topic];
    assert_eq!(parts.iter().map(|p| p.0).collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(parts.iter().map(|p| p.1).sum::<i64>(), total as i64);
}

/// Every record of `topic` across its partitions: (key, headers, value).
async fn read_all(
    client: &rskafka::client::Client,
    topic: &str,
) -> Vec<(
    String,
    std::collections::BTreeMap<String, Vec<u8>>,
    serde_json::Value,
)> {
    let parts = client
        .list_topics()
        .await
        .unwrap()
        .into_iter()
        .find(|t| t.name == topic)
        .unwrap_or_else(|| panic!("topic {topic} does not exist"))
        .partitions;
    let mut out = Vec::new();
    for p in parts {
        let pc = client
            .partition_client(topic, p, UnknownTopicHandling::Error)
            .await
            .unwrap();
        let end = pc.get_offset(OffsetAt::Latest).await.unwrap();
        let mut offset = 0;
        while offset < end {
            let (recs, _hw) = pc.fetch_records(offset, 1..1_000_000, 1_000).await.unwrap();
            if recs.is_empty() {
                break;
            }
            for r in recs {
                offset = r.offset + 1;
                out.push((
                    String::from_utf8(r.record.key.unwrap()).unwrap(),
                    r.record.headers,
                    serde_json::from_slice(&r.record.value.unwrap()).unwrap(),
                ));
            }
        }
    }
    out
}

#[tokio::test]
async fn filtered_rows_are_absent_and_type_errors_go_to_the_dead_letter_topic() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let topic = unique("aqueduct-it-filter");
    let dlq = unique("aqueduct-it-dlq");
    let route = KafkaRoute {
        name: Some("big".into()),
        pattern: "public.*".into(),
        topic: topic.clone(),
        ops: Some(vec!["insert".into()]),
        filter: Some("row.n > 10".into()),
        ..Default::default()
    };
    let plan = RoutePlan::with_dead_letter(vec![route], None, Some(dlq.clone())).unwrap();
    let stats = plan.stats();
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 2, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let client = ClientBuilder::new(brokers).build().await.unwrap();
    // The dead-letter topic is created at connect, before any row needs it.
    assert!(
        client
            .list_topics()
            .await
            .unwrap()
            .iter()
            .any(|t| t.name == dlq),
        "dead-letter topic opened at startup"
    );

    let mut batch: Vec<ChangeEvent> = (1..=20).map(|n| ev(n, "orders")).collect();
    let mut bad = ev(21, "orders");
    bad.after = Some(to_raw_value(&serde_json::json!({ "n": "not-a-number" })).unwrap());
    bad.key = Some(r#"{"id":21}"#.into());
    batch.push(bad);
    let mut update = ev(22, "orders");
    update.op = Op::Update;
    batch.push(update);
    // One batch, acked as a whole: the dead-letter record is produced before send_batch returns.
    sink.send_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();

    let kept = read_all(&client, &topic).await;
    let mut ns: Vec<u64> = kept
        .iter()
        .map(|(_, _, v)| v["after"]["n"].as_u64().unwrap())
        .collect();
    ns.sort_unstable();
    assert_eq!(
        ns,
        (11..=20).collect::<Vec<_>>(),
        "rows with n <= 10 and the update are filtered"
    );

    let dead = read_all(&client, &dlq).await;
    assert_eq!(dead.len(), 1, "one type-error row");
    let (key, headers, value) = &dead[0];
    assert_eq!(
        String::from_utf8(headers["aqueduct-error"].clone()).unwrap(),
        "route `big`: where `row.n > 10`: type error: `row.n` is not a finite number"
    );
    assert_eq!(headers["op"], b"c");
    assert_eq!(headers["lsn"], b"21");
    let k: serde_json::Value = serde_json::from_str(key).unwrap();
    assert_eq!(k["table"], "orders");
    assert_eq!(k["key"], serde_json::Value::Null, "no row data in the key");
    assert_eq!(
        value["after"]["n"], "not-a-number",
        "the value is the envelope"
    );

    let c = stats.snapshot()[0].counts;
    assert_eq!(
        (c.matched, c.produced, c.filtered, c.errors),
        (22, 10, 11, 1)
    );
}

#[tokio::test]
async fn route_key_keeps_each_rows_changes_on_one_partition() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let topic = unique("aqueduct-it-key");
    let route = KafkaRoute {
        pattern: "public.*".into(),
        topic: topic.clone(),
        key: Some(vec!["id".into()]),
        ..Default::default()
    };
    let plan = RoutePlan::new(vec![route], None).unwrap();
    // `partition_by = "table"` would put every record on one partition; the route key overrides it.
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 4, None, &KafkaSecurity::default())
        .await
        .unwrap()
        .with_partition_by(aqueduct_sinks::PartitionBy::Table);
    let mut batch = Vec::new();
    for id in 0..40u64 {
        let mut ins = ev(2 * id + 1, "orders");
        ins.after = Some(to_raw_value(&serde_json::json!({ "id": id, "n": 1 })).unwrap());
        let mut del = ev(2 * id + 2, "orders");
        del.op = Op::Delete;
        del.after = None;
        // A delete's before-image under the default replica identity: the key columns only.
        del.before = Some(to_raw_value(&serde_json::json!({ "id": id })).unwrap());
        batch.push(ins);
        batch.push(del);
    }
    sink.send_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    // id -> (partition, op) of each record, in partition order.
    let mut by_id: std::collections::BTreeMap<u64, Vec<(i32, String)>> = Default::default();
    for p in 0..4 {
        let pc = client
            .partition_client(&topic, p, UnknownTopicHandling::Error)
            .await
            .unwrap();
        let end = pc.get_offset(OffsetAt::Latest).await.unwrap();
        let mut offset = 0;
        while offset < end {
            let (recs, _hw) = pc.fetch_records(offset, 1..1_000_000, 1_000).await.unwrap();
            if recs.is_empty() {
                break;
            }
            for r in recs {
                offset = r.offset + 1;
                let k: serde_json::Value = serde_json::from_slice(&r.record.key.unwrap()).unwrap();
                assert_eq!(k["table"], "orders");
                let id = k["key"]["id"].as_u64().unwrap();
                assert_eq!(
                    k["key"],
                    serde_json::json!({ "id": id }),
                    "only the key columns"
                );
                let op = String::from_utf8(r.record.headers["op"].clone()).unwrap();
                by_id.entry(id).or_default().push((p, op));
            }
        }
    }
    assert_eq!(by_id.len(), 40);
    for (id, recs) in &by_id {
        assert_eq!(
            recs.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
            ["c", "d"],
            "row {id}: insert then delete"
        );
        assert_eq!(recs[0].0, recs[1].0, "row {id} split across partitions");
    }
    let used: std::collections::HashSet<i32> = by_id.values().map(|r| r[0].0).collect();
    assert!(
        used.len() > 1,
        "rows spread over partitions despite partition_by = table"
    );
}

#[tokio::test]
async fn hashed_columns_reach_the_topic_with_the_key_id_header() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let topic = unique("aqueduct-it-hash");
    let secret = b"0123456789abcdef0123456789abcdef";
    let route = KafkaRoute {
        name: Some("users".into()),
        pattern: "public.users".into(),
        topic: topic.clone(),
        fields: FieldSpec {
            hash: vec!["email".into()],
            mask: vec!["card".into()],
            ..Default::default()
        },
        ..Default::default()
    };
    let plan = RoutePlan::build(
        vec![route],
        None,
        None,
        Some(HashKey::new(secret, "2026-10").unwrap()),
    )
    .unwrap();
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 2, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let mut e = ev(1, "users");
    e.after = Some(
        to_raw_value(&serde_json::json!({ "email": "ada@example.com", "card": "4111", "n": 1 }))
            .unwrap(),
    );
    e.key = Some(r#"{"email":"ada@example.com"}"#.into());
    sink.send_batch(&[e]).await.unwrap();
    sink.flush().await.unwrap();

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let got = read_all(&client, &topic).await;
    assert_eq!(got.len(), 1);
    let (key, headers, value) = &got[0];
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
    let digest: String = ring::hmac::sign(&k, b"ada@example.com")
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(value["after"]["email"], digest.as_str());
    assert_eq!(value["after"]["card"], "***");
    assert_eq!(value["after"]["n"], 1);
    let k: serde_json::Value = serde_json::from_str(key).unwrap();
    assert_eq!(k["key"]["email"], digest.as_str(), "the key is hashed too");
    assert!(!key.contains("ada@") && !value.to_string().contains("ada@"));
    assert_eq!(headers["aqueduct-hash-key-id"], b"2026-10");
}
