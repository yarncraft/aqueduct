//! End-to-end Kafka sink test. Needs a broker, so it is skipped unless
//! `AQUEDUCT_KAFKA_BROKERS` is set (e.g. `localhost:9092`).

use aqueduct_core::{ChangeEvent, Op, Position, Sink};
use aqueduct_sinks::{
    FieldSpec, HashKey, KafkaInspector, KafkaRoute, KafkaSecurity, KafkaSink, OutboxSpec, RoutePlan,
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
        unchanged: Vec::new(),
        after_is_key: false,
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
    // The cockpit's per-topic rate counts every record once the broker acknowledged it.
    let acked = sink.topic_stats().snapshot();
    assert_eq!(acked.get(&topic).map(|o| o.total()), Some(total));
    // Per operation: every event was an insert.
    assert_eq!(acked[&topic].slots(), [total, 0, 0, 0, 0]);
    assert_eq!(acked.len(), 1, "{acked:?}");

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

/// One batch whose records for a single partition total far more than the broker's default
/// `message.max.bytes` (1 MiB): the sink splits it into several requests, so the batch succeeds
/// instead of being refused on every retry, and every record lands once, in order.
#[tokio::test]
async fn a_batch_larger_than_one_request_lands_whole_and_in_order() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let topic = unique("aqueduct-it-big");
    let router = RoutePlan::new(vec![], Some(topic.clone())).unwrap();
    // One partition: every record of the batch goes to the same partition.
    let mut sink = KafkaSink::connect(brokers.clone(), router, 1, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let total = 1_000u64;
    let pad = "x".repeat(2_000);
    let batch: Vec<ChangeEvent> = (1..=total)
        .map(|n| {
            let mut e = ev(n, "big");
            e.after = Some(to_raw_value(&serde_json::json!({ "n": n, "pad": pad })).unwrap());
            e
        })
        .collect();
    sink.send_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    assert_eq!(
        sink.topic_stats().snapshot().get(&topic).map(|o| o.total()),
        Some(total)
    );

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let pc = client
        .partition_client(&topic, 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let end = pc.get_offset(OffsetAt::Latest).await.unwrap();
    assert_eq!(end, total as i64, "exactly one record per event");
    let mut lsns = Vec::new();
    let mut offset = 0;
    while offset < end {
        let (recs, _hw) = pc.fetch_records(offset, 1..4_000_000, 1_000).await.unwrap();
        if recs.is_empty() {
            break;
        }
        for r in recs {
            offset = r.offset + 1;
            let lsn: u64 = String::from_utf8(r.record.headers["lsn"].clone())
                .unwrap()
                .parse()
                .unwrap();
            lsns.push(lsn);
        }
    }
    assert_eq!(lsns, (1..=total).collect::<Vec<_>>(), "in batch order");
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
    // No `dead_letter_topic` anywhere: the route's topic plus `.dlq`.
    let dlq = format!("{topic}.dlq");
    let route = KafkaRoute {
        name: Some("big".into()),
        pattern: "public.*".into(),
        topic: topic.clone(),
        filter: Some("op == 'insert' and row.n > 10".into()),
        ..Default::default()
    };
    // A `{table}` template: its dead-letter topics are rendered per event, created on first use.
    let prefix = unique("aqueduct-it-tpl");
    let templated = KafkaRoute {
        name: Some("audit".into()),
        pattern: "audit.*".into(),
        topic: format!("{prefix}.{{table}}"),
        filter: Some("row.n > 10".into()),
        ..Default::default()
    };
    let tpl_dlq = format!("{prefix}.log.dlq");
    let plan = RoutePlan::new(vec![route, templated], None).unwrap();
    assert_eq!(plan.dead_letter_topics(), std::slice::from_ref(&dlq));
    let stats = plan.stats();
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 2, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let client = ClientBuilder::new(brokers).build().await.unwrap();
    // The fixed dead-letter topic is created at connect, before any row needs it; the rendered
    // one is not.
    let listed = client.list_topics().await.unwrap();
    assert!(
        listed.iter().any(|t| t.name == dlq),
        "dead-letter topic opened at startup"
    );
    assert!(!listed.iter().any(|t| t.name == tpl_dlq));

    let mut batch: Vec<ChangeEvent> = (1..=20).map(|n| ev(n, "orders")).collect();
    let mut bad = ev(21, "orders");
    bad.after = Some(to_raw_value(&serde_json::json!({ "n": "not-a-number" })).unwrap());
    bad.key = Some(r#"{"id":21}"#.into());
    batch.push(bad);
    let mut update = ev(22, "orders");
    update.op = Op::Update;
    batch.push(update);
    let mut audit = ev(23, "log");
    audit.schema = "audit".into();
    audit.after = Some(to_raw_value(&serde_json::json!({ "n": [1] })).unwrap());
    batch.push(audit);
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
        "route `big`: where `op == 'insert' and row.n > 10`: type error: `row.n` is not a finite number"
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

    // The templated route's bad row landed on its own rendered dead-letter topic.
    let dead = read_all(&client, &tpl_dlq).await;
    assert_eq!(dead.len(), 1, "one type-error row on {tpl_dlq}");
    assert!(String::from_utf8(dead[0].1["aqueduct-error"].clone())
        .unwrap()
        .starts_with("route `audit`: where `row.n > 10`: type error"));

    let snap = stats.snapshot();
    let c = snap[0].counts;
    assert_eq!(
        (c.matched, c.produced, c.filtered, c.errors),
        (22, 10, 11, 1)
    );
    assert_eq!(snap[1].counts.errors, 1);
    // Produced per operation: the ten kept inserts (the update was filtered).
    assert_eq!(c.ops.slots(), [10, 0, 0, 0, 0]);
    // Acknowledged records per topic and op: the kept rows, and each dead-lettered insert on its
    // topic.
    let acked = sink.topic_stats().snapshot();
    assert_eq!(acked[&topic].slots(), [10, 0, 0, 0, 0]);
    assert_eq!(acked[&dlq].slots(), [1, 0, 0, 0, 0]);
    assert_eq!(acked[&tpl_dlq].slots(), [1, 0, 0, 0, 0]);
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
async fn content_routing_cap_counts_broker_topics_across_restarts() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let prefix = unique("aqueduct-it-content");
    let fallback = format!("{prefix}-fallback");
    let plan = || {
        let route = KafkaRoute {
            pattern: "public.*".into(),
            topic: format!("{prefix}.{{row.region}}"),
            topic_fallback: Some(fallback.clone()),
            ..Default::default()
        };
        let mut p = RoutePlan::new(vec![route], None).unwrap();
        p.set_max_dynamic_topics(2);
        p
    };
    let region = |n: u64, r: &str| {
        let mut e = ev(n, "orders");
        e.after = Some(to_raw_value(&serde_json::json!({ "n": n, "region": r })).unwrap());
        e
    };

    // First run: two topics fit under the cap, the third region goes to the fallback.
    let p1 = plan();
    let stats1 = p1.stats();
    let mut sink = KafkaSink::connect(brokers.clone(), p1, 1, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let batch = [
        region(1, "eu"),
        region(2, "us"),
        region(3, "apac"),
        region(4, "eu"),
    ];
    sink.send_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    drop(sink);
    let c = stats1.snapshot()[0].counts;
    assert_eq!((c.produced, c.fallback), (3, 1));

    // Restart: a new sink on the same broker counts the two topics it finds, so a new region
    // still goes to the fallback, while an existing one is used.
    let p2 = plan();
    let stats2 = p2.stats();
    let mut sink = KafkaSink::connect(brokers.clone(), p2, 1, None, &KafkaSecurity::default())
        .await
        .unwrap();
    assert_eq!(
        sink.plan().dynamic_topic_count(),
        2,
        "counted from the broker"
    );
    sink.send_batch(&[region(5, "latam"), region(6, "us")])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    let c = stats2.snapshot()[0].counts;
    assert_eq!((c.produced, c.fallback), (1, 1));

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let mut topics: Vec<String> = client
        .list_topics()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .filter(|t| t.starts_with(&prefix))
        .collect();
    topics.sort();
    assert_eq!(
        topics,
        [
            format!("{prefix}-fallback"),
            format!("{prefix}.eu"),
            format!("{prefix}.us")
        ]
    );
    let lsns = |recs: Vec<(
        String,
        std::collections::BTreeMap<String, Vec<u8>>,
        serde_json::Value,
    )>| {
        let mut v: Vec<u64> = recs
            .iter()
            .map(|r| r.2["after"]["n"].as_u64().unwrap())
            .collect();
        v.sort_unstable();
        v
    };
    assert_eq!(lsns(read_all(&client, &fallback).await), [3, 5]);
    assert_eq!(
        lsns(read_all(&client, &format!("{prefix}.eu")).await),
        [1, 4]
    );
    assert_eq!(
        lsns(read_all(&client, &format!("{prefix}.us")).await),
        [2, 6]
    );
}

#[tokio::test]
async fn content_topics_kafka_would_refuse_go_to_the_fallback() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let prefix = unique("aqueduct-it-collide");
    let fallback = format!("{prefix}-fallback");
    let client = ClientBuilder::new(brokers.clone()).build().await.unwrap();
    let ctrl = client.controller_client().unwrap();
    // `<prefix>_eu` exists before the sink starts: `<prefix>.eu` collides with it in Kafka.
    ctrl.create_topic(format!("{prefix}_eu"), 1, 1, 5_000)
        .await
        .unwrap();

    let route = KafkaRoute {
        pattern: "public.*".into(),
        topic: format!("{prefix}.{{row.region}}"),
        topic_fallback: Some(fallback.clone()),
        ..Default::default()
    };
    let plan = RoutePlan::new(vec![route], None).unwrap();
    let stats = plan.stats();
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 1, None, &KafkaSecurity::default())
        .await
        .unwrap();
    // `<prefix>_us` appears only after the sink listed the broker: the create is refused
    // (InvalidTopicException) and the record goes to the fallback instead of failing the batch.
    ctrl.create_topic(format!("{prefix}_us"), 1, 1, 5_000)
        .await
        .unwrap();
    let region = |n: u64, r: &str| {
        let mut e = ev(n, "orders");
        e.after = Some(to_raw_value(&serde_json::json!({ "n": n, "region": r })).unwrap());
        e
    };
    sink.send_batch(&[region(1, "eu"), region(2, "us"), region(3, "apac")])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let c = stats.snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (3, 1, 2));
    assert_eq!(
        sink.plan().dynamic_topic_count(),
        1,
        "refused names hold no cap slot"
    );
    // A later `us` row goes to the fallback at render time: no second create attempt.
    let attempts = sink.create_attempts();
    sink.send_batch(&[region(4, "us"), region(5, "us")])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    assert_eq!(
        sink.create_attempts(),
        attempts,
        "the refused name is remembered"
    );
    let c = stats.snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.fallback), (5, 1, 4));
    let ns = |recs: Vec<(
        String,
        std::collections::BTreeMap<String, Vec<u8>>,
        serde_json::Value,
    )>| {
        let mut v: Vec<u64> = recs
            .iter()
            .map(|r| r.2["after"]["n"].as_u64().unwrap())
            .collect();
        v.sort_unstable();
        v
    };
    assert_eq!(ns(read_all(&client, &fallback).await), [1, 2, 4, 5]);
    assert_eq!(ns(read_all(&client, &format!("{prefix}.apac")).await), [3]);
    let names: Vec<String> = client
        .list_topics()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .filter(|t| t.starts_with(&prefix))
        .collect();
    assert!(!names.contains(&format!("{prefix}.eu")) && !names.contains(&format!("{prefix}.us")));
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

/// Every record of `topic` across its partitions, value as bytes: (key, headers, value).
async fn read_raw(
    client: &rskafka::client::Client,
    topic: &str,
) -> Vec<(String, std::collections::BTreeMap<String, Vec<u8>>, Vec<u8>)> {
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
                    r.record.value.unwrap(),
                ));
            }
        }
    }
    out
}

#[tokio::test]
async fn outbox_inserts_are_published_raw_and_updates_produce_nothing() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let prefix = unique("aqueduct-it-outbox");
    let fallback = format!("{prefix}-fallback");
    // The outbox's dead-letter topic is its fallback's: `<topic_fallback>.dlq`.
    let dlq = format!("{fallback}.dlq");
    let route = KafkaRoute {
        name: Some("outbox".into()),
        pattern: "public.outbox".into(),
        topic_fallback: Some(fallback.clone()),
        outbox: Some(OutboxSpec {
            topic: format!("{prefix}.{{aggregate_type}}"),
            id: Some("id".into()),
            kind: Some("type".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let plan = RoutePlan::new(vec![route], None).unwrap();
    let stats = plan.stats();
    let inspector = KafkaInspector::new(brokers.clone(), &plan, KafkaSecurity::default());
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 3, None, &KafkaSecurity::default())
        .await
        .unwrap();

    let outbox = |n: u64, op: Op, row: serde_json::Value| {
        let mut e = ev(n, "outbox");
        e.op = op;
        e.after = Some(to_raw_value(&row).unwrap());
        e
    };
    let created = serde_json::json!({
        "id": "e-1", "aggregatetype": "order", "aggregateid": "A-1",
        "type": "OrderCreated", "payload": { "total": 12.5, "items": [1, 2] }
    });
    let text = serde_json::json!({
        "id": "e-2", "aggregatetype": "order", "aggregateid": 42,
        "type": "Note", "payload": "plain \"text\""
    });
    let missing = serde_json::json!({ "id": "e-3", "aggregatetype": "order", "payload": "x" });
    sink.send_batch(&[
        outbox(1, Op::Insert, created.clone()),
        outbox(2, Op::Insert, text),
        outbox(3, Op::Update, created),
        outbox(4, Op::Insert, missing),
    ])
    .await
    .unwrap();
    sink.flush().await.unwrap();

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let mut recs = read_raw(&client, &format!("{prefix}.order")).await;
    recs.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(recs.len(), 2, "two inserts; the update produced nothing");
    let header = |h: &std::collections::BTreeMap<String, Vec<u8>>, k: &str| {
        String::from_utf8(h[k].clone()).unwrap()
    };
    // Sorted by key: "42" before "A-1".
    let (key, headers, value) = &recs[0];
    assert_eq!(key, "42", "a numeric aggregate id is its JSON text");
    assert_eq!(value, b"plain \"text\"", "a text payload is its content");
    assert_eq!(header(headers, "id"), "e-2");
    assert_eq!(header(headers, "type"), "Note");
    let (key, headers, value) = &recs[1];
    assert_eq!(key, "A-1", "the raw aggregate id, not the table key");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(value).unwrap(),
        serde_json::json!({ "total": 12.5, "items": [1, 2] }),
        "a jsonb payload is passed through as JSON"
    );
    assert_eq!(header(headers, "id"), "e-1");
    assert_eq!(header(headers, "type"), "OrderCreated");
    assert_eq!(header(headers, "op"), "c");
    assert_eq!(header(headers, "lsn"), "1");

    let dead = read_raw(&client, &dlq).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(
        header(&dead[0].1, "aqueduct-error"),
        "route `outbox`: outbox `aggregate_id` column `aggregateid` is missing or null"
    );
    assert!(read_raw(&client, &fallback).await.is_empty());
    let c = stats.snapshot()[0].counts;
    assert_eq!((c.matched, c.produced, c.filtered, c.errors), (4, 2, 1, 1));

    // The inspector shows the raw values with their headers instead of failing to parse.
    let seen = inspector
        .inspect(Some(&format!("{prefix}.order")), 10)
        .await
        .unwrap();
    assert_eq!(seen.events.len(), 2);
    for e in &seen.events {
        match e.key.as_deref() {
            Some("42") => assert_eq!(e.event, serde_json::json!("plain \"text\"")),
            Some("A-1") => assert_eq!(e.event["total"], 12.5),
            other => panic!("unexpected key {other:?}"),
        }
        assert_eq!(e.headers["op"], "c");
        assert!(e.headers.contains_key("type"));
    }
}

/// Every record of `topic` in partition order: (partition, key, `op` header, value or `None` for
/// a tombstone).
async fn read_partitions(
    client: &rskafka::client::Client,
    topic: &str,
) -> Vec<(i32, String, String, Option<Vec<u8>>)> {
    let mut parts: Vec<i32> = client
        .list_topics()
        .await
        .unwrap()
        .into_iter()
        .find(|t| t.name == topic)
        .unwrap_or_else(|| panic!("topic {topic} does not exist"))
        .partitions
        .into_iter()
        .collect();
    parts.sort_unstable();
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
                    p,
                    String::from_utf8(r.record.key.unwrap()).unwrap(),
                    String::from_utf8(r.record.headers["op"].clone()).unwrap(),
                    r.record.value,
                ));
            }
        }
    }
    out
}

#[tokio::test]
async fn tombstones_follow_their_delete_and_row_format_values_are_the_row() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let events = unique("aqueduct-it-tomb");
    let rows = unique("aqueduct-it-rows");
    let routes = vec![
        KafkaRoute {
            pattern: "public.orders".into(),
            topic: events.clone(),
            tombstones: true,
            ..Default::default()
        },
        KafkaRoute {
            pattern: "public.items".into(),
            topic: rows.clone(),
            format: Some("row".into()),
            deletes: Some("row".into()),
            ..Default::default()
        },
    ];
    let plan = RoutePlan::new(routes, None).unwrap();
    let inspector = KafkaInspector::new(brokers.clone(), &plan, KafkaSecurity::default());
    let mut sink = KafkaSink::connect(brokers.clone(), plan, 4, None, &KafkaSecurity::default())
        .await
        .unwrap();
    let change = |n: u64, table: &str, op: Op, id: u64| {
        let mut e = ev(n, table);
        e.op = op;
        e.key = Some(format!(r#"{{"id":{id}}}"#));
        let row = Some(to_raw_value(&serde_json::json!({ "id": id, "n": n })).unwrap());
        (e.before, e.after) = match op {
            Op::Delete => (row, None),
            _ => (None, row),
        };
        e
    };
    let mut batch = Vec::new();
    for id in 0..20u64 {
        batch.push(change(4 * id + 1, "orders", Op::Insert, id));
        batch.push(change(4 * id + 2, "orders", Op::Delete, id));
        batch.push(change(4 * id + 3, "items", Op::Insert, id));
        batch.push(change(4 * id + 4, "items", Op::Delete, id));
    }
    sink.send_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    // Per-topic rates count every acknowledged record, tombstones included (20 inserts, 20 deletes,
    // 20 tombstones), and row-format deletes once each.
    let acked = sink.topic_stats().snapshot();
    assert_eq!((acked[&events].total(), acked[&rows].total()), (60, 40));
    // A tombstone carries its delete's op: 20 inserts, 20 deletes and 20 tombstones on the
    // envelope route; the row-format route's delete is itself the tombstone.
    assert_eq!(acked[&events].slots(), [20, 0, 40, 0, 0]);
    assert_eq!(acked[&rows].slots(), [20, 0, 20, 0, 0]);

    let client = ClientBuilder::new(brokers).build().await.unwrap();
    // Envelope route: per row, insert, delete, then the tombstone, all on one partition.
    let mut by_key: std::collections::BTreeMap<String, Vec<(i32, String, bool)>> =
        Default::default();
    for (p, key, op, value) in read_partitions(&client, &events).await {
        by_key
            .entry(key)
            .or_default()
            .push((p, op, value.is_none()));
    }
    assert_eq!(by_key.len(), 20);
    for (key, recs) in &by_key {
        assert_eq!(
            recs.iter().map(|r| (r.1.as_str(), r.2)).collect::<Vec<_>>(),
            [("c", false), ("d", false), ("d", true)],
            "{key}: insert, delete, then its tombstone"
        );
        assert!(recs.iter().all(|r| r.0 == recs[0].0), "{key} split");
    }
    let used: std::collections::HashSet<i32> = by_key.values().map(|r| r[0].0).collect();
    assert!(used.len() > 1, "rows spread over partitions");

    // Row format: the value is the row; a delete is the before-image with `__deleted`.
    let recs = read_partitions(&client, &rows).await;
    assert_eq!(recs.len(), 40);
    for (_, key, op, value) in &recs {
        let k: serde_json::Value = serde_json::from_str(key).unwrap();
        let v: serde_json::Value = serde_json::from_slice(value.as_ref().unwrap()).unwrap();
        assert_eq!(v["id"], k["key"]["id"], "the row, keyed by its id");
        assert!(v.get("source").is_none(), "no envelope");
        match op.as_str() {
            "c" => assert!(v.get("__deleted").is_none()),
            "d" => assert_eq!(v["__deleted"], true),
            o => panic!("op {o}"),
        }
    }

    // The inspector shows tombstones and rows raw, with their headers.
    let seen = inspector.inspect(Some(&events), 100).await.unwrap();
    let tombstones: Vec<_> = seen.events.iter().filter(|e| e.event.is_null()).collect();
    assert_eq!(tombstones.len(), 20);
    assert!(tombstones.iter().all(|e| e.headers["op"] == "d"));
    let seen = inspector.inspect(Some(&rows), 100).await.unwrap();
    assert_eq!(seen.events.len(), 40);
    assert!(seen.events.iter().all(|e| e.event["id"].is_number()));
}

/// The timestamps the broker stored for every record of a one-partition `topic`.
async fn stored_timestamps(brokers: Vec<String>, topic: &str) -> Vec<i64> {
    let client = ClientBuilder::new(brokers).build().await.unwrap();
    let pc = client
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await
        .unwrap();
    let end = pc.get_offset(OffsetAt::Latest).await.unwrap();
    let (recs, _hw) = pc.fetch_records(0, 1..1_000_000, 1_000).await.unwrap();
    assert_eq!(recs.len() as i64, end);
    recs.into_iter()
        .map(|r| r.record.timestamp.timestamp_millis())
        .collect()
}

/// #213: events from a backlog carry old commit times. By default the record timestamp is the
/// produce time, so time-based retention cannot delete the backlog right after it is written;
/// `record_timestamp = "commit"` keeps the commit time.
#[tokio::test]
async fn a_backlog_is_stamped_with_the_produce_time_not_its_old_commit_time() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    };
    let old_ms = 1_700_000_000_000; // `ev` commits in Nov 2023
    for (stamp, expect_old) in [
        (None, false),
        (Some(aqueduct_sinks::RecordTimestamp::Produce), false),
        (Some(aqueduct_sinks::RecordTimestamp::Commit), true),
    ] {
        let topic = unique("aqueduct-it-ts");
        let plan = RoutePlan::new(vec![], Some(topic.clone())).unwrap();
        let mut sink =
            KafkaSink::connect(brokers.clone(), plan, 1, None, &KafkaSecurity::default())
                .await
                .unwrap();
        if let Some(s) = stamp {
            sink = sink.with_record_timestamp(s);
        }
        let before = now_ms();
        sink.send_batch(&[ev(1, "users"), ev(2, "users")])
            .await
            .unwrap();
        sink.flush().await.unwrap();
        let after = now_ms();
        let stored = stored_timestamps(brokers.clone(), &topic).await;
        assert_eq!(stored.len(), 2, "{stamp:?}");
        for t in stored {
            if expect_old {
                assert_eq!(t, old_ms, "{stamp:?}: the commit time");
            } else {
                assert!(
                    (before - 5_000..=after + 5_000).contains(&t),
                    "{stamp:?}: {t} is not the produce time ({before}..{after})"
                );
            }
        }
    }
}
