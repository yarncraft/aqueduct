//! End-to-end Kafka sink test. Needs a broker, so it is skipped unless
//! `AQUEDUCT_KAFKA_BROKERS` is set (e.g. `localhost:9092`).

use aqueduct_core::{ChangeEvent, Op, Position, Sink};
use aqueduct_sinks::{KafkaInspector, KafkaRoute, KafkaSecurity, KafkaSink, RoutePlan};
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
    // The cockpit's per-topic rate counts every record once the broker acknowledged it.
    let acked = sink.topic_stats().snapshot();
    assert_eq!(acked.get(&topic).copied(), Some(total));
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
        key: None,
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
    // Acknowledged records per topic: the kept rows, and the dead-lettered one on its topic.
    let acked = sink.topic_stats().snapshot();
    assert_eq!(acked.get(&topic).copied(), Some(10));
    assert_eq!(acked.get(&dlq).copied(), Some(1));
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
