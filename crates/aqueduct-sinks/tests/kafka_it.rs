//! End-to-end Kafka sink test. Needs a broker, so it is skipped unless
//! `AQUEDUCT_KAFKA_BROKERS` is set (e.g. `localhost:9092`).

use aqueduct_core::{ChangeEvent, Op, Position, Sink};
use aqueduct_sinks::{KafkaSecurity, KafkaSink, TopicRouter};
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

#[tokio::test]
async fn produces_all_events_in_per_table_order() {
    let Ok(brokers) = std::env::var("AQUEDUCT_KAFKA_BROKERS") else {
        eprintln!("AQUEDUCT_KAFKA_BROKERS not set; skipping");
        return;
    };
    let brokers: Vec<String> = brokers.split(',').map(str::to_owned).collect();
    let topic = format!("aqueduct-it-{}", std::process::id());

    let router = TopicRouter::new(vec![], Some(topic.clone())).unwrap();
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
                let table = format!("{}.{}", k["schema"].as_str().unwrap(), k["table"].as_str().unwrap());
                assert_eq!(k["key"], serde_json::Value::Null, "no primary key in these events");
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
}
