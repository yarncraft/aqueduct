//! Needs a real broker: `AQUEDUCT_TEST_KAFKA=localhost:29092 cargo test -p aqueduct-source-kafka -- --ignored`.

use aqueduct_core::{Position, Source};
use aqueduct_source_kafka::{Format, KafkaSource, KafkaSourceConfig, StartAt};
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;

fn record(v: &str, key: &str) -> Record {
    Record {
        key: Some(key.as_bytes().to_vec()),
        value: Some(v.as_bytes().to_vec()),
        headers: Default::default(),
        timestamp: rskafka::chrono::Utc::now(),
    }
}

fn config(broker: &str, topic: &str, start_at: StartAt) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: vec![broker.to_string()],
        topics: vec![topic.to_string()],
        security: Default::default(),
        start_at,
        format: Format::Auto,
    }
}

async fn take(src: &mut KafkaSource, n: usize) -> Vec<aqueduct_core::ChangeEvent> {
    let mut out = Vec::new();
    while out.len() < n {
        let ev = tokio::time::timeout(std::time::Duration::from_secs(20), src.next())
            .await
            .expect("timed out waiting for events")
            .unwrap()
            .expect("stream ended");
        out.push(ev);
    }
    out
}

#[tokio::test]
#[ignore]
async fn consumes_resumes_and_respects_start_at() {
    let broker = std::env::var("AQUEDUCT_TEST_KAFKA").expect("AQUEDUCT_TEST_KAFKA");
    let topic = format!("src-test-{}", std::process::id());
    let client = ClientBuilder::new(vec![broker.clone()])
        .build()
        .await
        .unwrap();
    client
        .controller_client()
        .unwrap()
        .create_topic(&topic, 2, 1, 5_000)
        .await
        .unwrap();
    let parts = [
        client
            .partition_client(&topic, 0, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
        client
            .partition_client(&topic, 1, UnknownTopicHandling::Retry)
            .await
            .unwrap(),
    ];
    for i in 0..3 {
        parts[0]
            .produce(
                vec![record(&format!(r#"{{"n":{i}}}"#), "a")],
                Compression::NoCompression,
            )
            .await
            .unwrap();
        parts[1]
            .produce(
                vec![record(&format!("plain {i}"), "b")],
                Compression::NoCompression,
            )
            .await
            .unwrap();
    }

    // First run: read all 6, checkpoint after the 4th event.
    let mut src = KafkaSource::new(config(&broker, &topic, StartAt::Earliest));
    src.start(None).await.unwrap();
    let first = take(&mut src, 6).await;
    assert!(first
        .iter()
        .any(|e| e.after.as_ref().unwrap().get() == r#"{"n":2}"#));
    assert!(first
        .iter()
        .any(|e| e.after.as_ref().unwrap().get() == r#""plain 2""#));
    assert!(first.iter().all(|e| e.table == topic));
    let checkpoint = first[3].position.clone();
    assert!(matches!(checkpoint, Position::Text(_)));

    // Resume from the checkpoint: exactly the 2 events after the 4th come back, no more.
    let mut src = KafkaSource::new(config(&broker, &topic, StartAt::Earliest));
    src.start(Some(checkpoint)).await.unwrap();
    let rest = take(&mut src, 2).await;
    let again: Vec<_> = first[4..]
        .iter()
        .map(|e| e.after.as_ref().unwrap().get())
        .collect();
    let got: Vec<_> = rest
        .iter()
        .map(|e| e.after.as_ref().unwrap().get())
        .collect();
    assert_eq!(got, again);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), src.next())
            .await
            .is_err(),
        "no more events expected"
    );

    // New records arrive after a `latest` start; the old ones are skipped.
    let mut src = KafkaSource::new(config(&broker, &topic, StartAt::Latest));
    src.start(None).await.unwrap();
    parts[0]
        .produce(vec![record(r#"{"n":99}"#, "a")], Compression::NoCompression)
        .await
        .unwrap();
    let live = take(&mut src, 1).await;
    assert_eq!(live[0].after.as_ref().unwrap().get(), r#"{"n":99}"#);
}
