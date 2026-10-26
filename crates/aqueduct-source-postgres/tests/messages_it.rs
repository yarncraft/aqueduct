//! Logical messages (S3, #72) against a real Postgres 14+: what `pg_logical_emit_message` produces
//! on the wire must decode, and the peek only receives it when `messages` is asked for. Needs a
//! plaintext server with user postgres / password aq and `wal_level=logical`:
//!
//! ```text
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434
//! cargo test -p aqueduct-source-postgres --test messages_it -- --ignored
//! ```
// The workspace denies these outside tests; this whole crate is one.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use aqueduct_source_postgres::decoder::{Logical, Mark, Message, decode};
use tokio_postgres::NoTls;

const PEEK: &str = "SELECT data FROM pg_logical_slot_peek_binary_changes($1::text::name, NULL, NULL, \
     'proto_version', '1', 'publication_names', $2::text";

#[tokio::test]
#[ignore]
async fn emitted_messages_decode_and_are_only_sent_on_request() {
    let addr = std::env::var("AQUEDUCT_TEST_PG_PLAIN")
        .unwrap_or_else(|_| panic!("set AQUEDUCT_TEST_PG_PLAIN, see the module docs"));
    let url = format!("postgres://postgres:aq@{addr}/postgres");
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let (slot, publication) = (format!("aq_msg_{id}"), format!("aq_msg_pub_{id}"));
    db.batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    db.execute(
        "SELECT pg_create_logical_replication_slot($1::text::name, 'pgoutput')",
        &[&slot],
    )
    .await
    .unwrap();

    let low = format!(r#"{{"slot":"{slot}","snapshot_id":"snap-1","chunk":4,"mark":"low"}}"#);
    let foreign = r#"{"slot":"someone_else","snapshot_id":"snap-1","chunk":4,"mark":"high"}"#;
    for (transactional, prefix, body) in [
        (true, "aqueduct", low.as_str()),
        (true, "aqueduct", foreign),
        (false, "aqueduct", low.as_str()),
        // A commit flushes the WAL. Without it the peek stops before the unflushed
        // non-transactional message (the `flush` argument only exists from PG17).
        (true, "other_tool", low.as_str()),
    ] {
        db.execute(
            "SELECT pg_logical_emit_message($1, $2, $3)",
            &[&transactional, &prefix, &body],
        )
        .await
        .unwrap();
    }

    // Without `messages` the slot sends none of them.
    let quiet = db
        .query(&format!("{PEEK})"), &[&slot, &publication])
        .await
        .unwrap();
    for row in &quiet {
        let data: &[u8] = row.get(0);
        assert!(
            !matches!(decode(data).unwrap(), Message::Logical(_)),
            "a logical message arrived without 'messages'"
        );
    }

    let rows = db
        .query(
            &format!("{PEEK}, 'messages', 'true')"),
            &[&slot, &publication],
        )
        .await
        .unwrap();
    let logical: Vec<Logical> = rows
        .iter()
        .filter_map(|r| match decode(r.get::<_, &[u8]>(0)).unwrap() {
            Message::Logical(l) => Some(l),
            _ => None,
        })
        .collect();
    assert_eq!(logical.len(), 4, "all four messages decode");
    assert_eq!(
        logical.iter().map(|l| l.transactional).collect::<Vec<_>>(),
        [true, true, false, true]
    );
    assert!(logical.iter().all(|l| l.lsn > 0));

    let ours: Vec<_> = logical
        .iter()
        .filter_map(|l| l.watermark_for(&slot))
        .collect();
    assert_eq!(
        ours.len(),
        1,
        "only the transactional aqueduct message for this slot"
    );
    assert_eq!(ours[0].snapshot_id, "snap-1");
    assert_eq!(ours[0].chunk, 4);
    assert_eq!(ours[0].mark, Mark::Low);

    db.execute("SELECT pg_drop_replication_slot($1::text::name)", &[&slot])
        .await
        .unwrap();
    db.batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}
