//! Streaming replication client against a real server. Needs a Postgres with
//! `wal_level=logical` that accepts the `postgres` user **without a password** on replication
//! connections (`trust`), because the client does not do password authentication yet:
//!
//! ```text
//! podman run -d -p 5436:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16 -c wal_level=logical
//! AQUEDUCT_TEST_PG_TRUST=localhost:5436 cargo test -p aqueduct-source-postgres \
//!     --test replication_it -- --ignored
//! ```
//!
//! `AQUEDUCT_TEST_PG_TRUST` must be set: the test fails when it is not, so a run that skips
//! the server cannot report green. The integration jobs start a trust-auth `pg-trust` container.

use std::time::Duration;

use aqueduct_source_postgres::parse_lsn;
use aqueduct_source_postgres::replication::{Replication, ReplicationClient};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "needs a trust-auth Postgres server, see the module docs"]
async fn streamed_insert_matches_the_peeked_change() {
    let addr = std::env::var("AQUEDUCT_TEST_PG_TRUST")
        .expect("AQUEDUCT_TEST_PG_TRUST must name a trust-auth Postgres, see the module docs");
    let url = format!("postgres://postgres@{addr}/postgres?sslmode=disable");
    let (db, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("{}_{}", std::process::id(), nanos % 1_000_000_000);
    let (table, publication) = (format!("repl_{id}"), format!("aq_repl_pub_{id}"));
    let (stream_slot, peek_slot) = (format!("aq_repl_s_{id}"), format!("aq_repl_p_{id}"));
    db.batch_execute(&format!(
        "CREATE TABLE {table} (k int PRIMARY KEY, v text NOT NULL);
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();
    for slot in [&stream_slot, &peek_slot] {
        db.execute(
            "SELECT pg_create_logical_replication_slot($1::text, 'pgoutput')",
            &[slot],
        )
        .await
        .unwrap();
    }

    let mut client = ReplicationClient::connect(&url).await.unwrap();
    let info = client.identify_system().await.unwrap();
    assert!(info.xlogpos > 0 && info.timeline >= 1);
    client
        .start_replication(&stream_slot, 0, &publication)
        .await
        .unwrap();

    db.execute(&format!("INSERT INTO {table} VALUES (1, 'a')"), &[])
        .await
        .unwrap();

    // The Insert ('I') message as the stream delivers it.
    let streamed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.next_message().await.unwrap().expect("stream ended") {
                Replication::XLogData {
                    wal_start, data, ..
                } if data.first() == Some(&b'I') => break (wal_start, data),
                Replication::Keepalive {
                    reply_requested: true,
                    wal_end,
                    ..
                } => client.send_status(wal_end, 0, false).await.unwrap(),
                _ => {}
            }
        }
    })
    .await
    .expect("no Insert arrived over the stream");

    // The same change as a peek returns it from the other slot.
    let rows = db
        .query(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1::text::name, NULL, NULL, \
             'proto_version', '1', 'publication_names', $2::text)",
            &[&peek_slot, &publication],
        )
        .await
        .unwrap();
    let peeked = rows
        .iter()
        .map(|r| {
            let lsn: String = r.get(0);
            let data: Vec<u8> = r.get(1);
            (parse_lsn(&lsn).unwrap(), data)
        })
        .find(|(_, d)| d.first() == Some(&b'I'))
        .expect("the peek returned no Insert");

    client.close().await;
    for slot in [&stream_slot, &peek_slot] {
        // The walsender can hold the slot for a moment after the connection closes.
        for _ in 0..50 {
            if db
                .execute("SELECT pg_drop_replication_slot($1::text)", &[slot])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();

    assert_eq!(streamed.1, peeked.1, "pgoutput bytes differ");
    assert_eq!(streamed.0, peeked.0, "LSN differs");
}
