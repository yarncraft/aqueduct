//! Managed identity sign-in (#140) against a real TLS server. A mock
//! Container Apps identity endpoint on loopback hands out the password of a role whose
//! `pg_hba` line asks for a cleartext password, which is how Azure Database for PostgreSQL
//! takes a Microsoft Entra token. The URL carries no password, so a change that streams proves
//! the token was the password of the SQL session and of the replication connection.
//!
//! Uses the TLS fixture of `tls_it` and the `aq_clear` role of `replication_auth_it`:
//!
//! ```text
//! AQUEDUCT_TEST_PG_TLS=localhost:5433   # TLS server (cert SAN = localhost), postgres / aq
//! AQUEDUCT_TEST_PG_CA=/path/ca.pem      # CA that signed the TLS server cert
//!
//! # on that server, before the test:
//! psql -U postgres -c "CREATE ROLE aq_clear LOGIN REPLICATION PASSWORD 'aq'"
//! # first in $PGDATA/pg_hba.conf, then SELECT pg_reload_conf():
//! #   host all aq_clear all password
//!
//! cargo test -p aqueduct-source-postgres --test azure_identity_it -- --ignored
//! ```
//!
//! Every variable must be set: a test fails when one is missing.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{Op, Source};
use aqueduct_source_postgres::{AzureCredential, PostgresSource, PostgresSourceConfig, SourceAuth};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::error::SqlState;
use tokio_postgres::Client;

const ROLE: &str = "aq_clear";
const ROLE_PASSWORD: &str = "aq";
const IDENTITY_HEADER: &str = "test-identity-header";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}, see the module docs"))
}

fn tls() -> String {
    env("AQUEDUCT_TEST_PG_TLS")
}

fn ca() -> String {
    env("AQUEDUCT_TEST_PG_CA")
}

/// `postgres` over verified TLS, for setup.
async fn admin() -> Client {
    let tls_cfg = aqueduct_tls::client_config(Some(&ca()), aqueduct_tls::Verify::Full).unwrap();
    let url = format!("postgres://postgres:aq@{}/postgres?sslmode=require", tls());
    let (c, conn) = tokio_postgres::connect(
        &url,
        tokio_postgres_rustls::MakeRustlsConnect::new((*tls_cfg).clone()),
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    c
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos % 1_000_000_000)
}

/// A Container Apps identity endpoint on loopback. It answers `token` for a request with the
/// right header and query, and 401 otherwise; returns its URL and the number of tokens issued.
async fn identity_endpoint(token: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/msi/token", listener.local_addr().unwrap());
    let issued = Arc::new(AtomicUsize::new(0));
    let count = issued.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let count = count.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = sock.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&buf);
                let ok = head.starts_with(
                    "GET /msi/token?api-version=2019-08-01&resource=https%3A%2F%2Fossrdbms-aad.database.windows.net HTTP/1.1\r\n",
                ) && head.contains(&format!("\r\nX-IDENTITY-HEADER: {IDENTITY_HEADER}\r\n"));
                let (status, body) = if ok {
                    count.fetch_add(1, Ordering::SeqCst);
                    let exp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        + 3600;
                    (
                        "200 OK",
                        format!(
                            r#"{{"access_token":"{token}","expires_on":"{exp}","resource":"https://ossrdbms-aad.database.windows.net","token_type":"Bearer"}}"#
                        ),
                    )
                } else {
                    (
                        "401 Unauthorized",
                        r#"{"error":"unauthorized"}"#.to_string(),
                    )
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (url, issued)
}

fn source(endpoint: &str, slot: &str, publication: &str) -> PostgresSource {
    // No password in the URL: the identity endpoint's token is the only one.
    let url = format!("postgres://{ROLE}@{}/postgres", tls());
    let mut cfg = PostgresSourceConfig::new(url, slot, publication);
    cfg.ssl_root_cert = Some(ca());
    cfg.poll_interval = Duration::from_millis(50);
    cfg.auth = SourceAuth::Azure(
        AzureCredential::container_apps(endpoint, IDENTITY_HEADER, None).unwrap(),
    );
    cfg.check_auth().unwrap();
    PostgresSource::new(cfg)
}

async fn drop_slot(db: &Client, slot: &str) {
    for _ in 0..100 {
        match db
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
        {
            Err(e) if e.code() == Some(&SqlState::OBJECT_IN_USE) => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            r => {
                r.unwrap();
                return;
            }
        }
    }
    panic!("slot {slot} stayed in use");
}

async fn signs_in_and_streams() {
    let db = admin().await;
    let table = unique("mi_t");
    let publication = unique("mi_pub");
    let slot = unique("mi_slot");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, v text);
         GRANT SELECT ON {table} TO {ROLE};
         CREATE PUBLICATION {publication} FOR TABLE {table};"
    ))
    .await
    .unwrap();

    // A token that is not the role's password is refused by the server: the token, not some
    // other credential, is what signs in.
    let (wrong, _) = identity_endpoint("not-the-password").await;
    let mut s = source(&wrong, &slot, &publication);
    let e = tokio::time::timeout(Duration::from_secs(20), s.start(None))
        .await
        .expect("start timed out")
        .expect_err("a wrong token must not sign in")
        .to_string();
    assert!(e.contains("password"), "{e}");
    assert!(
        !e.contains("not-the-password"),
        "the error shows the token: {e}"
    );
    drop(s);

    let (endpoint, issued) = identity_endpoint(ROLE_PASSWORD).await;
    let mut s = source(&endpoint, &slot, &publication);
    tokio::time::timeout(Duration::from_secs(20), s.start(None))
        .await
        .expect("start timed out")
        .unwrap();
    db.execute(
        &format!("INSERT INTO {table} VALUES (1, 'signed in with a token')"),
        &[],
    )
    .await
    .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let ev = s.next().await.unwrap().expect("the source never ends");
            if ev.op == Op::Insert && ev.table == table {
                return ev;
            }
        }
    })
    .await
    .expect("no change streamed");
    let after = ev.after.as_ref().unwrap().get().to_string();
    assert!(after.contains("signed in with a token"), "{after}");
    s.ack(ev.checkpoint.clone().unwrap()).await.unwrap();
    // One token, cached across the SQL session and the replication connection.
    assert_eq!(issued.load(Ordering::SeqCst), 1);
    drop(s);

    drop_slot(&db, &slot).await;
    db.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table};"
    ))
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a TLS Postgres server, see the module docs"]
async fn stream_signs_in_with_a_container_apps_token() {
    signs_in_and_streams().await;
}
