//! Live-TLS checks for the Postgres source (#18). Needs two servers, set up by the CI
//! `integration` job: one with TLS on (cert SAN = localhost only, signed by a throwaway CA)
//! and one plaintext. Both need `wal_level=logical` and a publication named `aq_pub`.
//!
//! ```text
//! AQUEDUCT_TEST_PG_TLS=localhost:5433        # TLS server, user postgres / password aq
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434      # plaintext server, same credentials
//! AQUEDUCT_TEST_PG_CA=/path/ca.pem           # CA that signed the TLS server cert
//! AQUEDUCT_TEST_PG_WRONG_CA=/path/other.pem  # an unrelated CA
//! cargo test -p aqueduct-source-postgres --test tls_it -- --ignored
//! ```
mod common;
use aqueduct_core::Source;
use aqueduct_source_postgres::{PostgresSource, PostgresSourceConfig, SslMode};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}, see the module docs"))
}

fn tls() -> String {
    env("AQUEDUCT_TEST_PG_TLS")
}

fn plain() -> String {
    env("AQUEDUCT_TEST_PG_PLAIN")
}

fn ca() -> Option<String> {
    Some(env("AQUEDUCT_TEST_PG_CA"))
}

fn wrong_ca() -> Option<String> {
    Some(env("AQUEDUCT_TEST_PG_WRONG_CA"))
}

fn source(addr: &str, tag: &str, mode: Option<SslMode>, ca: Option<String>) -> PostgresSource {
    let url = format!("postgres://postgres:aq@{addr}/postgres");
    // Tests run in parallel threads of one process: every test needs its own slot.
    let slot = format!("aq_tls_{}_{tag}", std::process::id());
    let mut cfg = PostgresSourceConfig::new(url, slot, "aq_pub");
    cfg.transport = common::transport();
    cfg.ssl_mode = mode;
    cfg.ssl_root_cert = ca;
    PostgresSource::new(cfg)
}

async fn starts(mut s: PostgresSource) -> aqueduct_core::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(20), s.start(None))
        .await
        .expect("start timed out")
}

/// The start must fail, and the error must name the expected TLS cause. A bare `is_err()` would
/// also pass on a wrong port, a missing publication or bad credentials.
async fn fails_with(s: PostgresSource, any_of: &[&str]) {
    let err = starts(s).await.expect_err("start must fail").to_string();
    let lower = err.to_lowercase();
    assert!(
        any_of.iter().any(|needle| lower.contains(needle)),
        "expected an error containing one of {any_of:?}, got: {err}"
    );
}

const UNTRUSTED: &[&str] = &[
    "unknownissuer",
    "unknown issuer",
    "unknown certificate authority",
];
const HOSTNAME: &[&str] = &["notvalidforname", "not valid for name"];
const NO_TLS: &[&str] = &["server does not support tls"];

#[tokio::test]
#[ignore]
async fn verify_full_with_correct_ca_connects() {
    starts(source(&tls(), "ok", None, ca())).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn verify_full_without_ca_fails() {
    // No root cert: only the bundled public roots are trusted, so the self-signed CA is rejected.
    fails_with(source(&tls(), "noca", None, None), UNTRUSTED).await;
}

#[tokio::test]
#[ignore]
async fn verify_full_with_wrong_ca_fails() {
    fails_with(source(&tls(), "wrongca", None, wrong_ca()), UNTRUSTED).await;
}

#[tokio::test]
#[ignore]
async fn verify_full_checks_hostname_but_verify_ca_does_not() {
    // The cert only names `localhost`, so reaching it as 127.0.0.1 is a hostname mismatch.
    let port = tls().rsplit(':').next().unwrap().to_string();
    let addr = format!("127.0.0.1:{port}");
    let full = source(&addr, "host_full", Some(SslMode::VerifyFull), ca());
    fails_with(full, HOSTNAME).await;
    let chain = source(&addr, "host_ca", Some(SslMode::VerifyCa), ca());
    starts(chain).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn default_mode_fails_against_plaintext_server() {
    fails_with(source(&plain(), "plain_default", None, None), NO_TLS).await;
}

#[tokio::test]
#[ignore]
async fn disable_works_on_plaintext_server() {
    let s = source(&plain(), "plain_off", Some(SslMode::Disable), None);
    starts(s).await.unwrap();
}
