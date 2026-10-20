//! TLS and password authentication of the streaming replication client (#107) against real
//! servers: every `sslmode` against a TLS and a plaintext server, and SCRAM, md5 and cleartext
//! passwords. Uses the TLS fixture of `tls_it` plus three roles with their own `pg_hba` lines
//! on both servers (the image's default lines trust loopback, so `postgres` would not be asked
//! for a password), and a non-loopback name for the same servers:
//!
//! ```text
//! AQUEDUCT_TEST_PG_TLS=localhost:5433        # TLS server (cert SAN = localhost), postgres / aq
//! AQUEDUCT_TEST_PG_PLAIN=localhost:5434      # plaintext server, same credentials
//! AQUEDUCT_TEST_PG_CA=/path/ca.pem           # CA that signed the TLS server cert
//! AQUEDUCT_TEST_PG_WRONG_CA=/path/other.pem  # an unrelated CA
//! AQUEDUCT_TEST_PG_REMOTE_HOST=my-pod        # a non-loopback name that reaches both servers
//!
//! # on both servers, before the test:
//! psql -U postgres -c "SET password_encryption = 'md5'" \
//!      -c "CREATE ROLE aq_md5 LOGIN REPLICATION PASSWORD 'aq'" \
//!      -c "RESET password_encryption" \
//!      -c "CREATE ROLE aq_clear LOGIN REPLICATION PASSWORD 'aq'" \
//!      -c "CREATE ROLE aq_scram LOGIN REPLICATION PASSWORD 'aq'"
//! # put these first in $PGDATA/pg_hba.conf (the first matching line wins):
//! #   host all aq_md5 all md5
//! #   host all aq_clear all password
//! #   host all aq_scram all scram-sha-256
//! psql -U postgres -c 'SELECT pg_reload_conf()'
//!
//! cargo test -p aqueduct-source-postgres --test replication_auth_it -- --ignored
//! ```
//!
//! Every variable must be set: a test fails when one is missing, so a run that skips the
//! servers cannot report green.

use aqueduct_source_postgres::replication::{AuthMethod, ConnectOptions, ReplicationClient};
use aqueduct_source_postgres::SslMode;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}, see the module docs"))
}

fn port(var: &str) -> String {
    env(var).rsplit(':').next().unwrap().to_string()
}

/// `user@host:port` of the TLS or plaintext server, via `host`.
fn url(user: &str, host: &str, server: &str, sslmode: &str) -> String {
    let port = port(server);
    format!("postgres://{user}:aq@{host}:{port}/postgres?sslmode={sslmode}")
}

/// A role whose `pg_hba` line asks for SCRAM even on loopback.
const SCRAM: &str = "aq_scram";
const TLS: &str = "AQUEDUCT_TEST_PG_TLS";
const PLAIN: &str = "AQUEDUCT_TEST_PG_PLAIN";

fn remote() -> String {
    env("AQUEDUCT_TEST_PG_REMOTE_HOST")
}

fn with_ca() -> ConnectOptions {
    ConnectOptions {
        ssl_mode: None,
        ssl_root_cert: Some(env("AQUEDUCT_TEST_PG_CA")),
    }
}

async fn connect(url: &str, opts: &ConnectOptions) -> aqueduct_core::Result<ReplicationClient> {
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        ReplicationClient::connect(url, opts),
    )
    .await
    .expect("connect timed out")
}

/// Connects, proves the session works with `IDENTIFY_SYSTEM`, and returns how it is secured.
async fn session(url: &str, opts: &ConnectOptions) -> (bool, AuthMethod) {
    let mut c = connect(url, opts)
        .await
        .unwrap_or_else(|e| panic!("{url}: {e}"));
    let info = c.identify_system().await.unwrap();
    assert!(info.xlogpos > 0);
    let out = (c.encrypted(), c.auth_method());
    c.close().await;
    out
}

/// The connection must fail, and the error must name the expected cause. A bare `is_err()`
/// would also pass on a wrong port or bad credentials.
async fn fails_with(url: &str, opts: &ConnectOptions, any_of: &[&str]) {
    let e = match connect(url, opts).await {
        Ok(_) => panic!("{url}: connected, but must fail with one of {any_of:?}"),
        Err(e) => e.to_string(),
    };
    let lower = e.to_lowercase();
    assert!(
        any_of.iter().any(|n| lower.contains(n)),
        "{url}: expected one of {any_of:?}, got: {e}"
    );
    assert!(!e.contains(":aq@"), "the error shows the password: {e}");
}

const UNTRUSTED: &[&str] = &["unknownissuer", "unknown issuer"];
const NO_TLS: &[&str] = &["server does not support tls"];
const PLAINTEXT_REMOTE: &[&str] = &["plaintext to a non-loopback host"];
const NO_PASSWORD_IN_CLEAR: &[&str] = &["refusing to send the password"];

#[tokio::test]
#[ignore]
async fn verify_full_with_the_right_ca_binds_scram_to_the_certificate() {
    let got = session(&url(SCRAM, "localhost", TLS, "verify-full"), &with_ca()).await;
    assert_eq!(got, (true, AuthMethod::ScramSha256Plus));
    // The config override applies like the URL parameter.
    let opts = ConnectOptions {
        ssl_mode: Some(SslMode::VerifyFull),
        ..with_ca()
    };
    let got = session(&url(SCRAM, "localhost", TLS, "disable"), &opts).await;
    assert_eq!(got, (true, AuthMethod::ScramSha256Plus));
}

#[tokio::test]
#[ignore]
async fn verify_full_refuses_a_bad_certificate() {
    let u = url(SCRAM, "localhost", TLS, "verify-full");
    // Only the public roots: the self-signed test CA is unknown.
    fails_with(&u, &ConnectOptions::default(), UNTRUSTED).await;
    let wrong = ConnectOptions {
        ssl_mode: None,
        ssl_root_cert: Some(env("AQUEDUCT_TEST_PG_WRONG_CA")),
    };
    fails_with(&u, &wrong, UNTRUSTED).await;
    // Right CA, but the certificate names localhost only.
    fails_with(
        &url(SCRAM, "127.0.0.1", TLS, "verify-full"),
        &with_ca(),
        &["notvalidforname", "not valid for name"],
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn weaker_tls_modes_connect_encrypted() {
    // verify-ca checks the chain but not the name.
    let got = session(&url(SCRAM, "127.0.0.1", TLS, "verify-ca"), &with_ca()).await;
    assert_eq!(got, (true, AuthMethod::ScramSha256Plus));
    for mode in ["require", "prefer"] {
        let got = session(
            &url(SCRAM, "localhost", TLS, mode),
            &ConnectOptions::default(),
        )
        .await;
        assert_eq!(got, (true, AuthMethod::ScramSha256Plus), "{mode}");
    }
    // verify-ca without a CA trusts only public roots.
    fails_with(
        &url(SCRAM, "localhost", TLS, "verify-ca"),
        &ConnectOptions::default(),
        UNTRUSTED,
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn plaintext_server_needs_prefer_or_disable() {
    for mode in ["require", "verify-ca", "verify-full"] {
        fails_with(&url(SCRAM, "localhost", PLAIN, mode), &with_ca(), NO_TLS).await;
    }
    for mode in ["prefer", "disable"] {
        let got = session(
            &url(SCRAM, "localhost", PLAIN, mode),
            &ConnectOptions::default(),
        )
        .await;
        assert_eq!(got, (false, AuthMethod::ScramSha256), "{mode}");
    }
    // disable never asks for TLS, even when the server has it.
    let got = session(
        &url(SCRAM, "localhost", TLS, "disable"),
        &ConnectOptions::default(),
    )
    .await;
    assert_eq!(got, (false, AuthMethod::ScramSha256));
}

#[tokio::test]
#[ignore]
async fn plaintext_to_a_non_loopback_host_is_refused() {
    let host = remote();
    fails_with(
        &url(SCRAM, &host, PLAIN, "prefer"),
        &ConnectOptions::default(),
        PLAINTEXT_REMOTE,
    )
    .await;
    fails_with(
        &url(SCRAM, &host, PLAIN, "verify-full"),
        &ConnectOptions::default(),
        NO_TLS,
    )
    .await;
    // An explicit sslmode=disable accepts plaintext, as for the SQL connection; SCRAM does not
    // expose the password.
    let got = session(
        &url(SCRAM, &host, PLAIN, "disable"),
        &ConnectOptions::default(),
    )
    .await;
    assert_eq!(got, (false, AuthMethod::ScramSha256));
}

#[tokio::test]
#[ignore]
async fn cleartext_and_md5_are_refused_over_plaintext_to_a_non_loopback_host() {
    let host = remote();
    for user in ["aq_md5", "aq_clear"] {
        fails_with(
            &url(user, &host, PLAIN, "disable"),
            &ConnectOptions::default(),
            NO_PASSWORD_IN_CLEAR,
        )
        .await;
        fails_with(
            &url(user, &host, TLS, "disable"),
            &ConnectOptions::default(),
            NO_PASSWORD_IN_CLEAR,
        )
        .await;
    }
}

#[tokio::test]
#[ignore]
async fn cleartext_and_md5_work_over_tls_or_loopback() {
    let host = remote();
    for (user, method) in [
        ("aq_md5", AuthMethod::Md5),
        ("aq_clear", AuthMethod::Cleartext),
    ] {
        let tls = session(&url(user, "localhost", TLS, "verify-full"), &with_ca()).await;
        assert_eq!(tls, (true, method), "{user} over verify-full");
        // Encrypted to a non-loopback host is fine too.
        let tls = session(
            &url(user, &host, TLS, "require"),
            &ConnectOptions::default(),
        )
        .await;
        assert_eq!(tls, (true, method), "{user} over require to {host}");
        let plain = session(
            &url(user, "localhost", PLAIN, "disable"),
            &ConnectOptions::default(),
        )
        .await;
        assert_eq!(plain, (false, method), "{user} over loopback plaintext");
    }
}

#[tokio::test]
#[ignore]
async fn a_wrong_password_is_reported_by_the_server() {
    for user in [SCRAM, "aq_md5", "aq_clear"] {
        let u = format!(
            "postgres://{user}:wrong@localhost:{}/postgres?sslmode=require",
            port(TLS)
        );
        fails_with(
            &u,
            &ConnectOptions::default(),
            &["password authentication failed"],
        )
        .await;
    }
}
