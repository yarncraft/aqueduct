mod admin;
mod checkpoint;
mod config;
mod sink;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tokio::net::TcpListener;
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    // `aqueduct --healthcheck`: used by the container HEALTHCHECK (the runtime image has no curl).
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        std::process::exit(healthcheck().await);
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .json()
        .init();

    let cfg = match config::Config::load() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to load config");
            std::process::exit(1);
        }
    };
    tracing::info!(slot = %cfg.postgres.slot, publication = %cfg.postgres.publication, "aqueduct starting");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener = TcpListener::bind(&cfg.health_addr)
        .await
        .expect("bind health_addr");
    let metrics = Arc::new(aqueduct_core::Metrics::new());
    let admin = admin::Admin::new(metrics.clone(), cfg.admin_token.clone());
    tokio::spawn(admin.clone().sample_forever());
    tokio::spawn(admin::serve(listener, admin));

    let mut source = aqueduct_source_postgres::PostgresSource::new(
        aqueduct_source_postgres::PostgresSourceConfig::new(
            cfg.postgres.url.clone(),
            cfg.postgres.slot.clone(),
            cfg.postgres.publication.clone(),
        ),
    );
    let mut sink = match sink::build(&cfg.sink).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to build sink");
            std::process::exit(1);
        }
    };
    let mut store = checkpoint::FileCheckpoint::new(cfg.checkpoint_path.clone());

    let mut pipeline = tokio::spawn(async move {
        aqueduct_core::run_pipeline_with_metrics(
            &mut source,
            sink.as_mut(),
            &mut store,
            shutdown_rx,
            &metrics,
        )
        .await
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            let _ = shutdown_tx.send(true);
            let _ = pipeline.await;
        }
        res = &mut pipeline => {
            match res {
                Ok(Ok(())) => tracing::info!("pipeline finished"),
                Ok(Err(e)) => { tracing::error!(error = %e, "pipeline failed"); std::process::exit(1); }
                Err(e) => { tracing::error!(error = %e, "pipeline task panicked"); std::process::exit(1); }
            }
        }
    }
}

/// Probe our own health endpoint; returns the process exit code.
async fn healthcheck() -> i32 {
    let addr = std::env::var("AQUEDUCT_HEALTH_PROBE").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let probe = async {
        let mut sock = tokio::net::TcpStream::connect(&addr).await.ok()?;
        sock.write_all(b"GET /healthz HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
            .await
            .ok()?;
        let mut buf = [0u8; 64];
        let n = sock.read(&mut buf).await.ok()?;
        buf[..n].starts_with(b"HTTP/1.1 200").then_some(())
    };
    match tokio::time::timeout(std::time::Duration::from_secs(3), probe).await {
        Ok(Some(())) => 0,
        _ => 1,
    }
}
