mod config;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse().unwrap()),
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
    let listener = TcpListener::bind(&cfg.health_addr).await.expect("bind health_addr");
    tokio::spawn(health_server(listener));

    // TODO(pipeline): build the Postgres source (aqueduct-source-postgres), the
    // configured sink and a file checkpoint store, then call
    // aqueduct_core::run_pipeline(..., shutdown_rx).
    let _ = shutdown_rx;

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutting down");
    let _ = shutdown_tx.send(true);
}

/// Minimal HTTP server: any request gets `200 ok` (used by the container HEALTHCHECK).
async fn health_server(listener: TcpListener) {
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let _ = sock.read(&mut buf).await;
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .await;
        });
    }
}
