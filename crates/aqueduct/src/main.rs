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
                .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
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
    let source_cfg = match cfg.source() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "invalid source config");
            std::process::exit(1);
        }
    };
    tracing::info!(source = source_cfg.type_name(), "aqueduct starting");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    match cfg.check_admin_exposure() {
        Ok(true) => tracing::warn!(
            addr = %cfg.health_addr,
            "admin_insecure_no_auth is set: the admin API and cockpit are UNAUTHENTICATED on a non-loopback address"
        ),
        Ok(false) => {}
        Err(e) => {
            tracing::error!("{e}");
            std::process::exit(1);
        }
    }
    let listener = match TcpListener::bind(&cfg.health_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, addr = %cfg.health_addr, "failed to bind health_addr");
            std::process::exit(1);
        }
    };
    let metrics = Arc::new(aqueduct_core::Metrics::new());
    let (mut sink, inspector) = match sink::build(&cfg.sink).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to build sink");
            std::process::exit(1);
        }
    };
    // The inspector shows row data, so it needs the admin token as well.
    let inspector = (cfg.inspector && cfg.admin_token.as_deref().is_some_and(|t| !t.is_empty()))
        .then_some(inspector);
    let admin = admin::Admin::new(metrics.clone(), cfg.admin_token.clone(), inspector);
    tokio::spawn(admin.clone().sample_forever());
    tokio::spawn(admin::serve(listener, admin));

    let mut source: Box<dyn aqueduct_core::Source> = match source_cfg {
        config::SourceConfig::Postgres(p) => match p.source_config() {
            Ok(c) => Box::new(aqueduct_source_postgres::PostgresSource::new(c)),
            Err(e) => {
                tracing::error!(error = %e, "invalid postgres config");
                std::process::exit(1);
            }
        },
        config::SourceConfig::Kafka(k) => match k.source_config() {
            Ok(c) => Box::new(aqueduct_source_kafka::KafkaSource::new(c)),
            Err(e) => {
                tracing::error!(error = %e, "invalid kafka source config");
                std::process::exit(1);
            }
        },
        config::SourceConfig::Mongodb(m) => {
            if let Err(e) = m.validate() {
                tracing::error!(error = %e, "invalid mongodb config");
                std::process::exit(1);
            }
            Box::new(aqueduct_source_mongodb::MongoSource::new(m))
        }
    };
    let mut store = checkpoint::FileCheckpoint::new(cfg.checkpoint_path.clone());

    let mut pipeline = tokio::spawn(async move {
        aqueduct_core::run_pipeline_with_metrics(
            &mut *source,
            &mut sink,
            &mut store,
            shutdown_rx,
            &metrics,
        )
        .await
    });

    tokio::select! {
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received, draining");
            let _ = shutdown_tx.send(true);
            // Stay inside the orchestrator's grace period (docker stop = 10 s by default).
            match tokio::time::timeout(std::time::Duration::from_secs(25), &mut pipeline).await {
                Ok(_) => tracing::info!("clean shutdown"),
                Err(_) => tracing::error!("pipeline did not stop within 25 s, exiting"),
            }
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

/// Resolves on SIGINT or (on Unix) SIGTERM, which is what `docker stop` and Kubernetes send.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
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
