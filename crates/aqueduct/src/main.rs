#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
mod admin;
mod checkpoint;
mod config;
mod kpis;
mod rates;
mod sink;
mod telemetry;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tokio::net::TcpListener;
use tokio::sync::watch;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    // `aqueduct --healthcheck`: used by the container HEALTHCHECK (the runtime image has no curl).
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        std::process::exit(healthcheck().await);
    }
    // The config comes first: it can turn on the OTLP log layer, which has to be part of the
    // subscriber. Its error is logged once the subscriber exists.
    let loaded = config::Config::load();
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let telemetry_cfg = loaded
        .as_ref()
        .map(|c| c.telemetry.clone())
        .unwrap_or_default();
    // Invalid telemetry settings switch export off; they never stop the pipeline.
    let (otlp_layer, otlp) = match telemetry::resolve(&telemetry_cfg, &env) {
        Ok(Some(s)) => match telemetry::start(&s) {
            Ok(layer) => (layer, Ok(Some(s))),
            Err(e) => (None, Err(e)),
        },
        Ok(None) => (None, Ok(None)),
        Err(e) => (None, Err(e)),
    };
    {
        use tracing_subscriber::prelude::*;
        let otlp_layer = otlp_layer.map(|l| {
            l.with_filter(tracing_subscriber::filter::filter_fn(|m| {
                telemetry::exported_target(m.target())
            }))
        });
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into()),
            )
            .with(tracing_subscriber::fmt::layer().json())
            .with(otlp_layer)
            .init();
    }
    match &otlp {
        Ok(Some(s)) => tracing::info!(
            metrics = s.metrics_endpoint.as_deref().map(telemetry::origin),
            logs = s.logs_endpoint.as_deref().map(telemetry::origin),
            "OTLP export on"
        ),
        Ok(None) => {}
        Err(e) => tracing::error!(error = %e, "OTLP export is off: invalid telemetry settings"),
    }

    let cfg = match loaded {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to load config");
            exit(1);
        }
    };
    let source_cfg = match cfg.source() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "invalid source config");
            exit(1);
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
            exit(1);
        }
    }
    if cfg.admin_exposed_beyond_loopback() {
        tracing::info!(
            addr = %cfg.health_addr,
            "the admin server speaks plain HTTP: the admin_token and inspector rows cross the network in clear text; put a TLS-terminating proxy in front (see docs/DEPLOYMENT.md, \"Cockpit over TLS\") or bind to 127.0.0.1"
        );
    }
    let listener = match TcpListener::bind(&cfg.health_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, addr = %cfg.health_addr, "failed to bind health_addr");
            exit(1);
        }
    };
    let metrics = Arc::new(aqueduct_core::Metrics::new());
    // The cockpit flags a snapshot whose pace this limit sets.
    metrics.set_snapshot_rate_limit(
        match &source_cfg {
            config::SourceConfig::Postgres(p) => p.snapshot_max_rows_per_sec,
            config::SourceConfig::Mongodb(m) => m.snapshot_max_rows_per_sec,
        }
        .filter(|n| *n > 0),
    );
    // Named in every record's `source` block and key.
    let source_info = match &source_cfg {
        config::SourceConfig::Postgres(p) => aqueduct_sinks::SourceInfo::Postgres {
            db: aqueduct_source_postgres::database_name(&p.url).unwrap_or_default(),
        },
        config::SourceConfig::Mongodb(_) => aqueduct_sinks::SourceInfo::Mongodb,
    };
    let (mut sink, inspector) = match sink::build(&cfg.sink, source_info).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to build sink");
            exit(1);
        }
    };
    // The inspector shows row data, so it needs the admin token as well.
    let inspector = (cfg.inspector && cfg.admin_token.as_deref().is_some_and(|t| !t.is_empty()))
        .then_some(inspector);
    let admin = admin::Admin::new(metrics.clone(), cfg.admin_token.clone(), inspector);
    // Without a token, its host is a `Host` the admin server accepts besides IPs and localhost.
    admin.set_health_addr(&cfg.health_addr);
    // The sink's plan: it names each table's topic, and its counters are shared (an `Arc`)
    // with the sink, so the admin server reads what the sink counts.
    admin.set_router(sink.plan().clone());
    // Records acknowledged per topic, counted by the sink after each produce.
    admin.set_topic_stats(sink.topic_stats());
    tokio::spawn(admin.clone().sample_forever());
    // OTLP metrics read the same exposition `/metrics` serves (a no-op when export is off).
    telemetry::export_metrics({
        let admin = admin.clone();
        move || admin.exposition()
    });
    tokio::spawn(admin::serve(listener, admin.clone()));

    let mut source: Box<dyn aqueduct_core::Source> = match source_cfg {
        config::SourceConfig::Postgres(p) => match p.source_config() {
            Ok(c) => {
                let (tx, rx) = aqueduct_core::snapshot_channel();
                admin.set_snapshots(tx);
                Box::new(
                    aqueduct_source_postgres::PostgresSource::new(c)
                        .with_metrics(metrics.clone())
                        .with_snapshot_control(rx),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "invalid postgres config");
                exit(1);
            }
        },
        config::SourceConfig::Mongodb(m) => {
            if let Err(e) = m.validate() {
                tracing::error!(error = %e, "invalid mongodb config");
                exit(1);
            }
            let (tx, rx) = aqueduct_core::snapshot_channel();
            admin.set_snapshots(tx);
            Box::new(
                aqueduct_source_mongodb::MongoSource::new(m)
                    .with_metrics(metrics.clone())
                    .with_snapshot_control(rx),
            )
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
                Ok(Err(e)) => { tracing::error!(error = %e, "pipeline failed"); exit(1); }
                Err(e) => { tracing::error!(error = %e, "pipeline task panicked"); exit(1); }
            }
        }
    }
    let _ = tokio::task::spawn_blocking(telemetry::shutdown).await;
}

/// Exits with `code` after a bounded flush of the OTLP exporters, so the reason for the exit
/// reaches the collector too.
fn exit(code: i32) -> ! {
    telemetry::shutdown();
    std::process::exit(code)
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

/// Probe our own health endpoint; returns the process exit code. Prints nothing: the
/// orchestrator only reads the exit code.
async fn healthcheck() -> i32 {
    let addr = config::health_probe_addr();
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
