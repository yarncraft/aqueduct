use aqueduct_core::{Error, Result};
use aqueduct_sinks::{
    KafkaInspector, KafkaRoute, KafkaSasl, KafkaSecurity, KafkaSink, PartitionBy, SourceInfo,
    TopicRouter,
};

use crate::config::KafkaConfig;

fn router(cfg: &KafkaConfig) -> Result<TopicRouter> {
    let routes = cfg
        .routes
        .iter()
        .map(|r| KafkaRoute {
            pattern: r.pattern.clone(),
            topic: r.topic.clone(),
        })
        .collect();
    TopicRouter::new(routes, cfg.topic.clone())
}

fn security(cfg: &KafkaConfig) -> Result<KafkaSecurity> {
    let sasl = match (&cfg.sasl_mechanism, &cfg.sasl_username, &cfg.sasl_password) {
        (None, None, None) => None,
        (Some(m), Some(u), Some(p)) => Some(KafkaSasl {
            mechanism: m.parse().map_err(Error::Config)?,
            username: u.clone(),
            password: p.expose(),
        }),
        _ => {
            return Err(Error::Config(
                "kafka sasl needs sasl_mechanism, sasl_username and sasl_password together".into(),
            ))
        }
    };
    Ok(KafkaSecurity {
        tls: cfg.tls,
        tls_ca_file: cfg.tls_ca_file.clone(),
        sasl,
    })
}

/// Build the Kafka sink and the inspector that reads the same topics back
/// (with the same TLS/SASL settings).
pub async fn build(cfg: &KafkaConfig, source: SourceInfo) -> Result<(KafkaSink, KafkaInspector)> {
    let router = router(cfg)?;
    let security = security(cfg)?;
    let inspector = KafkaInspector::new(cfg.brokers.clone(), &router, security.clone());
    let by: PartitionBy = match &cfg.partition_by {
        Some(s) => s.parse().map_err(Error::Config)?,
        None => PartitionBy::default(),
    };
    cfg.check_replication_factor().map_err(Error::Config)?;
    let sink = KafkaSink::connect(
        cfg.brokers.clone(),
        router,
        cfg.partitions,
        cfg.replication_factor,
        &security,
    )
    .await?
    .with_partition_by(by)
    .with_source(source);
    Ok((sink, inspector))
}
