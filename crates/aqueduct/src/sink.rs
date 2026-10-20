use aqueduct_core::{Error, Result};
use aqueduct_sinks::{
    KafkaInspector, KafkaRoute, KafkaSasl, KafkaSecurity, KafkaSink, PartitionBy, RoutePlan,
    SourceInfo,
};

use crate::config::KafkaConfig;

fn plan(cfg: &KafkaConfig) -> Result<RoutePlan> {
    let routes = cfg
        .routes
        .iter()
        .map(|r| KafkaRoute {
            name: r.name.clone(),
            pattern: r.pattern.clone(),
            topic: r.topic.clone(),
            ops: r.ops.clone(),
            filter: r.filter.clone(),
            key: r.key.clone(),
        })
        .collect();
    RoutePlan::with_dead_letter(routes, cfg.topic.clone(), cfg.dead_letter_topic.clone())
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
    let plan = plan(cfg)?;
    let security = security(cfg)?;
    let inspector = KafkaInspector::new(cfg.brokers.clone(), &plan, security.clone());
    let by: PartitionBy = match &cfg.partition_by {
        Some(s) => s.parse().map_err(Error::Config)?,
        None => PartitionBy::default(),
    };
    cfg.check_replication_factor().map_err(Error::Config)?;
    let sink = KafkaSink::connect(
        cfg.brokers.clone(),
        plan,
        cfg.partitions,
        cfg.replication_factor,
        &security,
    )
    .await?
    .with_partition_by(by)
    .with_source(source);
    Ok((sink, inspector))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka(toml_text: &str) -> KafkaConfig {
        toml::from_str(&format!("brokers = ['b:9092']\n{toml_text}")).unwrap()
    }

    #[test]
    fn route_filters_come_from_the_config_and_are_validated() {
        let cfg = kafka(
            "topic = 'rest'
dead_letter_topic = 'aqueduct.dlq'
[[routes]]
name = 'paid'
match = 'public.orders'
ops = ['insert', 'update']
where = \"row.status == 'paid'\"
topic = 'orders'
",
        );
        let d = plan(&cfg).unwrap().describe();
        assert_eq!(d.dead_letter_topic.as_deref(), Some("aqueduct.dlq"));
        assert_eq!(d.routes[0].ops, Some(vec!["insert", "update"]));
        assert_eq!(d.routes[0].filter.as_deref(), Some("row.status == 'paid'"));

        // `where` without a dead-letter topic, and a syntax error, fail naming the route.
        let err = |text: &str| plan(&kafka(text)).unwrap_err().to_string();
        let e = err("[[routes]]
name = 'paid'
match = 'public.orders'
where = \"row.status == 'paid'\"
topic = 'orders'
");
        assert!(
            e.contains("route `paid`") && e.contains("dead_letter_topic"),
            "{e}"
        );
        let e = err("dead_letter_topic = 'dlq'
[[routes]]
name = 'paid'
match = 'public.orders'
where = 'row.status =='
topic = 'orders'
");
        assert!(e.contains("route `paid`: invalid `where`"), "{e}");
        let e = err("[[routes]]
match = 'public.orders'
ops = ['upsert']
topic = 'orders'
");
        assert!(
            e.contains("route `public.orders`: unknown op `upsert`"),
            "{e}"
        );
    }

    #[test]
    fn route_keys_come_from_the_config_and_are_validated() {
        let d = plan(&kafka(
            "[[routes]]
match = 'public.orders'
key = ['tenant', 'id']
topic = 'orders'
",
        ))
        .unwrap()
        .describe();
        assert_eq!(
            d.routes[0].key,
            Some(vec!["tenant".to_string(), "id".to_string()])
        );
        let e = plan(&kafka(
            "[[routes]]
match = 'public.orders'
key = ['address.city']
topic = 'orders'
",
        ))
        .unwrap_err()
        .to_string();
        assert!(e.contains("route `public.orders`: `key`"), "{e}");
    }
}
