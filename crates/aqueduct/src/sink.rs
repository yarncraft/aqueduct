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
            // Empty when unset; the plan requires a topic unless the route is an outbox.
            topic: r.topic.clone().unwrap_or_default(),
            ops: r.ops.clone(),
            filter: r.filter.clone(),
            key: r.key.clone(),
            topic_fallback: r.topic_fallback.clone(),
            topic_values: r.topic_values.clone(),
            outbox: r.outbox.as_ref().map(|o| o.spec()),
        })
        .collect();
    let mut plan =
        RoutePlan::with_dead_letter(routes, cfg.topic.clone(), cfg.dead_letter_topic.clone())?;
    plan.set_max_dynamic_topics(cfg.max_dynamic_topics);
    Ok(plan)
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

    #[test]
    fn content_routing_comes_from_the_config_and_is_validated() {
        let cfg = kafka(
            "max_dynamic_topics = 7
[[routes]]
match = 'public.orders'
topic = 'orders.{row.region}'
topic_fallback = 'orders.unknown'
topic_values = ['eu', 'us']
",
        );
        assert_eq!(cfg.max_dynamic_topics, 7);
        let d = plan(&cfg).unwrap().describe();
        assert_eq!(
            d.routes[0].topic_fallback.as_deref(),
            Some("orders.unknown")
        );
        assert_eq!(
            d.routes[0].topic_values,
            Some(vec!["eu".to_string(), "us".to_string()])
        );
        assert_eq!(
            kafka("topic = 't'").max_dynamic_topics,
            aqueduct_sinks::DEFAULT_MAX_DYNAMIC_TOPICS
        );
        let e = plan(&kafka(
            "[[routes]]
match = 'public.orders'
topic = 'orders.{row.region}'
",
        ))
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("route `public.orders`") && e.contains("topic_fallback"),
            "{e}"
        );
    }

    #[test]
    fn outbox_routes_come_from_the_config_and_are_validated() {
        let d = plan(&kafka(
            "dead_letter_topic = 'aqueduct.dlq'
[[routes]]
match = 'public.outbox'
topic_fallback = 'outbox.unknown'
outbox = { aggregate_type = 'agg_type', aggregate_id = 'agg_id', payload = 'body', topic = 'outbox.{aggregate_type}', id = 'event_id', type = 'event_type' }
",
        ))
        .unwrap()
        .describe();
        let r = &d.routes[0];
        assert_eq!(r.topic, "outbox.{aggregate_type}");
        let o = r.outbox.as_ref().unwrap();
        assert_eq!(
            (o.aggregate_type.as_str(), o.aggregate_id.as_str()),
            ("agg_type", "agg_id")
        );
        assert_eq!(o.payload, "body");
        assert_eq!(o.id.as_deref(), Some("event_id"));
        assert_eq!(o.kind.as_deref(), Some("event_type"));

        // Unset keys take Debezium's defaults.
        let d = plan(&kafka(
            "dead_letter_topic = 'aqueduct.dlq'
[[routes]]
match = 'public.outbox'
topic_fallback = 'outbox.unknown'
outbox = {}
",
        ))
        .unwrap()
        .describe();
        assert_eq!(
            d.routes[0].outbox,
            Some(aqueduct_sinks::OutboxSpec::default())
        );
        assert_eq!(d.routes[0].topic, "outbox.event.{aggregate_type}");

        let err = |text: &str| plan(&kafka(text)).unwrap_err().to_string();
        // A misspelled outbox key fails to parse rather than falling back to a default column.
        let parsed: std::result::Result<KafkaConfig, _> = toml::from_str(
            "brokers = ['b:9092']
[[routes]]
match = 'public.outbox'
outbox = { aggregateid = 'id' }
",
        );
        assert!(parsed.unwrap_err().to_string().contains("aggregateid"));
        let e = err("dead_letter_topic = 'dlq'
[[routes]]
match = 'public.outbox'
topic = 'events'
outbox = { topic = 'outbox.events' }
");
        assert!(
            e.contains("`topic` and `outbox` are mutually exclusive"),
            "{e}"
        );
        let e = err("[[routes]]
match = 'public.outbox'
outbox = { topic = 'outbox.events' }
");
        assert!(e.contains("dead_letter_topic"), "{e}");
        let e = err("[[routes]]
match = 'public.orders'
");
        assert!(e.contains("route `public.orders` has no `topic`"), "{e}");
    }
}
