use aqueduct_core::{Error, Result};
use aqueduct_sinks::{
    FieldSpec, HashKey, KafkaInspector, KafkaRoute, KafkaSasl, KafkaSecurity, KafkaSink,
    PartitionBy, RoutePlan, SourceInfo,
};

use crate::config::{KafkaConfig, KafkaRouteConfig};

/// The error for a route that still sets `ops`, which was removed before it was released: the
/// message is the `where` that does the same.
fn removed_ops(r: &KafkaRouteConfig) -> Option<Error> {
    let ops = r.ops.as_ref()?;
    let name = r.name.as_deref().unwrap_or(&r.pattern);
    let names: Option<Vec<String>> = ops.as_array().and_then(|a| {
        a.iter()
            .map(|v| v.as_str().map(|s| format!("'{}'", s.to_ascii_lowercase())))
            .collect()
    });
    let hint = match names {
        Some(n) if !n.is_empty() => {
            let op = format!("op in [{}]", n.join(", "));
            let filter = match &r.filter {
                Some(w) => format!("({w}) and {op}"),
                None => op,
            };
            let quoted = filter.replace('\\', "\\\\").replace('"', "\\\"");
            format!(": replace it with `where = \"{quoted}\"`")
        }
        _ => ": list the operations to keep in `where` instead, e.g. `where = \"op in ['insert', 'update']\"`".into(),
    };
    Some(Error::Config(format!(
        "sink route `{name}`: `ops` was removed; filter operations in `where`{hint}"
    )))
}

fn plan(cfg: &KafkaConfig, hash_key: Option<HashKey>) -> Result<RoutePlan> {
    if let Some(e) = cfg.routes.iter().find_map(removed_ops) {
        return Err(e);
    }
    let routes = cfg
        .routes
        .iter()
        .map(|r| KafkaRoute {
            name: r.name.clone(),
            pattern: r.pattern.clone(),
            // Empty when unset; the plan requires a topic unless the route is an outbox.
            topic: r.topic.clone().unwrap_or_default(),
            filter: r.filter.clone(),
            key: r.key.clone(),
            topic_fallback: r.topic_fallback.clone(),
            topic_values: r.topic_values.clone(),
            outbox: r.outbox.as_ref().map(|o| o.spec()),
            fields: FieldSpec {
                include: r.include.clone(),
                exclude: r.exclude.clone(),
                mask: r.mask.clone(),
                hash: r.hash.clone(),
                rename: r.rename.clone(),
            },
            format: r.format.clone(),
            deletes: r.deletes.clone(),
            tombstones: r.tombstones,
            unchanged: r.unchanged.clone(),
        })
        .collect();
    let mut plan = RoutePlan::build(
        routes,
        cfg.topic.clone(),
        cfg.dead_letter_topic.clone(),
        hash_key,
    )?;
    plan.set_max_dynamic_topics(cfg.max_dynamic_topics);
    let by = partition_by(cfg)?;
    plan.check_partition_by(by)?;
    plan.set_partition_by(by);
    Ok(plan)
}

fn partition_by(cfg: &KafkaConfig) -> Result<PartitionBy> {
    match &cfg.partition_by {
        Some(s) => s.parse().map_err(Error::Config),
        None => Ok(PartitionBy::default()),
    }
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
    let hash_key = cfg
        .hash_key(std::env::var("AQUEDUCT_HASH_KEY").ok())
        .map_err(Error::Config)?;
    let plan = plan(cfg, hash_key)?;
    let security = security(cfg)?;
    let inspector = KafkaInspector::new(cfg.brokers.clone(), &plan, security.clone());
    let by = partition_by(cfg)?;
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
where = \"op in ['insert', 'update'] and row.status == 'paid'\"
topic = 'orders'
",
        );
        let d = plan(&cfg, None).unwrap().describe();
        assert_eq!(d.dead_letter_topic.as_deref(), Some("aqueduct.dlq"));
        assert_eq!(
            d.routes[0].filter.as_deref(),
            Some("op in ['insert', 'update'] and row.status == 'paid'")
        );

        // A `where` on `op`, `schema` and `table` alone needs no dead-letter topic.
        let d = plan(
            &kafka(
                "[[routes]]
match = 'public.orders'
where = \"op in ['insert', 'update']\"
topic = 'orders'
",
            ),
            None,
        )
        .unwrap()
        .describe();
        assert_eq!(d.dead_letter_topic, None);

        // `where` without a dead-letter topic, and a syntax error, fail naming the route.
        let err = |text: &str| plan(&kafka(text), None).unwrap_err().to_string();
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
where = \"op == 'upsert'\"
topic = 'orders'
");
        assert!(
            e.contains("route `public.orders`: invalid `where`")
                && e.contains("unknown op `upsert`"),
            "{e}"
        );
    }

    #[test]
    fn a_route_that_still_sets_ops_fails_with_the_equivalent_where() {
        let err = |text: &str| plan(&kafka(text), None).unwrap_err().to_string();
        let e = err("[[routes]]
name = 'orders'
match = 'public.orders'
ops = ['insert', 'Update']
topic = 'orders'
");
        assert_eq!(
            e,
            "config error: sink route `orders`: `ops` was removed; filter operations in `where`: replace it with `where = \"op in ['insert', 'update']\"`"
        );
        let e = err("dead_letter_topic = 'dlq'
[[routes]]
match = 'public.orders'
ops = ['insert']
where = 'row.note == \"a\\\\b\" or row.n > 1'
topic = 'orders'
");
        assert!(
            e.ends_with(
                r#"sink route `public.orders`: `ops` was removed; filter operations in `where`: replace it with `where = "(row.note == \"a\\\\b\" or row.n > 1) and op in ['insert']"`"#
            ),
            "{e}"
        );
        // Not a list of strings: still refused, with an example.
        for bad in ["ops = []", "ops = 'insert'", "ops = [1]"] {
            let e = err(&format!("[[routes]]\nmatch = 'p.t'\n{bad}\ntopic = 't'\n"));
            assert!(
                e.contains("`ops` was removed") && e.contains("op in ['insert', 'update']"),
                "{bad}: {e}"
            );
        }
    }

    #[test]
    fn route_keys_come_from_the_config_and_are_validated() {
        let d = plan(
            &kafka(
                "[[routes]]
match = 'public.orders'
key = ['tenant', 'id']
topic = 'orders'
",
            ),
            None,
        )
        .unwrap()
        .describe();
        assert_eq!(
            d.routes[0].key,
            Some(vec!["tenant".to_string(), "id".to_string()])
        );
        let e = plan(
            &kafka(
                "[[routes]]
match = 'public.orders'
key = ['address.city']
topic = 'orders'
",
            ),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("route `public.orders`: `key`"), "{e}");
    }

    #[test]
    fn route_field_ops_come_from_the_config_and_hashing_needs_the_key() {
        let cfg = kafka(
            "topic = 'rest'
hash_key_id = 'k1'
[[routes]]
name = 'users'
match = 'public.users'
topic = 'users'
include = ['id', 'email']
hash = ['email']
",
        );
        let e = plan(&cfg, None).unwrap_err().to_string();
        assert!(
            e.contains("route `users` uses `hash`") && e.contains("AQUEDUCT_HASH_KEY"),
            "{e}"
        );
        let key = cfg
            .hash_key(Some("0123456789abcdef0123456789abcdef".into()))
            .unwrap();
        let d = plan(&cfg, key).unwrap().describe();
        assert_eq!(d.hash_key_id.as_deref(), Some("k1"));
        assert_eq!(
            d.routes[0].include,
            Some(vec!["id".to_string(), "email".to_string()])
        );
        assert_eq!(d.routes[0].hash, Some(vec!["email".to_string()]));
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
        let d = plan(&cfg, None).unwrap().describe();
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
        let e = plan(
            &kafka(
                "[[routes]]
match = 'public.orders'
topic = 'orders.{row.region}'
",
            ),
            None,
        )
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
        ),
        None,
        )
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
        let d = plan(
            &kafka(
                "dead_letter_topic = 'aqueduct.dlq'
[[routes]]
match = 'public.outbox'
topic_fallback = 'outbox.unknown'
outbox = {}
",
            ),
            None,
        )
        .unwrap()
        .describe();
        assert_eq!(
            d.routes[0].outbox,
            Some(aqueduct_sinks::OutboxSpec::default())
        );
        assert_eq!(d.routes[0].topic, "outbox.event.{aggregate_type}");

        let err = |text: &str| plan(&kafka(text), None).unwrap_err().to_string();
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
        let e = err("dead_letter_topic = 'dlq'
[[routes]]
match = 'public.outbox'
outbox = {}
format = 'row'
tombstones = true
");
        assert!(
            e.contains("`outbox` and `format`, `tombstones` are mutually exclusive"),
            "{e}"
        );
    }

    #[test]
    fn route_shapes_come_from_the_config_and_are_validated() {
        let cfg = kafka(
            "dead_letter_topic = 'dlq'
[[routes]]
name = 'rows'
match = 'public.orders'
topic = 'orders'
format = 'row'
deletes = 'row'
unchanged = 'dead_letter'
[[routes]]
name = 'events'
match = 'public.*'
topic = 'cdc.{table}'
tombstones = true
",
        );
        let d = plan(&cfg, None).unwrap().describe();
        assert_eq!(
            (
                d.routes[0].format,
                d.routes[0].deletes,
                d.routes[0].unchanged,
                d.routes[0].tombstones
            ),
            (Some("row"), Some("row"), Some("dead_letter"), false)
        );
        assert!(d.routes[1].tombstones && d.routes[1].format.is_none());

        let err = |text: &str| plan(&kafka(text), None).unwrap_err().to_string();
        // Tombstones need a per-row key: `partition_by = "table"` without a route `key` is refused.
        let e = err("partition_by = 'table'
[[routes]]
match = 'public.*'
topic = 't'
tombstones = true
");
        assert!(
            e.contains("route `public.*` uses `tombstones`") && e.contains("per-row key"),
            "{e}"
        );
        assert!(plan(
            &kafka(
                "partition_by = 'table'
[[routes]]
match = 'public.*'
topic = 't'
key = ['id']
tombstones = true
"
            ),
            None
        )
        .is_ok());
        let e = err("[[routes]]
match = 'public.*'
topic = 't'
unchanged = 'dead_letter'
");
        assert!(e.contains("dead_letter_topic"), "{e}");
        let e = err("[[routes]]
match = 'public.*'
topic = 't'
format = 'flat'
");
        assert!(e.contains("unknown format `flat`"), "{e}");
    }
}
