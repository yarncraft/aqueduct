use aqueduct_core::{Result, Sink};
use aqueduct_sinks::{FileSink, KafkaRoute, KafkaSink, StdoutSink, TopicRouter, WebhookSink};

use crate::config::SinkConfig;

/// Build the sink selected in the config.
pub async fn build(cfg: &SinkConfig) -> Result<Box<dyn Sink>> {
    Ok(match cfg {
        SinkConfig::Stdout => Box::new(StdoutSink::new()),
        SinkConfig::File { path } => Box::new(FileSink::open(path).await?),
        SinkConfig::Webhook { url } => Box::new(WebhookSink::new(url.clone())?),
        SinkConfig::Kafka {
            brokers,
            topic,
            partitions,
            routes,
        } => {
            let routes = routes
                .iter()
                .map(|r| KafkaRoute {
                    pattern: r.pattern.clone(),
                    topic: r.topic.clone(),
                })
                .collect();
            let router = TopicRouter::new(routes, topic.clone())?;
            Box::new(KafkaSink::connect(brokers.clone(), router, *partitions).await?)
        }
    })
}
