use aqueduct_core::{Result, Sink};
use aqueduct_sinks::{FileSink, KafkaSink, StdoutSink, WebhookSink};

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
        } => Box::new(KafkaSink::connect(brokers.clone(), topic.clone(), *partitions).await?),
    })
}
