use aqueduct_core::{Result, Sink};
use aqueduct_sinks::{FileSink, StdoutSink, WebhookSink};

use crate::config::SinkConfig;

/// Build the sink selected in the config.
#[allow(dead_code)] // wired into run_pipeline once the Postgres source lands
pub async fn build(cfg: &SinkConfig) -> Result<Box<dyn Sink>> {
    Ok(match cfg {
        SinkConfig::Stdout => Box::new(StdoutSink::new()),
        SinkConfig::File { path } => Box::new(FileSink::open(path).await?),
        SinkConfig::Webhook { url } => Box::new(WebhookSink::new(url.clone())?),
    })
}
