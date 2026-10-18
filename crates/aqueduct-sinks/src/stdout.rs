use aqueduct_core::{ChangeEvent, Error, Result, Sink};
use async_trait::async_trait;
use tokio::io::{AsyncWriteExt, Stdout};

/// Writes JSON lines to stdout (the container log stream).
pub struct StdoutSink {
    out: Stdout,
}

impl StdoutSink {
    pub fn new() -> Self {
        Self { out: tokio::io::stdout() }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for StdoutSink {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()> {
        let mut line = crate::encode(event)?;
        line.push('\n');
        self.out.write_all(line.as_bytes()).await.map_err(|e| Error::Sink(e.to_string()))
    }

    async fn flush(&mut self) -> Result<()> {
        self.out.flush().await.map_err(|e| Error::Sink(e.to_string()))
    }
}
