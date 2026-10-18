use std::path::Path;

use aqueduct_core::{ChangeEvent, Error, Result, Sink};
use async_trait::async_trait;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

/// Appends JSON lines to a file. `flush` fsyncs, so the pipeline only
/// checkpoints events that are on disk.
pub struct FileSink {
    file: File,
}

impl FileSink {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| Error::Sink(format!("open: {e}")))?;
        Ok(Self { file })
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()> {
        let mut line = crate::encode(event)?;
        line.push('\n');
        self.file
            .write_all(line.as_bytes())
            .await
            .map_err(|e| Error::Sink(e.to_string()))
    }

    async fn flush(&mut self) -> Result<()> {
        self.file
            .flush()
            .await
            .map_err(|e| Error::Sink(e.to_string()))?;
        self.file
            .sync_data()
            .await
            .map_err(|e| Error::Sink(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn appends_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        let mut sink = FileSink::open(&path).await.unwrap();
        sink.send(&crate::sample()).await.unwrap();
        sink.send(&crate::sample()).await.unwrap();
        sink.flush().await.unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let back: ChangeEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(back, crate::sample());
    }
}
