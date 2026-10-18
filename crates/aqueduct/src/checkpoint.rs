use std::path::PathBuf;

use aqueduct_core::{CheckpointStore, Error, Lsn, Result};
use async_trait::async_trait;

/// Stores the last acked LSN as decimal text, written atomically (tmp + rename).
pub struct FileCheckpoint {
    path: PathBuf,
}

impl FileCheckpoint {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpoint {
    async fn load(&mut self) -> Result<Option<Lsn>> {
        match tokio::fs::read_to_string(&self.path).await {
            Ok(s) => s
                .trim()
                .parse::<u64>()
                .map(|n| Some(Lsn(n)))
                .map_err(|e| Error::Checkpoint(format!("{}: {e}", self.path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Checkpoint(e.to_string())),
        }
    }

    async fn save(&mut self, lsn: Lsn) -> Result<()> {
        let map = |e: std::io::Error| Error::Checkpoint(e.to_string());
        if let Some(dir) = self.path.parent() {
            tokio::fs::create_dir_all(dir).await.map_err(map)?;
        }
        let tmp = self.path.with_extension("tmp");
        tokio::fs::write(&tmp, lsn.0.to_string())
            .await
            .map_err(map)?;
        tokio::fs::rename(&tmp, &self.path).await.map_err(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip() {
        let p = std::env::temp_dir().join(format!("aqueduct-ckpt-{}", std::process::id()));
        let mut c = FileCheckpoint::new(&p);
        assert_eq!(c.load().await.unwrap(), None);
        c.save(Lsn(42)).await.unwrap();
        assert_eq!(c.load().await.unwrap(), Some(Lsn(42)));
        let _ = std::fs::remove_file(&p);
    }
}
