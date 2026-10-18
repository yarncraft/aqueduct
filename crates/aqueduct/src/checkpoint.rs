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
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || write_durable(&path, lsn.0))
            .await
            .map_err(|e| Error::Checkpoint(e.to_string()))?
            .map_err(|e| Error::Checkpoint(e.to_string()))
    }
}

/// tmp file + fsync + rename + fsync of the directory, so a power loss leaves either the old
/// or the new value, never a torn or empty file.
fn write_durable(path: &std::path::Path, value: u64) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    if let Some(dir) = dir {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(value.to_string().as_bytes())?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)?;
    #[cfg(unix)]
    if let Some(dir) = dir {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
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
