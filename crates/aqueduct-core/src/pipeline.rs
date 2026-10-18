use async_trait::async_trait;
use tokio::sync::watch;

use crate::{ChangeEvent, Lsn};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("source error: {0}")]
    Source(String),
    #[error("sink error: {0}")]
    Sink(String),
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
    #[error("config error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Produces change events, e.g. Postgres logical replication.
#[async_trait]
pub trait Source: Send {
    /// Start streaming from `start` (the last durable checkpoint, if any).
    async fn start(&mut self, start: Option<Lsn>) -> Result<()>;
    /// Next event; `Ok(None)` means the stream ended cleanly.
    async fn next(&mut self) -> Result<Option<ChangeEvent>>;
    /// Tell the source everything up to `lsn` is durably delivered.
    async fn ack(&mut self, lsn: Lsn) -> Result<()>;
}

/// Delivers change events somewhere. Events may be redelivered after a crash,
/// so sinks should tolerate duplicates.
#[async_trait]
pub trait Sink: Send {
    async fn send(&mut self, event: &ChangeEvent) -> Result<()>;
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Persists the last acknowledged LSN.
#[async_trait]
pub trait CheckpointStore: Send {
    async fn load(&mut self) -> Result<Option<Lsn>>;
    async fn save(&mut self, lsn: Lsn) -> Result<()>;
}

/// Drive `source` -> `sink` until the stream ends or `shutdown` flips to true.
/// The LSN is checkpointed and acked only after the sink succeeds.
pub async fn run_pipeline(
    source: &mut dyn Source,
    sink: &mut dyn Sink,
    store: &mut dyn CheckpointStore,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let start = store.load().await?;
    source.start(start).await?;
    loop {
        let event = tokio::select! {
            _ = shutdown.changed() => break,
            ev = source.next() => match ev? {
                Some(ev) => ev,
                None => break,
            },
        };
        sink.send(&event).await?;
        sink.flush().await?;
        store.save(event.lsn).await?;
        source.ack(event.lsn).await?;
    }
    sink.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Op;
    use std::collections::VecDeque;

    struct VecSource(VecDeque<ChangeEvent>, Vec<Lsn>);
    #[async_trait]
    impl Source for VecSource {
        async fn start(&mut self, _: Option<Lsn>) -> Result<()> {
            Ok(())
        }
        async fn next(&mut self) -> Result<Option<ChangeEvent>> {
            Ok(self.0.pop_front())
        }
        async fn ack(&mut self, lsn: Lsn) -> Result<()> {
            self.1.push(lsn);
            Ok(())
        }
    }
    struct VecSink(Vec<Lsn>);
    #[async_trait]
    impl Sink for VecSink {
        async fn send(&mut self, e: &ChangeEvent) -> Result<()> {
            self.0.push(e.lsn);
            Ok(())
        }
    }
    struct MemStore(Option<Lsn>);
    #[async_trait]
    impl CheckpointStore for MemStore {
        async fn load(&mut self) -> Result<Option<Lsn>> {
            Ok(self.0)
        }
        async fn save(&mut self, l: Lsn) -> Result<()> {
            self.0 = Some(l);
            Ok(())
        }
    }

    fn ev(n: u64) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn(n),
            xid: 1,
            commit_ts_micros: 0,
            schema: "public".into(),
            table: "t".into(),
            op: Op::Insert,
            before: None,
            after: None,
        }
    }

    #[tokio::test]
    async fn acks_after_sink() {
        let mut src = VecSource((1..=3).map(ev).collect(), vec![]);
        let mut sink = VecSink(vec![]);
        let mut store = MemStore(None);
        let (_tx, rx) = watch::channel(false);
        run_pipeline(&mut src, &mut sink, &mut store, rx)
            .await
            .unwrap();
        assert_eq!(sink.0, vec![Lsn(1), Lsn(2), Lsn(3)]);
        assert_eq!(src.1, sink.0);
        assert_eq!(store.0, Some(Lsn(3)));
    }
}
