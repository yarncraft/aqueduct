//! Produce lanes: one per (topic, partition), so a partition's records go out in order while
//! several batches are in flight (ADR 0004).
//!
//! A lane has at most one produce request outstanding. While it waits, the parts later batches
//! hand it queue up; the next request takes all of them at once, cut into requests under the
//! byte budget. Because nothing overtakes a request in flight, rskafka's own retries of a request
//! cannot reorder a partition either. A lane stops at its first failed request: that request's
//! parts, everything queued behind it and everything handed to it later fail, so no record of a
//! later batch reaches the partition ahead of an earlier one that was never written.

use std::future::Future;

use aqueduct_core::{Error, Result};
use rskafka::record::Record;
use tokio::sync::{mpsc, oneshot};

/// One batch's records for one partition, and where to report their outcome.
struct Part {
    records: Vec<Record>,
    done: oneshot::Sender<Result<()>>,
}

/// The sending end of a lane. Dropping it ends the lane once its queue is written.
pub(crate) struct Lane {
    tx: mpsc::UnboundedSender<Part>,
}

impl Lane {
    /// Starts a lane task that writes requests with `produce`, cutting what is queued into
    /// requests with `chunk` (in order).
    pub(crate) fn spawn<P, F>(produce: P, chunk: fn(Vec<Record>) -> Vec<Vec<Record>>) -> Lane
    where
        P: FnMut(Vec<Record>) -> F + Send + 'static,
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(rx, produce, chunk));
        Lane { tx }
    }

    /// Queues `records` behind everything handed to this lane before. The future resolves once
    /// they are all acknowledged, or with the lane's failure.
    pub(crate) fn push(
        &self,
        records: Vec<Record>,
    ) -> impl Future<Output = Result<()>> + Send + 'static + use<> {
        let (done, rx) = oneshot::channel();
        // A send fails only when the task is gone; `rx` then reports it below.
        let _ = self.tx.send(Part { records, done });
        async move {
            rx.await
                .unwrap_or_else(|_| Err(Error::Sink("kafka produce lane stopped".into())))
        }
    }
}

async fn run<P, F>(
    mut rx: mpsc::UnboundedReceiver<Part>,
    mut produce: P,
    chunk: fn(Vec<Record>) -> Vec<Vec<Record>>,
) where
    P: FnMut(Vec<Record>) -> F,
    F: Future<Output = Result<()>>,
{
    // The first failure, which every later part reports too.
    let mut failed: Option<String> = None;
    while let Some(first) = rx.recv().await {
        let mut parts = vec![first];
        while let Ok(p) = rx.try_recv() {
            parts.push(p);
        }
        if let Some(why) = &failed {
            for p in parts {
                let _ = p.done.send(Err(Error::Sink(why.clone())));
            }
            continue;
        }
        // All queued records in order; a part is done once the records up to its end are acked.
        let mut records = Vec::new();
        let mut ends = Vec::with_capacity(parts.len());
        for p in parts {
            records.extend(p.records);
            ends.push((records.len(), p.done));
        }
        let mut ends = ends.into_iter().peekable();
        let mut acked = 0;
        for req in chunk(records) {
            let n = req.len();
            if let Err(e) = produce(req).await {
                let why = e.to_string();
                for (_, done) in ends.by_ref() {
                    let _ = done.send(Err(Error::Sink(why.clone())));
                }
                failed = Some(why);
                break;
            }
            acked += n;
            while let Some((_, done)) = ends.next_if(|(end, _)| *end <= acked) {
                let _ = done.send(Ok(()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn rec(n: u8) -> Record {
        Record {
            key: Some(vec![n]),
            value: None,
            headers: Default::default(),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        }
    }

    fn keys(rs: &[Record]) -> Vec<u8> {
        rs.iter().map(|r| r.key.as_ref().unwrap()[0]).collect()
    }

    /// Requests of at most two records.
    fn pairs(rs: Vec<Record>) -> Vec<Vec<Record>> {
        let mut rs = rs.into_iter().peekable();
        let mut out = Vec::new();
        while rs.peek().is_some() {
            out.push(rs.by_ref().take(2).collect());
        }
        out
    }

    /// A lane over a fake broker that logs each request and fails the request holding `bad`.
    fn lane(bad: Option<u8>) -> (Lane, Arc<Mutex<Vec<Vec<u8>>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let seen = log.clone();
        let produce = move |req: Vec<Record>| {
            let ks = keys(&req);
            let seen = seen.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let fail = bad.is_some_and(|b| ks.contains(&b));
                seen.lock().unwrap().push(ks);
                if fail {
                    return Err(Error::Sink("kafka produce to t: broker down".into()));
                }
                Ok(())
            }
        };
        (Lane::spawn(produce, pairs), log)
    }

    #[tokio::test]
    async fn queued_parts_share_requests_in_order() {
        let (lane, log) = lane(None);
        // The first part goes out alone; the next three queue behind it and go out together.
        let a = lane.push(vec![rec(1)]);
        tokio::time::sleep(Duration::from_millis(1)).await;
        let b = lane.push(vec![rec(2), rec(3)]);
        let c = lane.push(vec![rec(4)]);
        let d = lane.push(vec![rec(5)]);
        let (a, b, c, d) = tokio::join!(a, b, c, d);
        for r in [a, b, c, d] {
            r.unwrap();
        }
        assert_eq!(*log.lock().unwrap(), vec![vec![1], vec![2, 3], vec![4, 5]]);
    }

    #[tokio::test]
    async fn a_lane_stops_at_its_first_failed_request() {
        let (lane, log) = lane(Some(3));
        let a = lane.push(vec![rec(1)]);
        tokio::time::sleep(Duration::from_millis(1)).await;
        // Queued behind `a`: requests [2, 3] (fails) and [4] (never sent).
        let b = lane.push(vec![rec(2)]);
        let c = lane.push(vec![rec(3), rec(4)]);
        let (a, b, c) = tokio::join!(a, b, c);
        a.unwrap();
        // `b` shared the failed request, so it is not known to be written either.
        assert!(b.unwrap_err().to_string().contains("broker down"));
        assert!(c.unwrap_err().to_string().contains("broker down"));
        // A part handed over after the failure fails without a request.
        let e = lane.push(vec![rec(9)]).await.unwrap_err();
        assert!(e.to_string().contains("broker down"), "{e}");
        assert_eq!(*log.lock().unwrap(), vec![vec![1], vec![2, 3]]);
    }

    #[tokio::test]
    async fn a_part_completes_with_its_last_request() {
        let (lane, log) = lane(Some(5));
        // One part over three requests: [1, 2] [3, 4] [5]. The second part rides in [5] and fails.
        let a = lane.push(vec![rec(1), rec(2), rec(3), rec(4)]);
        let b = lane.push(vec![rec(5)]);
        let (a, b) = tokio::join!(a, b);
        a.unwrap();
        assert!(b.is_err());
        assert_eq!(*log.lock().unwrap(), vec![vec![1, 2], vec![3, 4], vec![5]]);
    }
}
