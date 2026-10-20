//! The task that owns a streaming replication connection (ADR 0003 §3, #108).
//!
//! It runs whether or not the pipeline is reading. It forwards every `XLogData` and keepalive,
//! in order, into a channel bounded by bytes, and it answers the server on its own clock:
//!
//! - a standby status update every `status_interval`, and at once when a keepalive asks for a
//!   reply. Writes never wait for the channel, so a full channel or a stalled sink cannot trip
//!   the server's `wal_sender_timeout`;
//! - the LSN it reports is only ever the one [`ReplicationHandle::ack`] set. The task never
//!   decides on its own what is safe to confirm; before the first ack it reports 0, which the
//!   server ignores.
//!
//! `status_interval` must stay well below the server's `wal_sender_timeout` (60 s by default):
//! while the channel is full the timer is the only thing the server hears from us.
//!
//! When the channel is full the task stops reading the socket, so TCP flow control stalls the
//! walsender and the backlog stays on the server. A single message larger than the bound is
//! admitted on its own once the channel is empty. A keepalive that asks for a reply while the
//! task is stalled is read only when space frees up; the timer keeps the server satisfied
//! meanwhile.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{Error, Result};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use super::client::{ReplicationStream, StatusSender};
use super::frame::Replication;

/// Default bound on the bytes waiting between the connection task and the source.
pub const DEFAULT_STREAM_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Default time between standby status updates.
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);

/// Largest accepted `buffer_bytes`: what the semaphore can hand out in one acquire.
const MAX_BUFFER_BYTES: usize = u32::MAX as usize;

/// Cost charged to a keepalive: its wire size, so a flood of them is bounded too.
const KEEPALIVE_COST: usize = 18;

/// Header bytes charged to every `XLogData` on top of its payload.
const XLOGDATA_COST: usize = 25;

/// Settings of the connection task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamOptions {
    /// Bytes that may wait in the channel (`stream_buffer_bytes`). At least 1; capped at 4 GiB.
    pub buffer_bytes: usize,
    /// Time between standby status updates. Keep it well under the server's
    /// `wal_sender_timeout` (default 60 s).
    pub status_interval: Duration,
}

impl Default for StreamOptions {
    fn default() -> Self {
        StreamOptions {
            buffer_bytes: DEFAULT_STREAM_BUFFER_BYTES,
            status_interval: DEFAULT_STATUS_INTERVAL,
        }
    }
}

/// What the channel carries: a message and the channel bytes it holds until received.
type Item = Result<Option<(Replication, OwnedSemaphorePermit)>>;

/// The consumer side of a replication connection owned by a spawned task.
pub struct ReplicationHandle {
    rx: mpsc::UnboundedReceiver<Item>,
    flush: watch::Sender<u64>,
    /// Bytes in the channel. Not `capacity - available permits`: a waiting acquire already
    /// holds the permits it was assigned.
    queued: Arc<AtomicUsize>,
    task: Option<JoinHandle<()>>,
    done: bool,
}

impl ReplicationHandle {
    /// Spawns the task that owns `stream` and `status` (both halves of one session, after
    /// `START_REPLICATION`).
    pub fn spawn(stream: ReplicationStream, status: StatusSender, opts: StreamOptions) -> Self {
        let capacity = opts.buffer_bytes.clamp(1, MAX_BUFFER_BYTES);
        let budget = Arc::new(Semaphore::new(capacity));
        let (tx, rx) = mpsc::unbounded_channel();
        let (flush, flush_rx) = watch::channel(0u64);
        let queued = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(run(
            stream,
            status,
            Channel {
                tx,
                budget,
                capacity,
                queued: queued.clone(),
            },
            flush_rx,
            opts.status_interval,
        ));
        ReplicationHandle {
            rx,
            flush,
            queued,
            task: Some(task),
            done: false,
        }
    }

    /// The next message, in the order the server sent it. `Ok(None)` when the server ended the
    /// stream; an error when the connection failed. Either one is final: later calls return
    /// an error.
    ///
    /// **Cancel safety:** cancel-safe; a dropped call loses no message.
    pub async fn recv(&mut self) -> Result<Option<Replication>> {
        if self.done {
            return Err(Error::Source(
                "replication connection: the stream has already ended".into(),
            ));
        }
        let item = self.rx.recv().await;
        self.take(item)
    }

    /// Like [`Self::recv`] without waiting: `Ok(None)` when nothing is queued **or** the
    /// stream ended; [`Self::is_finished`] tells them apart.
    pub fn try_recv(&mut self) -> Result<Option<Replication>> {
        if self.done {
            return Err(Error::Source(
                "replication connection: the stream has already ended".into(),
            ));
        }
        match self.rx.try_recv() {
            Ok(item) => self.take(Some(item)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => self.take(None),
        }
    }

    fn take(&mut self, item: Option<Item>) -> Result<Option<Replication>> {
        match item {
            Some(Ok(Some((msg, permit)))) => {
                // Those bytes leave the channel when the permit drops here.
                self.queued
                    .fetch_sub(permit.num_permits(), Ordering::Relaxed);
                Ok(Some(msg))
            }
            Some(Ok(None)) => {
                self.done = true;
                Ok(None)
            }
            Some(Err(e)) => {
                self.done = true;
                Err(e)
            }
            None => {
                self.done = true;
                Err(Error::Source(
                    "replication connection: the connection task stopped".into(),
                ))
            }
        }
    }

    /// Whether the stream has ended (cleanly or with an error) and been fully received.
    pub fn is_finished(&self) -> bool {
        self.done
    }

    /// Sets the LSN the task reports as flushed in every later status update: the slot may be
    /// confirmed up to it. Only ever moves forward; a lower value is ignored.
    pub fn ack(&self, flush_lsn: u64) {
        self.flush.send_if_modified(|cur| {
            if flush_lsn > *cur {
                *cur = flush_lsn;
                true
            } else {
                false
            }
        });
    }

    /// The LSN currently reported as flushed.
    pub fn acked(&self) -> u64 {
        *self.flush.borrow()
    }

    /// Bytes currently held in the channel.
    pub fn queued_bytes(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Stops the task: it sends a last status update with the acked LSN and `Terminate`, then
    /// closes the connection.
    pub async fn shutdown(mut self) {
        self.rx.close();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ReplicationHandle {
    fn drop(&mut self) {
        // Closing the receiver makes the task send its last status update and exit; it is not
        // aborted, so that write is never cut in half.
        self.rx.close();
    }
}

fn cost(msg: &Replication) -> usize {
    match msg {
        Replication::XLogData { data, .. } => data.len() + XLOGDATA_COST,
        Replication::Keepalive { .. } => KEEPALIVE_COST,
    }
}

/// The task's end of the byte-bounded channel.
struct Channel {
    tx: mpsc::UnboundedSender<Item>,
    budget: Arc<Semaphore>,
    capacity: usize,
    queued: Arc<AtomicUsize>,
}

async fn run(
    mut stream: ReplicationStream,
    mut status: StatusSender,
    chan: Channel,
    flush: watch::Receiver<u64>,
    every: Duration,
) {
    let Channel {
        tx,
        budget,
        capacity,
        queued,
    } = chan;
    // `interval` panics on a zero period.
    let every = every.max(Duration::from_millis(1));
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // A new ack is reported within this, so the slot (and the WAL it holds) follows delivery
    // closely, without one status update per ack.
    let mut ack_tick = tokio::time::interval(ACK_REPORT_EVERY);
    ack_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut reported = 0u64;
    // A message read from the socket that waits for room in the channel. While it waits,
    // nothing else is read.
    let mut pending: Option<Replication> = None;
    let end: Result<Option<()>> = loop {
        let want = pending
            .as_ref()
            .map(|m| u32::try_from(cost(m).min(capacity)).unwrap_or(u32::MAX));
        tokio::select! {
            // Every branch future below is cancel-safe: `next_message` keeps partial frames in
            // its buffer, the semaphore releases a dropped acquire, and `closed` and `tick`
            // hold no data. Writes happen only in branch bodies, never raced.
            _ = tx.closed() => break Ok(Some(())),
            _ = ticker.tick() => {
                let lsn = *flush.borrow();
                if let Err(e) = status.send_status(lsn, false).await {
                    break Err(e);
                }
                reported = lsn;
            }
            _ = ack_tick.tick() => {
                let lsn = *flush.borrow();
                if lsn > reported {
                    if let Err(e) = status.send_status(lsn, false).await {
                        break Err(e);
                    }
                    reported = lsn;
                }
            }
            permit = budget.clone().acquire_many_owned(want.unwrap_or(0)), if want.is_some() => {
                let Ok(permit) = permit else { break Ok(Some(())) };
                if let Some(msg) = pending.take() {
                    queued.fetch_add(permit.num_permits(), Ordering::Relaxed);
                    if tx.send(Ok(Some((msg, permit)))).is_err() {
                        break Ok(Some(()));
                    }
                }
            }
            msg = stream.next_message(), if pending.is_none() => match msg {
                Ok(Some(msg)) => {
                    if let Replication::Keepalive { reply_requested: true, .. } = msg {
                        // Answer now, before the keepalive waits for room in the channel.
                        let lsn = *flush.borrow();
                        if let Err(e) = status.send_status(lsn, false).await {
                            break Err(e);
                        }
                        reported = lsn;
                    }
                    pending = Some(msg);
                }
                Ok(None) => break Ok(None),
                Err(e) => break Err(e),
            },
        }
    };
    match end {
        // The consumer is gone: report the acked position once more and leave cleanly.
        Ok(Some(())) => {
            let lsn = *flush.borrow();
            let _ = status.send_status(lsn, false).await;
            status.close().await;
        }
        // The stream ended or failed. The consumer hears it first; then, best effort and
        // bounded so a dead peer cannot hold the task, the server gets the last acked LSN.
        Ok(None) => {
            let _ = tx.send(Ok(None));
            final_status(&mut status, &flush).await;
        }
        Err(e) => {
            let _ = tx.send(Err(e));
            final_status(&mut status, &flush).await;
        }
    }
}

/// How soon a new ack is reported to the server.
const ACK_REPORT_EVERY: Duration = Duration::from_millis(200);

/// How long the last status update after the stream ended may take.
const FINAL_STATUS_TIMEOUT: Duration = Duration::from_secs(1);

async fn final_status(status: &mut StatusSender, flush: &watch::Receiver<u64>) {
    let lsn = *flush.borrow();
    let _ = tokio::time::timeout(FINAL_STATUS_TIMEOUT, status.send_status(lsn, false)).await;
}

/// Runs the connection task over `input` as the server's bytes after `START_REPLICATION`,
/// with a small channel, and drains it. Shared by the fuzz target and the randomized tests:
/// it must end with the stream's end or an error, never panic or hang.
#[doc(hidden)]
pub fn fuzz_stream(input: &[u8]) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    else {
        return;
    };
    rt.block_on(async {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let (stream, status) = super::client::streaming(Box::new(client));
        let mut h = ReplicationHandle::spawn(
            stream,
            status,
            StreamOptions {
                buffer_bytes: 256,
                status_interval: Duration::from_secs(3600),
            },
        );
        let input = input.to_vec();
        let writer = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut rd, mut wr) = tokio::io::split(&mut server);
            let drain = async {
                let mut sink = [0u8; 4096];
                while matches!(rd.read(&mut sink).await, Ok(n) if n > 0) {}
            };
            let write = async {
                let _ = wr.write_all(&input).await;
                let _ = wr.shutdown().await;
            };
            tokio::join!(drain, write);
        });
        // Until the stream ends or fails.
        while let Ok(Some(_)) = h.recv().await {}
        drop(h);
        writer.abort();
    });
}

/// The task against a scripted server byte stream (#108).
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    const WAIT: Duration = Duration::from_secs(5);

    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut m = vec![tag];
        m.extend(((body.len() + 4) as i32).to_be_bytes());
        m.extend(body);
        m
    }

    fn xlog(start: u64, payload: &[u8]) -> Vec<u8> {
        let mut w = vec![b'w'];
        w.extend(start.to_be_bytes());
        w.extend((start + 1).to_be_bytes());
        w.extend(0i64.to_be_bytes());
        w.extend(payload);
        frame(b'd', &w)
    }

    fn keepalive(wal_end: u64, reply: bool) -> Vec<u8> {
        let mut k = vec![b'k'];
        k.extend(wal_end.to_be_bytes());
        k.extend(0i64.to_be_bytes());
        k.push(u8::from(reply));
        frame(b'd', &k)
    }

    fn spawn(buffer_bytes: usize, every: Duration) -> (ReplicationHandle, DuplexStream) {
        let (client, server) = tokio::io::duplex(1 << 20);
        let (stream, status) = super::super::client::streaming(Box::new(client));
        let h = ReplicationHandle::spawn(
            stream,
            status,
            StreamOptions {
                buffer_bytes,
                status_interval: every,
            },
        );
        (h, server)
    }

    /// One frontend message from the client: tag and body.
    async fn read_frame(server: &mut DuplexStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(WAIT, async {
            let tag = server.read_u8().await.unwrap();
            let len = server.read_i32().await.unwrap() as usize;
            let mut body = vec![0; len - 4];
            server.read_exact(&mut body).await.unwrap();
            (tag, body)
        })
        .await
        .expect("no message from the client")
    }

    /// The next standby status update: (write = flush = apply LSN, reply flag).
    async fn read_status(server: &mut DuplexStream) -> (u64, bool) {
        let (tag, body) = read_frame(server).await;
        assert_eq!((tag, body.len(), body[0]), (b'd', 34, b'r'));
        let lsn = u64::from_be_bytes(body[1..9].try_into().unwrap());
        for i in 1..3 {
            assert_eq!(
                &body[1 + 8 * i..9 + 8 * i],
                &body[1..9],
                "write = flush = apply"
            );
        }
        (lsn, body[33] != 0)
    }

    async fn recv(h: &mut ReplicationHandle) -> Replication {
        tokio::time::timeout(WAIT, h.recv())
            .await
            .expect("nothing arrived")
            .unwrap()
            .expect("stream ended")
    }

    async fn wait_queued(h: &ReplicationHandle, bytes: usize) {
        tokio::time::timeout(WAIT, async {
            while h.queued_bytes() != bytes {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("queued {} bytes, expected {bytes}", h.queued_bytes()));
    }

    fn start_of(m: &Replication) -> u64 {
        match m {
            Replication::XLogData { wal_start, .. } => *wal_start,
            other => panic!("expected XLogData, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_keepalive_asking_for_a_reply_is_answered_at_once_and_forwarded_in_order() {
        let (mut h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        h.ack(0x42);
        let mut bytes = xlog(1, b"a");
        bytes.extend(keepalive(0x99, true));
        bytes.extend(xlog(2, b"b"));
        server.write_all(&bytes).await.unwrap();
        // The reply carries the acked LSN, long before the hourly timer.
        assert_eq!(read_status(&mut server).await, (0x42, false));
        assert_eq!(start_of(&recv(&mut h).await), 1);
        assert_eq!(
            recv(&mut h).await,
            Replication::Keepalive {
                wal_end: 0x99,
                server_time_micros: 0,
                reply_requested: true
            }
        );
        assert_eq!(start_of(&recv(&mut h).await), 2);
    }

    #[tokio::test]
    async fn a_keepalive_without_a_reply_request_gets_no_immediate_answer() {
        let (mut h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        server.write_all(&keepalive(5, false)).await.unwrap();
        assert!(matches!(
            recv(&mut h).await,
            Replication::Keepalive { wal_end: 5, .. }
        ));
        let mut b = [0u8; 1];
        let quiet = tokio::time::timeout(Duration::from_millis(100), server.read(&mut b)).await;
        assert!(quiet.is_err(), "the task wrote without being asked");
    }

    #[tokio::test]
    async fn a_full_channel_still_sends_status_updates() {
        // Room for one message: the first fills the channel, the task then stops reading.
        let (mut h, mut server) = spawn(100, Duration::from_millis(50));
        let mut bytes = Vec::new();
        for i in 0..5 {
            bytes.extend(xlog(i, &[7u8; 80]));
        }
        server.write_all(&bytes).await.unwrap();
        wait_queued(&h, 100).await;
        // Nothing is received, yet the timer keeps reporting.
        for _ in 0..3 {
            assert_eq!(read_status(&mut server).await, (0, false));
        }
        // An ack shows up in a later update; the task reports it but never invents one.
        h.ack(0x77);
        let mut seen = 0;
        while seen != 0x77 {
            seen = read_status(&mut server).await.0;
        }
        assert_eq!(h.queued_bytes(), 100, "still stalled");
        for i in 0..5 {
            assert_eq!(start_of(&recv(&mut h).await), i);
        }
    }

    #[tokio::test]
    async fn an_oversize_message_is_admitted_on_its_own() {
        let (mut h, mut server) = spawn(100, Duration::from_secs(3600));
        let mut bytes = xlog(1, &[1u8; 10]); // costs 35
        bytes.extend(xlog(2, &[2u8; 1000])); // larger than the whole channel
        server.write_all(&bytes).await.unwrap();
        wait_queued(&h, 35).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            h.queued_bytes(),
            35,
            "the large message must wait for an empty channel"
        );
        assert_eq!(start_of(&recv(&mut h).await), 1);
        wait_queued(&h, 100).await;
        match recv(&mut h).await {
            Replication::XLogData {
                wal_start, data, ..
            } => {
                assert_eq!((wal_start, data.len()), (2, 1000));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(h.queued_bytes(), 0);
    }

    #[tokio::test]
    async fn an_over_cap_frame_ends_the_stream_with_an_error() {
        let (mut h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        let mut bytes = xlog(1, b"ok");
        bytes.push(b'd');
        bytes.extend((super::super::frame::MAX_FRAME as i32 + 5).to_be_bytes());
        server.write_all(&bytes).await.unwrap();
        assert_eq!(start_of(&recv(&mut h).await), 1);
        let e = tokio::time::timeout(WAIT, h.recv())
            .await
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(e.contains("over the"), "{e}");
        assert!(h.is_finished());
        assert!(h.recv().await.is_err());
    }

    #[tokio::test]
    async fn server_copy_done_ends_the_stream_after_queued_messages() {
        let (mut h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        h.ack(0x31);
        let mut bytes = xlog(1, b"x");
        bytes.extend(frame(b'c', b""));
        server.write_all(&bytes).await.unwrap();
        assert_eq!(start_of(&recv(&mut h).await), 1);
        assert_eq!(
            tokio::time::timeout(WAIT, h.recv()).await.unwrap().unwrap(),
            None
        );
        assert!(h.is_finished());
        // The server still gets the last acked LSN.
        assert_eq!(read_status(&mut server).await, (0x31, false));
    }

    #[tokio::test]
    async fn a_failed_stream_still_reports_the_last_ack() {
        let (mut h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        h.ack(0x64);
        server.write_all(&frame(b'Z', b"I")).await.unwrap();
        assert!(tokio::time::timeout(WAIT, h.recv()).await.unwrap().is_err());
        assert_eq!(read_status(&mut server).await, (0x64, false));
    }

    #[tokio::test]
    async fn acks_only_move_forward() {
        let (h, mut server) = spawn(1 << 20, Duration::from_millis(30));
        h.ack(10);
        h.ack(5);
        assert_eq!(h.acked(), 10);
        let mut lsn = 0;
        while lsn == 0 {
            lsn = read_status(&mut server).await.0;
        }
        assert_eq!(lsn, 10);
    }

    #[tokio::test]
    async fn shutting_down_reports_the_ack_and_terminates() {
        let (h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        h.ack(0x55);
        h.shutdown().await;
        assert_eq!(read_status(&mut server).await, (0x55, false));
        assert_eq!(read_frame(&mut server).await, (b'X', vec![]));
        let mut rest = Vec::new();
        server.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
    }

    #[tokio::test]
    async fn dropping_the_handle_stops_the_task() {
        let (h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        drop(h);
        assert_eq!(read_status(&mut server).await, (0, false));
        assert_eq!(read_frame(&mut server).await.0, b'X');
    }

    #[tokio::test]
    async fn try_recv_does_not_wait() {
        let (mut h, mut server) = spawn(1 << 20, Duration::from_secs(3600));
        assert_eq!(h.try_recv().unwrap(), None);
        assert!(!h.is_finished());
        server.write_all(&xlog(3, b"z")).await.unwrap();
        wait_queued(&h, 26).await;
        assert_eq!(
            h.try_recv().unwrap(),
            Some(Replication::XLogData {
                wal_start: 3,
                wal_end: 4,
                server_time_micros: 0,
                data: Bytes::from_static(b"z")
            })
        );
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// The stable counterpart of the `stream_task` fuzz target: random mixes of well-formed
    /// XLogData and keepalives (some asking for replies), malformed CopyData, other messages
    /// and over-cap or truncated frames. The task must end with the stream's end or an error.
    #[test]
    fn random_server_streams_end_cleanly() {
        let mut rng = Rng(0xA076_1D64_78BD_642F);
        for _ in 0..200 {
            let mut bytes = Vec::new();
            for _ in 0..(rng.next() % 12) {
                match rng.next() % 9 {
                    0..=2 => {
                        let n = (rng.next() % 600) as usize;
                        bytes.extend(xlog(rng.next(), &vec![rng.next() as u8; n]));
                    }
                    3 | 4 => bytes.extend(keepalive(rng.next(), rng.next().is_multiple_of(2))),
                    5 => {
                        let n = (rng.next() % 30) as usize;
                        let junk: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
                        bytes.extend(frame(b'd', &junk));
                    }
                    6 => bytes.extend(frame(b"NSAZC"[(rng.next() % 5) as usize], b"\0")),
                    7 => {
                        bytes.push(b'd');
                        bytes.extend((rng.next() as i32).to_be_bytes());
                    }
                    _ => bytes.extend(frame(b'c', b"")),
                }
            }
            let cut = (rng.next() as usize) % (bytes.len() + 1);
            if rng.next().is_multiple_of(3) {
                bytes.truncate(cut);
            }
            fuzz_stream(&bytes);
        }
    }
}
