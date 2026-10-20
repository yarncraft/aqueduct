//! Requests from the admin API to the running source: start or cancel a snapshot (ADR 0001 §5).
//!
//! The admin server sends a [`SnapshotRequest`] and waits for the source's reply. The source
//! drains requests between batches, so a reply can take as long as one sink round trip.

use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotCommand {
    /// Copy these `schema.table`s. Appended to a running snapshot, otherwise a new one starts.
    Start(Vec<String>),
    /// Stop the running snapshot after the chunk in flight.
    Cancel,
}

/// A command and where to send the answer: the snapshot id, or why it was refused.
#[derive(Debug)]
pub struct SnapshotRequest {
    pub command: SnapshotCommand,
    pub reply: oneshot::Sender<Result<String, String>>,
}

pub type SnapshotSender = mpsc::Sender<SnapshotRequest>;
pub type SnapshotReceiver = mpsc::Receiver<SnapshotRequest>;

/// The channel between the admin server and the source. Small: requests are rare and a full
/// channel means the source is not taking them.
pub fn snapshot_channel() -> (SnapshotSender, SnapshotReceiver) {
    mpsc::channel(8)
}
