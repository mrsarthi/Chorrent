/// Something that happened during a seed or a download.
///
/// Subscribe with `SeedHandle::events` or `DownloadHandle::events`. Only
/// events sent after subscribing are received, and a receiver that falls
/// too far behind skips the oldest ones (`RecvError::Lagged`), so treat
/// these as progress updates, not as the source of truth for the result:
/// `DownloadHandle::finished` is that.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// A peer for this file announced itself on the gossip network.
    PeerDiscovered { peer: String },
    /// We connected to a peer and learned which pieces it holds.
    PeerConnected { peer: String, pieces: usize, total: usize },
    /// A piece arrived, passed hash verification, and was written to disk.
    PieceVerified { index: usize, bytes: u64 },
    /// A piece could not be fetched or failed verification; it will be retried.
    PieceFailed { index: usize, reason: String },
    /// We served `bytes` (encoded, including proof data) to a peer.
    Uploaded { bytes: u64 },
    /// The download finished and the whole file matched its root hash.
    Completed,
}

pub(crate) const EVENT_CAPACITY: usize = 1024;
