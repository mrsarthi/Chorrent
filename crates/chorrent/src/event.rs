/// Something that happened during a seed or a download.
///
/// Subscribe with `SeedHandle::events` or `DownloadHandle::events`. Only
/// events sent after subscribing are received, and a receiver that falls
/// too far behind skips the oldest ones (`RecvError::Lagged`), so treat
/// these as progress updates, not as the source of truth for the result:
/// `DownloadHandle::finished` is that.
///
/// Peers are identified by their endpoint id, as a string.
///
/// Events serialize with serde (tagged by `"type"`), so a UI in another
/// process or language can receive them as JSON.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Event {
    /// We learned about a peer for this share (from the share code or gossip).
    PeerDiscovered { peer: String },
    /// We got the share's file list and verified it. Sent once, before any pieces.
    ManifestReceived { name: String, files: usize, total_size: u64, total_pieces: usize },
    /// Pieces already on disk from an earlier, interrupted download.
    Resumed { pieces: usize, bytes: u64 },
    /// We connected to a peer and learned which pieces it holds.
    PeerConnected { peer: String, pieces: usize, total: usize },
    /// How we reach a peer: `direct` (hole punched, fastest) or through a
    /// relay server (works everywhere, but slower). Sent when known and
    /// again whenever it changes.
    PeerPath { peer: String, direct: bool, rtt_ms: u64 },
    /// We stopped using a peer (it left, or kept failing).
    PeerDisconnected { peer: String },
    /// A piece arrived, passed hash verification, and was written to disk.
    PieceVerified { index: usize, bytes: u64 },
    /// A piece could not be fetched or failed verification; it will be retried.
    PieceFailed { index: usize, reason: String },
    /// We served `bytes` (encoded, including proof data) to a peer.
    Uploaded { bytes: u64 },
    /// The download finished and every file matched its root hash.
    Completed,
}

pub(crate) const EVENT_CAPACITY: usize = 4096;
