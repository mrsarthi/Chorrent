use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Everything that can go wrong when using a [`Client`](crate::Client).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid share code: {0}")]
    InvalidShareCode(String),

    #[error(transparent)]
    Chunker(#[from] ChunkerError),

    #[error(transparent)]
    Node(#[from] NodeError),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    #[error("failed to read {path}: {source}")]
    Io { path: PathBuf, #[source] source: std::io::Error },

    #[error("failed to join the gossip network: {0}")]
    Gossip(String),

    #[error("no peers were discovered within {0:?}")]
    NoPeersFound(Duration),

    #[error("could not connect to any discovered peer")]
    NoPeersConnected,

    #[error("downloaded file does not match the expected root hash")]
    HashMismatch,

    #[error("this client is already seeding a file (one seed per client for now)")]
    AlreadySeeding,

    #[error("the transfer was cancelled")]
    Cancelled,

    #[error("a background task failed: {0}")]
    Task(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum ChunkerError {
    #[error("failed to open {path}: {source}")]
    Open { path: PathBuf, #[source] source: std::io::Error },

    #[error("failed to hash {path}: {source}")]
    Hash { path: PathBuf, #[source] source: std::io::Error },

    #[error("failed to extract a piece from {path}: {source}")]
    Serve { path: PathBuf, #[source] source: std::io::Error },

    #[error("failed to verify/save an incoming piece to {path}: {source}")]
    Receive { path: PathBuf, #[source] source: std::io::Error },
}

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("failed to bind endpoint: {message}")]
    Bind { message: String },

    #[error("failed to connect to peer: {message}")]
    Connect { message: String },

    #[error("failed to accept incoming connection: {message}")]
    Accept { message: String },
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("failed to send protocol message: {message}")]
    Send { message: String },

    #[error("failed to receive protocol message: {message}")]
    Receive { message: String },
}
