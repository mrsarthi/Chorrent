use std::path::PathBuf;
use thiserror::Error;

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