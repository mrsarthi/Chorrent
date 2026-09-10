use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkerError {
    #[error("failed to open {path}: {source}")]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to hash {path}: {source}")]
    Hash {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}