//! Peer-to-peer and swarm file transfer over Iroh/QUIC.
//!
//! Files are split into 64 KiB pieces, each verified against a BLAKE3/Bao
//! root hash as it arrives. Peers find each other over gossip and a download
//! pulls pieces from every peer it finds.
//!
//! ```no_run
//! # async fn demo() -> chorrent::Result<()> {
//! // Seeder
//! let client = chorrent::Client::new().await?;
//! let seed = client.seed("movie.mp4").await?;
//! println!("share this: {}", seed.share_code());
//!
//! // Downloader (another process or machine)
//! let client = chorrent::Client::new().await?;
//! let code: chorrent::ShareCode = "<share code>".parse()?;
//! let path = client.download(&code, None).await?.finished().await?;
//! # Ok(()) }
//! ```

mod chunker;
mod client;
mod discovery;
pub mod error;
mod event;
mod handler;
mod node;
mod protocol;
mod scheduler;
mod share;

pub use client::{Client, DownloadHandle, SeedHandle, SeedOptions};
pub use error::{Error, Result};
pub use event::Event;
pub use share::ShareCode;

/// Size of one piece, in bytes.
pub const PIECE_SIZE: u64 = chunker::BLOCK_SIZE.bytes() as u64;
