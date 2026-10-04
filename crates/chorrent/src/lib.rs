//! Peer-to-peer and swarm file transfer over Iroh/QUIC.
//!
//! Share a file or folder and get a share code; anyone with the code can
//! download it. Files are split into 64 KiB pieces, each verified against a
//! BLAKE3/Bao hash as it arrives, and pulled from every peer in the swarm at
//! once. Downloaders serve what they have to each other (reseeding), and with
//! a data dir, downloads resume after a restart.
//!
//! ```no_run
//! # async fn demo() -> chorrent::Result<()> {
//! // Seeder
//! let client = chorrent::Client::new().await?;
//! let seed = client.seed("holiday-photos/").await?;
//! println!("share this: {}", seed.share_code());
//!
//! // Downloader (another process or machine)
//! let client = chorrent::Client::new().await?;
//! let code: chorrent::ShareCode = "chr2...".parse()?;
//! let download = client.download(&code, None).await?;
//! let mut events = download.events(); // progress, peers, ...
//! let done = download.finished().await?;
//! if let Some(path) = &done.path {
//!     println!("saved to {}", path.display());
//! }
//! # Ok(()) }
//! ```

mod chunker;
mod client;
mod discovery;
mod download;
/// Error types. [`Error`] is the one most callers need.
pub mod error;
mod event;
mod handler;
mod limits;
mod local;
mod manifest;
mod node;
mod protocol;
mod registration;
mod scheduler;
mod share;
mod storage;
mod store;

pub use client::{
    Client, ClientBuilder, DownloadHandle, DownloadOptions, Finished, NetworkStatus, PeerCheck, SeedHandle,
    SeedOptions, Transfer,
};
/// Re-exported so embedding apps name the same types (keep your iroh
/// dependency semver-compatible with chorrent's).
pub use iroh::{Endpoint, EndpointId};
pub use error::{Error, Result};
pub use event::Event;
pub use manifest::{FileEntry, Manifest, ShareId};
pub use share::ShareCode;
pub use store::SavedTransfer;

/// The ALPN chorrent's protocol uses; see [`Client::alpns`] for the full list.
pub const ALPN: &[u8] = protocol::ALPN;

/// Size of one piece, in bytes.
pub const PIECE_SIZE: u64 = chunker::BLOCK_SIZE.bytes() as u64;
