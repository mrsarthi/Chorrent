use crate::error::Error;
use base64::{engine::general_purpose::STANDARD, Engine};
use iroh_tickets::endpoint::EndpointTicket;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Everything a downloader needs to find a file's swarm and verify it.
///
/// Parse one with `str::parse` and print one with `Display`; the text form is
/// what you hand to other people.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareCode {
    pub(crate) ticket: EndpointTicket,
    pub(crate) root_hash: blake3::Hash,
    pub(crate) total_size: u64,
    pub(crate) file_name: String,
}

/// The on-the-wire shape, kept identical to the pre-library CLI so existing
/// share codes still parse. Protocol v2 will replace it.
#[derive(Serialize, Deserialize)]
struct WireShareCode {
    ticket: String,
    root_hash: String,
    total_size: u64,
    file_name: String,
}

impl ShareCode {
    /// BLAKE3/Bao root hash of the file, as hex.
    pub fn root_hash(&self) -> String {
        self.root_hash.to_hex().to_string()
    }

    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// The name the seeder gave the file. Untrusted input: use
    /// [`ShareCode::safe_file_name`] when turning it into a path.
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// The file name with any directory parts stripped, so a malicious share
    /// code can't make a download land outside the current directory.
    pub fn safe_file_name(&self) -> String {
        std::path::Path::new(&self.file_name)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| "download".to_string())
    }
}

impl fmt::Display for ShareCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let wire = WireShareCode {
            ticket: self.ticket.to_string(),
            root_hash: self.root_hash.to_hex().to_string(),
            total_size: self.total_size,
            file_name: self.file_name.clone(),
        };
        let bytes = postcard::to_allocvec(&wire).map_err(|_| fmt::Error)?;
        f.write_str(&STANDARD.encode(bytes))
    }
}

impl FromStr for ShareCode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = STANDARD
            .decode(s.trim())
            .map_err(|e| Error::InvalidShareCode(format!("not valid base64: {e}")))?;
        let wire: WireShareCode = postcard::from_bytes(&bytes)
            .map_err(|e| Error::InvalidShareCode(format!("corrupted: {e}")))?;
        let root_hash = blake3::Hash::from_hex(&wire.root_hash)
            .map_err(|e| Error::InvalidShareCode(format!("bad root hash: {e}")))?;
        let ticket = wire
            .ticket
            .parse()
            .map_err(|e| Error::InvalidShareCode(format!("bad ticket: {e}")))?;
        Ok(Self { ticket, root_hash, total_size: wire.total_size, file_name: wire.file_name })
    }
}
