//! Wire protocol v2.
//!
//! Every message is a frame: a little-endian u32 length followed by that
//! many bytes of postcard. Each request opens a fresh bidirectional QUIC
//! stream (cheap, and lets many requests run in parallel on one connection).
//! Most requests get one response frame; `Subscribe` gets a bitfield and
//! then a stream of `Have` frames as the peer verifies new pieces.

use crate::error::ProtocolError;
use crate::manifest::ShareId;
use iroh::endpoint::{RecvStream, SendStream};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// The ALPN doubles as the protocol version: v1 and v2 nodes simply won't connect.
pub(crate) const ALPN: &[u8] = b"chorrent/2";

/// Big enough for a 64 KiB piece plus proof, or a large manifest.
const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    Manifest { share: ShareId, auth: Option<[u8; 32]> },
    Subscribe { share: ShareId, auth: Option<[u8; 32]> },
    Piece { share: ShareId, auth: Option<[u8; 32]>, index: u32 },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Response {
    Manifest(Vec<u8>),
    /// One bit per piece, least significant bit first.
    Bitfield(Vec<u8>),
    Have(u32),
    Piece(Vec<u8>),
    DontHave,
    /// Unknown share, or a private share and the auth token was wrong.
    /// Deliberately the same answer, so private shares can't be probed.
    NotFound,
    /// Too many uploads in progress; try again later.
    Busy,
}

pub(crate) async fn write_frame<T: Serialize>(send: &mut SendStream, msg: &T) -> Result<(), ProtocolError> {
    let body = postcard::to_allocvec(msg).map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    send.write_all(&frame).await.map_err(|e| ProtocolError::Send { message: e.to_string() })
}

/// `Ok(None)` means the stream ended cleanly between frames.
pub(crate) async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<Option<T>, ProtocolError> {
    let mut len = [0u8; 4];
    match recv.read_exact(&mut len).await {
        Ok(()) => {}
        Err(iroh::endpoint::ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(ProtocolError::Receive { message: e.to_string() }),
    }
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(ProtocolError::Receive { message: format!("frame of {len} bytes is too large") });
    }
    let mut body = vec![0u8; len];
    recv.read_exact(&mut body).await.map_err(|e| ProtocolError::Receive { message: e.to_string() })?;
    postcard::from_bytes(&body).map(Some).map_err(|e| ProtocolError::Receive { message: e.to_string() })
}

pub(crate) async fn expect_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T, ProtocolError> {
    read_frame(recv)
        .await?
        .ok_or_else(|| ProtocolError::Receive { message: "peer closed the stream".into() })
}

/// Pack a `have` list into bits, least significant bit first.
pub(crate) fn pack_bits(have: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; have.len().div_ceil(8)];
    for (i, _) in have.iter().enumerate().filter(|(_, h)| **h) {
        out[i / 8] |= 1 << (i % 8);
    }
    out
}

/// Unpack exactly `len` bits; missing bytes count as "don't have".
pub(crate) fn unpack_bits(bytes: &[u8], len: usize) -> Vec<bool> {
    (0..len).map(|i| bytes.get(i / 8).is_some_and(|b| b & (1 << (i % 8)) != 0)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_round_trip_and_tolerate_short_input() {
        let have = vec![true, false, true, true, false, false, false, false, true, false];
        assert_eq!(unpack_bits(&pack_bits(&have), have.len()), have);
        assert_eq!(unpack_bits(&[0b1], 3), vec![true, false, false]);
        assert_eq!(unpack_bits(&[], 2), vec![false, false]);
    }
}
