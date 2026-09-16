use crate::error::ProtocolError;
use iroh::endpoint::{RecvStream, SendStream};

/// A request for one piece of a file: "give me bytes start..end".
pub struct PieceRequest {
    pub start: u64,
    pub end: u64,
}

impl PieceRequest {
    fn to_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.start.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.end.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let start = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let end = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Self { start, end }
    }
}

/// Ask a peer for one piece, over a stream you've already opened.
pub async fn send_request(send: &mut SendStream, request: &PieceRequest) -> Result<(), ProtocolError> {
    send.write_all(&request.to_bytes()).await
        .map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    send.finish().map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    Ok(())
}

/// Read an incoming piece request, on a stream you've already accepted.
pub async fn receive_request(recv: &mut RecvStream) -> Result<PieceRequest, ProtocolError> {
    let bytes = recv.read_to_end(16).await
        .map_err(|e| ProtocolError::Receive { message: e.to_string() })?;
    Ok(PieceRequest::from_bytes(&bytes))
}

/// Send back a piece's data (already produced by `chunker::serve_range`).
pub async fn send_response(send: &mut SendStream, encoded: &[u8]) -> Result<(), ProtocolError> {
    send.write_all(encoded).await
        .map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    send.finish().map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    Ok(())
}

/// Read an incoming piece response.
pub async fn receive_response(recv: &mut RecvStream) -> Result<Vec<u8>, ProtocolError> {
    recv.read_to_end(10_000_000).await
        .map_err(|e| ProtocolError::Receive { message: e.to_string() })
}