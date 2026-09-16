use crate::error::ProtocolError;
use iroh::endpoint::{RecvStream, SendStream};

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

/// Everything a peer might ask us for, over one stream.
pub enum IncomingMessage {
    BitfieldRequest,
    PieceRequest(PieceRequest),
}

// ---- Asking (leecher side) ----

pub async fn request_bitfield(send: &mut SendStream) -> Result<(), ProtocolError> {
    send.write_all(&[0u8]).await
        .map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    send.finish().map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    Ok(())
}

pub async fn send_piece_request(send: &mut SendStream, request: &PieceRequest) -> Result<(), ProtocolError> {
    let mut bytes = vec![1u8];
    bytes.extend_from_slice(&request.to_bytes());
    send.write_all(&bytes).await
        .map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    send.finish().map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    Ok(())
}

// ---- Reading a question (peer side) ----

pub async fn receive_message(recv: &mut RecvStream) -> Result<IncomingMessage, ProtocolError> {
    let bytes = recv.read_to_end(17).await
        .map_err(|e| ProtocolError::Receive { message: e.to_string() })?;

    match bytes.first() {
        Some(0) => Ok(IncomingMessage::BitfieldRequest),
        Some(1) => Ok(IncomingMessage::PieceRequest(PieceRequest::from_bytes(&bytes[1..]))),
        _ => Err(ProtocolError::Receive { message: "unrecognized message tag".to_string() }),
    }
}

// ---- Answering (peer side) ----

pub async fn send_bitfield(send: &mut SendStream, have: &[bool]) -> Result<(), ProtocolError> {
    let bytes: Vec<u8> = have.iter().map(|&b| b as u8).collect();
    send.write_all(&bytes).await
        .map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    send.finish().map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    Ok(())
}

/// `None` means "I don't have that piece."
pub async fn send_piece_response(send: &mut SendStream, encoded: Option<&[u8]>) -> Result<(), ProtocolError> {
    let bytes = match encoded {
        Some(data) => {
            let mut b = vec![1u8];
            b.extend_from_slice(data);
            b
        }
        None => vec![0u8],
    };
    send.write_all(&bytes).await
        .map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    send.finish().map_err(|e| ProtocolError::Send { message: e.to_string() })?;
    Ok(())
}

// ---- Reading an answer (leecher side) ----

pub async fn receive_bitfield(recv: &mut RecvStream, total_pieces: usize) -> Result<Vec<bool>, ProtocolError> {
    let bytes = recv.read_to_end(total_pieces).await
        .map_err(|e| ProtocolError::Receive { message: e.to_string() })?;
    Ok(bytes.iter().map(|&b| b == 1).collect())
}

/// `None` means the peer told us they don't have that piece.
pub async fn receive_piece_response(recv: &mut RecvStream) -> Result<Option<Vec<u8>>, ProtocolError> {
    let bytes = recv.read_to_end(10_000_000).await
        .map_err(|e| ProtocolError::Receive { message: e.to_string() })?;

    match bytes.first() {
        Some(0) => Ok(None),
        Some(1) => Ok(Some(bytes[1..].to_vec())),
        _ => Err(ProtocolError::Receive { message: "unrecognized response tag".to_string() }),
    }
}