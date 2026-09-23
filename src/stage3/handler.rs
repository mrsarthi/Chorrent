use crate::stage1::chunker;
use crate::stage3::protocol::{self, IncomingMessage};
use bao_tree::blake3::Hash;
use bao_tree::io::outboard::PreOrderOutboard;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ChorrentProtocol {
    pub path: PathBuf,
    pub root_hash: Hash,
    pub outboard: PreOrderOutboard<Vec<u8>>,
    pub held: HashSet<usize>,
    pub total_pieces: usize,
    /// Running total of bytes served — shared with whoever's displaying
    /// the live upload status outside this handler.
    pub bytes_served: Arc<AtomicU64>,
}

impl ProtocolHandler for ChorrentProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };

            let have: Vec<bool> = (0..self.total_pieces).map(|i| self.held.contains(&i)).collect();

            match protocol::receive_message(&mut recv).await {
                Ok(IncomingMessage::BitfieldRequest) => {
                    protocol::send_bitfield(&mut send, &have).await.ok();
                }
                Ok(IncomingMessage::PieceRequest(req)) => {
                    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
                    let piece_index = (req.start / chunk_size) as usize;

                    if self.held.contains(&piece_index) {
                        match chunker::serve_range(&self.path, &self.outboard, req.start, req.end) {
                            Ok(encoded) => {
                                self.bytes_served.fetch_add(encoded.len() as u64, Ordering::Relaxed);
                                protocol::send_piece_response(&mut send, Some(&encoded)).await.ok();
                            }
                            Err(_) => { protocol::send_piece_response(&mut send, None).await.ok(); }
                        }
                    } else {
                        protocol::send_piece_response(&mut send, None).await.ok();
                    }
                }
                Err(_) => break,
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct NullProtocol;

impl ProtocolHandler for NullProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.close(0u32.into(), b"not serving");
        Ok(())
    }
}