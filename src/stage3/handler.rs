use crate::stage1::chunker;
use crate::stage3::protocol::{self, IncomingMessage};
use bao_tree::blake3::Hash;
use bao_tree::io::outboard::PreOrderOutboard;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use std::collections::HashSet;
use std::path::PathBuf;

/// Everything needed to answer another peer's requests for one file.
/// Router calls `accept` automatically for every incoming connection
/// that negotiates our ALPN — this replaces the old manual accept loop.
#[derive(Debug, Clone)]
pub struct ChorrentProtocol {
    pub path: PathBuf,
    pub root_hash: Hash,
    pub outboard: PreOrderOutboard<Vec<u8>>,
    pub held: HashSet<usize>,
    pub total_pieces: usize,
}

impl ProtocolHandler for ChorrentProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break, // peer disconnected
            };

            let have: Vec<bool> = (0..self.total_pieces).map(|i| self.held.contains(&i)).collect();

            match protocol::receive_message(&mut recv).await {
                Ok(IncomingMessage::BitfieldRequest) => {
                    println!("Sending bitfield ({} of {} pieces)", self.held.len(), self.total_pieces);
                    protocol::send_bitfield(&mut send, &have).await.ok();
                }
                Ok(IncomingMessage::PieceRequest(req)) => {
                    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
                    let piece_index = (req.start / chunk_size) as usize;

                    if self.held.contains(&piece_index) {
                        println!("Serving piece {} (bytes {}..{})", piece_index, req.start, req.end);
                        match chunker::serve_range(&self.path, &self.outboard, req.start, req.end) {
                            Ok(encoded) => { protocol::send_piece_response(&mut send, Some(&encoded)).await.ok(); }
                            Err(_) => { protocol::send_piece_response(&mut send, None).await.ok(); }
                        }
                    } else {
                        println!("Don't have piece {}, declining", piece_index);
                        protocol::send_piece_response(&mut send, None).await.ok();
                    }
            }
    Err(_) => break,
}
        }

        Ok(())
    }
}

/// For nodes that only download, never serve anything (yet).
#[derive(Debug, Clone)]
pub struct NullProtocol;

impl ProtocolHandler for NullProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.close(0u32.into(), b"not serving");
        Ok(())
    }
}