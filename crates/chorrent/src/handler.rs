use crate::chunker;
use crate::event::Event;
use crate::protocol::{self, IncomingMessage};
use bao_tree::blake3::Hash;
use bao_tree::io::outboard::PreOrderOutboard;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

/// A file this node is currently willing to hand out pieces of.
#[derive(Debug)]
pub(crate) struct ServedFile {
    pub path: PathBuf,
    #[allow(dead_code)] // used once requests name the file they're for (protocol v2)
    pub root_hash: Hash,
    pub outboard: PreOrderOutboard<Vec<u8>>,
    pub held: Vec<bool>,
    pub events: broadcast::Sender<Event>,
}

/// What the node is serving right now. Requests don't yet say which file
/// they want, so a node can serve at most one file at a time.
pub(crate) type ServingSlot = Arc<RwLock<Option<Arc<ServedFile>>>>;

#[derive(Debug, Clone)]
pub(crate) struct ChorrentProtocol {
    pub serving: ServingSlot,
}

impl ProtocolHandler for ChorrentProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };

            let Some(file) = self.serving.read().unwrap().clone() else {
                connection.close(0u32.into(), b"not serving");
                break;
            };

            match protocol::receive_message(&mut recv).await {
                Ok(IncomingMessage::BitfieldRequest) => {
                    protocol::send_bitfield(&mut send, &file.held).await.ok();
                }
                Ok(IncomingMessage::PieceRequest(req)) => {
                    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
                    let piece_index = (req.start / chunk_size) as usize;

                    if file.held.get(piece_index).copied().unwrap_or(false) {
                        match chunker::serve_range(&file.path, &file.outboard, req.start, req.end) {
                            Ok(encoded) => {
                                let _ = file.events.send(Event::Uploaded { bytes: encoded.len() as u64 });
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
