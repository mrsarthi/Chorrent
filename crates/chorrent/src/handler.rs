//! Answers other peers: hands out manifests, progress and pieces for every
//! share this node holds.

use crate::event::Event;
use crate::limits::RateLimiter;
use crate::local::SharedShare;
use crate::manifest::ShareId;
use crate::protocol::{self, Request, Response};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::EndpointId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinSet;

/// How long a piece request may wait for a free upload slot.
const UPLOAD_SLOT_WAIT: Duration = Duration::from_secs(10);

/// Every share this node is currently serving, by id.
pub(crate) type Registry = Arc<RwLock<HashMap<ShareId, SharedShare>>>;

#[derive(Debug, Clone)]
pub(crate) struct ChorrentProtocol {
    pub shares: Registry,
    pub upload_slots: Arc<Semaphore>,
    pub upload_rate: Option<Arc<RateLimiter>>,
}

impl ProtocolHandler for ChorrentProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote = connection.remote_id();
        // Streams are served concurrently; they're all aborted when the
        // connection ends and this set is dropped.
        let mut streams = JoinSet::new();
        while let Ok((send, recv)) = connection.accept_bi().await {
            let this = self.clone();
            streams.spawn(async move { this.serve_stream(remote, send, recv).await });
            while streams.try_join_next().is_some() {}
        }
        Ok(())
    }
}

impl ChorrentProtocol {
    fn lookup(&self, share: &ShareId, remote: &EndpointId, auth: Option<&[u8; 32]>) -> Option<SharedShare> {
        let shares = self.shares.read().unwrap();
        shares.get(share).filter(|s| s.authorized(remote, auth)).cloned()
    }

    fn still_registered(&self, share: &SharedShare) -> bool {
        self.shares.read().unwrap().get(&share.id).is_some_and(|s| Arc::ptr_eq(s, share))
    }

    async fn serve_stream(&self, remote: EndpointId, mut send: SendStream, mut recv: RecvStream) {
        let Ok(request) = protocol::expect_frame::<Request>(&mut recv).await else { return };
        let response = match request {
            Request::Manifest { share, auth } => match self.lookup(&share, &remote, auth.as_ref()) {
                Some(s) => Response::Manifest(s.manifest_bytes.clone()),
                None => Response::NotFound,
            },
            Request::Subscribe { share, auth } => match self.lookup(&share, &remote, auth.as_ref()) {
                Some(s) => return self.serve_subscription(s, send).await,
                None => Response::NotFound,
            },
            Request::Piece { share, auth, index } => match self.lookup(&share, &remote, auth.as_ref()) {
                Some(s) => self.serve_piece(s, index as usize).await,
                None => Response::NotFound,
            },
        };
        if protocol::write_frame(&mut send, &response).await.is_ok() {
            let _ = send.finish();
            // Wait for the peer to read it; dropping early could reset the stream.
            let _ = send.stopped().await;
        }
    }

    async fn serve_piece(&self, share: SharedShare, index: usize) -> Response {
        if index >= share.total_pieces() || !share.has(index) {
            return Response::DontHave;
        }
        // Wait a while for a free upload slot rather than refusing at once: a
        // distant downloader keeps many requests queued, and refusing them
        // would only cause retries.
        let Ok(Ok(_permit)) = tokio::time::timeout(UPLOAD_SLOT_WAIT, self.upload_slots.acquire()).await else {
            return Response::Busy;
        };
        if let Some(limiter) = &self.upload_rate {
            limiter.acquire(share.piece_len(index)).await;
        }
        let encoded = {
            let share = Arc::clone(&share);
            tokio::task::spawn_blocking(move || share.encode_piece(index)).await
        };
        match encoded {
            Ok(Ok(bytes)) => {
                let _ = share.events.send(Event::Uploaded { bytes: bytes.len() as u64 });
                Response::Piece(bytes)
            }
            _ => Response::DontHave,
        }
    }

    /// Send our bitfield, then every piece we verify from now on, until the
    /// peer goes away or this share stops being served.
    async fn serve_subscription(&self, share: SharedShare, mut send: SendStream) {
        let mut updates = share.have_tx.subscribe(); // before the snapshot, so nothing is missed
        if protocol::write_frame(&mut send, &Response::Bitfield(share.packed_bitfield())).await.is_err() {
            return;
        }
        let mut check = tokio::time::interval(Duration::from_secs(5));
        loop {
            let msg = tokio::select! {
                update = updates.recv() => match update {
                    Ok(piece) => Response::Have(piece),
                    // Fell behind: resend everything instead of individual pieces.
                    Err(broadcast::error::RecvError::Lagged(_)) => Response::Bitfield(share.packed_bitfield()),
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = check.tick() => {
                    if !self.still_registered(&share) { break }
                    continue;
                }
            };
            if protocol::write_frame(&mut send, &msg).await.is_err() {
                return;
            }
        }
        let _ = send.finish();
    }
}
