use crate::event::Event;
use bao_tree::blake3::Hash;
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use iroh_gossip::proto::TopicId;
use futures_lite::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;

/// Just enough to find and reach a peer — no bitfield, so this stays
/// tiny regardless of how large the file itself is.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Announcement {
    pub ticket: String,
}

pub fn topic_for(root_hash: &Hash) -> TopicId {
    TopicId::from_bytes(*root_hash.as_bytes())
}

pub async fn announce_periodically(sender: GossipSender, announcement: Announcement) {
    let bytes = postcard::to_allocvec(&announcement).expect("Announcement always serializes");
    loop {
        // A failed broadcast (e.g. no neighbours yet) is retried on the next tick.
        let _ = sender.broadcast(bytes.clone().into()).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub async fn listen_for_peers(
    mut receiver: GossipReceiver,
    known: Arc<Mutex<HashSet<String>>>,
    events: broadcast::Sender<Event>,
) {
    while let Some(event) = receiver.next().await {
        let Ok(GossipEvent::Received(message)) = event else { continue };
        if let Ok(announcement) = postcard::from_bytes::<Announcement>(&message.content) {
            let peer = peer_label(&announcement.ticket);
            if known.lock().unwrap().insert(announcement.ticket) {
                let _ = events.send(Event::PeerDiscovered { peer });
            }
        }
    }
}

/// A stable, human-readable name for a peer: its endpoint id, which is the
/// same no matter how its addresses change.
pub fn peer_label(ticket: &str) -> String {
    ticket
        .parse::<iroh_tickets::endpoint::EndpointTicket>()
        .map(|t| t.endpoint_addr().id.to_string())
        .unwrap_or_else(|_| ticket.to_string())
}
