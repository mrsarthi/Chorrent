use bao_tree::blake3::Hash;
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::proto::TopicId;
use futures_lite::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What we broadcast into the gossip room: "here's how to reach me,
/// and here's what pieces I currently have."
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Announcement {
    pub ticket: String,
    pub bitfield: Vec<bool>,
}

/// The room for a file is named after its root hash — so everyone
/// interested in the same file ends up in the same room automatically.
pub fn topic_for(root_hash: &Hash) -> TopicId {
    TopicId::from_bytes(*root_hash.as_bytes())
}

/// Keep broadcasting our own announcement, so peers who join the room
/// later still eventually hear about us.
pub async fn announce_periodically(sender: GossipSender, announcement: Announcement) {
    let bytes = postcard::to_allocvec(&announcement).expect("Announcement always serializes");
    loop {
        let _ = sender.broadcast(bytes.clone().into()).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Listen for other peers' announcements, adding each one to a shared list.
pub async fn listen_for_peers(
    mut receiver: GossipReceiver,
    known: Arc<Mutex<HashMap<String, Vec<bool>>>>,
) {
    while let Some(event) = receiver.next().await {
        let Ok(Event::Received(message)) = event else { continue };
        if let Ok(announcement) = postcard::from_bytes::<Announcement>(&message.content) {
            println!("Discovered a peer via gossip!");
            known.lock().unwrap().insert(announcement.ticket, announcement.bitfield);
        }
    }
}