use bao_tree::blake3::Hash;
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::proto::TopicId;
use futures_lite::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
        if let Err(e) = sender.broadcast(bytes.clone().into()).await {
            eprintln!("Gossip broadcast failed: {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub async fn listen_for_peers(mut receiver: GossipReceiver, known: Arc<Mutex<HashSet<String>>>) {
    while let Some(event) = receiver.next().await {
        let Ok(Event::Received(message)) = event else { continue };
        if let Ok(announcement) = postcard::from_bytes::<Announcement>(&message.content) {
            println!("Discovered a peer via gossip!");
            known.lock().unwrap().insert(announcement.ticket);
        }
    }
}