//! Keeping a share's swarm together over gossip: joining it, staying in it,
//! announcing ourselves, and hearing about other peers.

use crate::error::{Error, Result};
use crate::event::Event;
use crate::node::ChorrentNode;
use futures_lite::stream::StreamExt;
use iroh::endpoint::Connection;
use iroh::{EndpointAddr, EndpointId};
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::Instant;

const ANNOUNCE_EVERY: Duration = Duration::from_secs(5);
/// While we have no swarm neighbours, try to (re)join this often.
const REJOIN_EVERY: Duration = Duration::from_secs(15);
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// "I hold (some of) this share, reach me here." Stays tiny regardless of
/// the share's size; what we actually hold is asked for directly.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Announcement {
    pub addr: EndpointAddr,
}

/// Connect to peers using their full addresses, so a later dial by id alone
/// (which is how gossip dials) finds them even when looking an id up fails.
/// The returned connections keep those paths warm; drop them when done.
async fn predial(node: &Arc<ChorrentNode>, peers: &[EndpointAddr]) -> Vec<Connection> {
    let mut dials = tokio::task::JoinSet::new();
    for peer in peers.iter().filter(|p| p.id != node.id() && !p.is_empty()) {
        let (node, peer) = (Arc::clone(node), peer.clone());
        dials.spawn(async move { tokio::time::timeout(DIAL_TIMEOUT, node.connect(peer)).await.ok()?.ok() });
    }
    dials.join_all().await.into_iter().flatten().collect()
}

/// Everything a swarm keeper needs.
pub(crate) struct Swarm {
    pub node: Arc<ChorrentNode>,
    pub topic: TopicId,
    /// Peers to (re)join through, with full addresses.
    pub bootstrap: Vec<EndpointAddr>,
    /// Whether to announce ourselves (a downloader turns this on once it
    /// has something to share).
    pub announce: Arc<AtomicBool>,
    /// Where to send peers we hear about (downloads want them; seeds don't).
    pub found: Option<mpsc::Sender<EndpointAddr>>,
    pub events: broadcast::Sender<Event>,
}

/// Join the share's gossip topic. Returns once subscribed (joining carries
/// on in the background); then run [`keep`] for as long as we're in it.
pub(crate) async fn join(swarm: &Swarm) -> Result<(GossipSender, GossipReceiver, Vec<Connection>)> {
    let gossip = swarm.node.gossip().ok_or_else(|| Error::Gossip("gossip is disabled".into()))?;
    let warm = predial(&swarm.node, &swarm.bootstrap).await;
    let ids = swarm.bootstrap.iter().map(|p| p.id).filter(|id| *id != swarm.node.id()).collect();
    let (sender, receiver) = gossip
        .subscribe(swarm.topic, ids)
        .await
        .map_err(|e| Error::Gossip(e.to_string()))?
        .split();
    Ok((sender, receiver, warm))
}

/// Stay in the swarm: announce with our current address, re-join when we
/// have no neighbours, pass on peers we hear about, and report status.
pub(crate) async fn keep(swarm: Swarm, sender: GossipSender, mut receiver: GossipReceiver, mut warm: Vec<Connection>) {
    let me = swarm.node.id();
    let mut neighbours: HashSet<EndpointId> = HashSet::new();
    // Everyone we've heard announce, as possible re-join points.
    let mut known: HashMap<EndpointId, EndpointAddr> =
        swarm.bootstrap.iter().filter(|p| p.id != me).map(|p| (p.id, p.clone())).collect();
    let mut announce = tokio::time::interval(ANNOUNCE_EVERY);
    let mut last_rejoin = Instant::now();
    let _ = swarm.events.send(Event::SwarmPeers { connected: 0 });

    loop {
        tokio::select! {
            event = receiver.next() => {
                let Some(event) = event else { break };
                let before = neighbours.len();
                match event {
                    Ok(GossipEvent::NeighborUp(id)) => { neighbours.insert(id); }
                    Ok(GossipEvent::NeighborDown(id)) => { neighbours.remove(&id); }
                    Ok(GossipEvent::Received(message)) => {
                        if let Ok(announcement) = postcard::from_bytes::<Announcement>(&message.content)
                            && announcement.addr.id != me
                        {
                            known.insert(announcement.addr.id, announcement.addr.clone());
                            if let Some(found) = &swarm.found {
                                let _ = found.try_send(announcement.addr);
                            }
                        }
                    }
                    _ => {}
                }
                if neighbours.len() != before {
                    let _ = swarm.events.send(Event::SwarmPeers { connected: neighbours.len() });
                    if !neighbours.is_empty() {
                        warm.clear(); // gossip has its own connections now
                    }
                }
            }
            _ = announce.tick() => {
                if swarm.announce.load(Ordering::Relaxed) {
                    // Our address may have changed (e.g. the relay connected late).
                    let bytes = postcard::to_allocvec(&Announcement { addr: swarm.node.addr() })
                        .expect("Announcement always serializes");
                    let _ = sender.broadcast(bytes.into()).await;
                }
                if neighbours.is_empty() && !known.is_empty() && last_rejoin.elapsed() >= REJOIN_EVERY {
                    last_rejoin = Instant::now();
                    let peers: Vec<EndpointAddr> = known.values().cloned().collect();
                    warm = predial(&swarm.node, &peers).await;
                    let _ = sender.join_peers(peers.iter().map(|p| p.id).collect()).await;
                }
            }
        }
    }
}
