use iroh::EndpointAddr;
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use futures_lite::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;

/// "I hold (some of) this share, reach me here." Stays tiny regardless of
/// the share's size; what we actually hold is asked for directly.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Announcement {
    pub addr: EndpointAddr,
}

pub(crate) async fn announce_periodically(sender: GossipSender, announcement: Announcement) {
    let bytes = postcard::to_allocvec(&announcement).expect("Announcement always serializes");
    loop {
        // A failed broadcast (e.g. no neighbours yet) is retried on the next tick.
        let _ = sender.broadcast(bytes.clone().into()).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Forward every announced peer address to `found` (when given). Repeats
/// are forwarded too; the downloader decides whether to (re)connect.
pub(crate) async fn listen_for_peers(mut receiver: GossipReceiver, found: Option<mpsc::Sender<EndpointAddr>>) {
    while let Some(event) = receiver.next().await {
        let Ok(GossipEvent::Received(message)) = event else { continue };
        let (Some(found), Ok(announcement)) = (&found, postcard::from_bytes::<Announcement>(&message.content)) else {
            continue;
        };
        let _ = found.try_send(announcement.addr);
    }
}
