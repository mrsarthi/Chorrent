//! Swarms where peers can't be looked up by id: the situation of a device
//! whose address lookup fails (it happened on a real three-machine test).
//! Every node here has address lookup switched off, so a peer can only be
//! reached through addresses it was actually given. Needs internet access.

use chorrent::{Client, Endpoint, Event, SeedOptions, PIECE_SIZE};
use iroh::endpoint::presets;
use std::sync::Arc;
use std::time::Duration;

struct Node {
    client: Arc<Client>,
    _accept: tokio::task::JoinHandle<()>,
}

async fn node(download_limit: Option<u64>) -> Node {
    let endpoint = Endpoint::builder(presets::N0)
        .clear_address_lookup()
        .alpns(vec![chorrent::ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    endpoint.online().await;
    let client = Arc::new(
        Client::builder()
            .endpoint(endpoint.clone())
            .gossip(true)
            .download_limit(download_limit)
            .discovery_timeout(Duration::from_secs(20))
            .build()
            .await
            .unwrap(),
    );
    assert_eq!(client.alpns().len(), 2, "chorrent + gossip");
    let accept = tokio::spawn({
        let client = Arc::clone(&client);
        async move {
            while let Some(incoming) = endpoint.accept().await {
                let client = Arc::clone(&client);
                tokio::spawn(async move {
                    if let Ok(connection) = incoming.await {
                        client.handle_connection(connection).await;
                    }
                });
            }
        }
    });
    Node { client, _accept: accept }
}

fn content(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 17) % 251) as u8).collect()
}

/// Download `code` on `node` and return how many distinct peers it used.
async fn peers_used(node: &Node, code: &chorrent::ShareCode, expected: &[u8]) -> usize {
    let download = node.client.download(code, Some(tempfile::tempdir().unwrap().keep())).await.unwrap();
    let mut events = download.events();
    let done = tokio::time::timeout(Duration::from_secs(150), download.finished()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(done.path.unwrap()).unwrap(), expected);
    let mut peers = std::collections::HashSet::new();
    while let Ok(event) = events.try_recv() {
        if let Event::PeerConnected { peer, .. } = event {
            peers.insert(peer);
        }
    }
    peers.len()
}

#[tokio::test(flavor = "multi_thread")]
async fn joining_seeder_is_found_without_id_lookup() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("video.bin");
    let data = content(40 * PIECE_SIZE as usize);
    std::fs::write(&file, &data).unwrap();

    let a = node(None).await;
    let seed_a = a.client.seed(&file).await.unwrap();
    let b = node(None).await;
    let _seed_b = b.client.seed_with(&file, SeedOptions::default().join(seed_a.share_code())).await.unwrap();
    // Let B join A's swarm and announce itself.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // C only has A's code, and must still find and use B: that only works
    // if B really joined A's swarm (it couldn't before the fix).
    let c = node(Some(512 * 1024)).await;
    assert_eq!(peers_used(&c, &seed_a.share_code(), &data).await, 2, "C should use both seeders");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_joined_seeders_code_reaches_every_seeder() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("video.bin");
    let data = content(24 * PIECE_SIZE as usize);
    std::fs::write(&file, &data).unwrap();

    let a = node(None).await;
    let seed_a = a.client.seed(&file).await.unwrap();
    let b = node(None).await;
    let seed_b = b.client.seed_with(&file, SeedOptions::default().join(seed_a.share_code())).await.unwrap();
    assert_eq!(seed_b.share_code().peer_ids().len(), 2, "B's code lists B and A");

    // C only has B's code: it should reach A directly from it, too.
    let c = node(Some(512 * 1024)).await;
    assert_eq!(peers_used(&c, &seed_b.share_code(), &data).await, 2);
}
