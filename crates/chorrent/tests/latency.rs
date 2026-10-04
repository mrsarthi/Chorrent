//! Throughput over a slow path: two nodes that can only reach each other
//! through the relay server (no direct connection), which is what peers on
//! mobile networks or hard NATs get. Ignored by default (slow, and depends
//! on the public relay's load); run with:
//!
//!   cargo test -p chorrent --test latency -- --ignored --nocapture

use chorrent::{Client, Endpoint, Event, PIECE_SIZE};
use iroh::endpoint::presets;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn relay_only_client() -> (Arc<Client>, tokio::task::JoinHandle<()>) {
    let endpoint = Endpoint::builder(presets::N0)
        .clear_ip_transports()
        .alpns(vec![chorrent::ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    endpoint.online().await;
    let client = Arc::new(Client::builder().endpoint(endpoint.clone()).build().await.unwrap());
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
    (client, accept)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throughput_through_the_relay() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("payload.bin");
    let size = 256 * PIECE_SIZE as usize; // 16 MiB
    std::fs::write(&file, (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>()).unwrap();

    let (seeder, _a) = relay_only_client().await;
    let seed = seeder.seed(&file).await.unwrap();
    let (leecher, _b) = relay_only_client().await;

    let out = tempfile::tempdir().unwrap();
    let download = leecher.download(&seed.share_code(), Some(out.path().to_path_buf())).await.unwrap();
    let mut events = download.events();
    // A finished download keeps reseeding, so its event stream never ends:
    // record the first piece's time and stop watching once it's done.
    let first = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let watch = tokio::spawn({
        let first = Arc::clone(&first);
        async move {
            while let Ok(event) = events.recv().await {
                match event {
                    Event::PeerPath { direct, rtt_ms, .. } => println!("path: direct={direct}, rtt {rtt_ms} ms"),
                    Event::PieceVerified { .. } => {
                        first.lock().unwrap().get_or_insert_with(Instant::now);
                    }
                    _ => {}
                }
            }
        }
    });
    let done = tokio::time::timeout(Duration::from_secs(300), download.finished()).await.unwrap().unwrap();
    let end = Instant::now();
    watch.abort();
    let start = first.lock().unwrap().expect("at least one piece");
    let secs = (end - start).as_secs_f64();
    println!("relay-only: {} MiB in {secs:.1}s = {:.2} MiB/s", size >> 20, (size as f64 / 1048576.0) / secs);
    assert_eq!(std::fs::metadata(done.path.unwrap()).unwrap().len(), size as u64);
}
