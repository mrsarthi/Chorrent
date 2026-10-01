//! End-to-end tests that run real nodes in one process.
//!
//! These go over the real network (Iroh relays and address lookup), so they
//! need internet access.

use chorrent::{Client, Error, Event, ShareCode};
use std::io::Write;
use std::time::Duration;

fn test_file(len: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let content: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
    file.write_all(&content).unwrap();
    file
}

#[tokio::test(flavor = "multi_thread")]
async fn seed_then_download_round_trips() {
    // A few pieces plus a partial one, to exercise the last-piece edge.
    let source = test_file(3 * chorrent::PIECE_SIZE as usize + 1234);
    let out_dir = tempfile::tempdir().unwrap();
    let dest = out_dir.path().join("copy.bin");

    let seeder = Client::new().await.unwrap();
    let seed = seeder.seed(source.path()).await.unwrap();

    // Go through the text form, as a real user would.
    let code: ShareCode = seed.share_code().to_string().parse().unwrap();
    assert_eq!(&code, seed.share_code());

    let leecher = Client::new().await.unwrap();
    let download = leecher.download(&code, Some(dest.clone())).await.unwrap();
    let mut events = download.events();

    let path = tokio::time::timeout(Duration::from_secs(90), download.finished())
        .await
        .expect("download timed out")
        .expect("download failed");

    assert_eq!(path, dest);
    assert_eq!(std::fs::read(&dest).unwrap(), std::fs::read(source.path()).unwrap());

    let mut saw_completed = false;
    while let Ok(event) = events.try_recv() {
        saw_completed |= event == Event::Completed;
    }
    assert!(saw_completed, "expected a Completed event");

    seed.stop();
    leecher.shutdown().await;
    seeder.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_seed_per_client_until_stopped() {
    let a = test_file(1000);
    let b = test_file(2000);
    let client = Client::new().await.unwrap();

    let seed = client.seed(a.path()).await.unwrap();
    assert!(matches!(client.seed(b.path()).await, Err(Error::AlreadySeeding)));

    seed.stop();
    let _seed_b = client.seed(b.path()).await.expect("slot should be free after stop");
    client.shutdown().await;
}

#[test]
fn garbage_share_codes_are_rejected() {
    assert!(matches!("not a share code".parse::<ShareCode>(), Err(Error::InvalidShareCode(_))));
    assert!(matches!("AAAA".parse::<ShareCode>(), Err(Error::InvalidShareCode(_))));
}
