//! End-to-end tests that run real nodes in one process.
//!
//! These go over the real network (Iroh relays and address lookup), so they
//! need internet access.

use chorrent::{Client, Error, Event, SeedOptions, ShareCode, PIECE_SIZE};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;

fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i * 7) as u8).wrapping_add(seed)).collect()
}

fn write(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Every file under `dir`, as (relative path, contents), sorted.
fn tree(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                out.push((path.strip_prefix(base).unwrap().to_path_buf(), std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

async fn quick_client() -> Client {
    Client::builder().discovery_timeout(Duration::from_secs(20)).build().await.unwrap()
}

fn drain(events: &mut Receiver<Event>) -> Vec<Event> {
    let mut out = Vec::new();
    while let Ok(e) = events.try_recv() {
        out.push(e);
    }
    out
}

async fn finish(download: chorrent::DownloadHandle) -> chorrent::Finished {
    tokio::time::timeout(Duration::from_secs(120), download.finished())
        .await
        .expect("download timed out")
        .expect("download failed")
}

#[tokio::test(flavor = "multi_thread")]
async fn single_file_round_trips() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("movie.bin");
    write(&file, &bytes(3 * PIECE_SIZE as usize + 1234, 1));

    let seeder = quick_client().await;
    let seed = seeder.seed(&file).await.unwrap();
    let code: ShareCode = seed.share_code().to_string().parse().unwrap();
    assert_eq!(code.name(), "movie.bin");

    let out = tempfile::tempdir().unwrap();
    let leecher = quick_client().await;
    let download = leecher.download(&code, Some(out.path().to_path_buf())).await.unwrap();
    let mut events = download.events();
    let done = finish(download).await;

    assert_eq!(done.path.clone().unwrap(), std::path::absolute(out.path().join("movie.bin")).unwrap());
    assert_eq!(std::fs::read(done.path.as_ref().unwrap()).unwrap(), std::fs::read(&file).unwrap());
    assert!(drain(&mut events).contains(&Event::Completed));
    assert!(done.seed.is_some(), "reseeding is on by default");

    // Downloading again into the same folder finds the file already there.
    let again = quick_client().await;
    let download = again.download(&code, Some(out.path().to_path_buf())).await.unwrap();
    let mut events = download.events();
    finish(download).await;
    assert!(drain(&mut events).contains(&Event::Resumed { pieces: 4, bytes: code.total_size() }));
}

#[tokio::test(flavor = "multi_thread")]
async fn folder_with_nested_and_empty_files_round_trips() {
    let src = tempfile::tempdir().unwrap();
    let root = src.path().join("album");
    write(&root.join("a.txt"), b"hello");
    write(&root.join("sub/deeper/b.bin"), &bytes(2 * PIECE_SIZE as usize + 7, 2));
    write(&root.join("sub/empty.dat"), b"");
    write(&root.join("z.bin"), &bytes(PIECE_SIZE as usize, 3));

    let seeder = quick_client().await;
    let seed = seeder.seed(&root).await.unwrap();
    let code = seed.share_code().clone();

    let out = tempfile::tempdir().unwrap();
    let leecher = quick_client().await;
    let done = finish(leecher.download(&code, Some(out.path().to_path_buf())).await.unwrap()).await;

    assert!(done.path.as_ref().unwrap().ends_with("album"));
    assert_eq!(tree(done.path.as_ref().unwrap()), tree(&root));
}

#[tokio::test(flavor = "multi_thread")]
async fn downloaders_reseed_after_the_original_seeder_leaves() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("data.bin");
    write(&file, &bytes(5 * PIECE_SIZE as usize, 4));

    let a = quick_client().await;
    let seed_a = a.seed(&file).await.unwrap();

    let b = quick_client().await;
    let b_out = tempfile::tempdir().unwrap();
    let b_done = finish(b.download(&seed_a.share_code(), Some(b_out.path().to_path_buf())).await.unwrap()).await;
    let b_seed = b_done.seed.expect("b reseeds");

    // The original seeder goes away; c can only get the file from b.
    drop(seed_a);
    a.shutdown().await;

    let c = quick_client().await;
    let c_out = tempfile::tempdir().unwrap();
    let c_done = finish(c.download(&b_seed.share_code(), Some(c_out.path().to_path_buf())).await.unwrap()).await;
    assert_eq!(std::fs::read(c_done.path.as_ref().unwrap()).unwrap(), std::fs::read(&file).unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn private_shares_need_the_secret() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("secret.bin");
    write(&file, &bytes(100_000, 5));

    let seeder = quick_client().await;
    let seed = seeder.seed_with(&file, SeedOptions::default().private(true)).await.unwrap();
    let code = seed.share_code().clone();
    assert!(code.is_private());

    // Right code: works.
    let out = tempfile::tempdir().unwrap();
    let ok = quick_client().await;
    let done = finish(ok.download(&code, Some(out.path().to_path_buf())).await.unwrap()).await;
    assert_eq!(std::fs::read(done.path.as_ref().unwrap()).unwrap(), std::fs::read(&file).unwrap());

    // Same share id and seeder address, but no secret: the seeder won't answer.
    let forged = strip_secret(code.clone());
    let snoop = Client::builder().discovery_timeout(Duration::from_secs(8)).build().await.unwrap();
    let out = tempfile::tempdir().unwrap();
    let result = snoop.download(&forged, Some(out.path().to_path_buf())).await.unwrap().finished().await;
    assert!(matches!(result, Err(Error::NoPeersFound(_))), "got {:?}", result.err());
}

/// The same share code with its secret removed. ShareCode has no setter on
/// purpose, so edit its postcard form: the secret is the trailing
/// `Option<[u8; 32]>`, encoded as tag 1 plus 32 bytes.
fn strip_secret(code: ShareCode) -> ShareCode {
    let mut bytes = postcard::to_allocvec(&code).unwrap();
    bytes.truncate(bytes.len() - 33);
    bytes.push(0);
    postcard::from_bytes(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_pull_from_several_seeders() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("big.bin");
    write(&file, &bytes(24 * PIECE_SIZE as usize, 6));

    let a = quick_client().await;
    let seed_a = a.seed(&file).await.unwrap();
    let a2 = quick_client().await;
    let _seed_a2 = a2.seed_with(&file, SeedOptions::default().join(seed_a.share_code().clone())).await.unwrap();

    // Slow enough that gossip has time to introduce the second seeder.
    let leecher = Client::builder().download_limit(Some(128 * 1024)).build().await.unwrap();
    let out = tempfile::tempdir().unwrap();
    let download = leecher.download(&seed_a.share_code(), Some(out.path().to_path_buf())).await.unwrap();
    let mut events = download.events();
    let done = finish(download).await;

    assert_eq!(std::fs::read(done.path.as_ref().unwrap()).unwrap(), std::fs::read(&file).unwrap());
    let connected = drain(&mut events)
        .into_iter()
        .filter(|e| matches!(e, Event::PeerConnected { .. }))
        .count();
    assert!(connected >= 2, "expected both seeders to be used, saw {connected}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_downloads_resume_where_they_left_off() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("resume.bin");
    write(&file, &bytes(32 * PIECE_SIZE as usize, 7));
    let seeder = quick_client().await;
    let seed = seeder.seed(&file).await.unwrap();
    let code = seed.share_code().clone();

    let data_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();

    // First run: slow, cancelled part way.
    {
        let client = Client::builder()
            .data_dir(data_dir.path())
            .download_limit(Some(256 * 1024))
            .build()
            .await
            .unwrap();
        let download = client.download(&code, Some(out.path().to_path_buf())).await.unwrap();
        let mut events = download.events();
        let mut verified = 0;
        while verified < 8 {
            if let Ok(Event::PieceVerified { .. }) = events.recv().await {
                verified += 1;
            }
        }
        download.cancel();
        assert!(matches!(download.finished().await, Err(Error::Cancelled)));
        assert_eq!(client.saved().unwrap().len(), 1, "unfinished download is remembered");
        client.shutdown().await;
    }

    // Second run: same data dir, picks up the saved download.
    let client = Client::builder().data_dir(data_dir.path()).build().await.unwrap();
    let saved = client.saved().unwrap();
    let chorrent::Transfer::Download(download) = client.resume(&saved[0]).await.unwrap() else {
        panic!("expected a download");
    };
    let mut events = download.events();
    let done = finish(download).await;
    assert_eq!(std::fs::read(done.path.as_ref().unwrap()).unwrap(), std::fs::read(&file).unwrap());

    let resumed = drain(&mut events).into_iter().find_map(|e| match e {
        Event::Resumed { pieces, .. } => Some(pieces),
        _ => None,
    });
    assert!(resumed.is_some_and(|p| p >= 8), "expected to resume with >= 8 pieces, got {resumed:?}");

    // Finished downloads become saved seeds.
    assert!(matches!(client.saved().unwrap().as_slice(), [chorrent::SavedTransfer::Seed { .. }]));
}

#[tokio::test(flavor = "multi_thread")]
async fn one_client_seeds_many_shares_but_not_the_same_twice() {
    let src = tempfile::tempdir().unwrap();
    write(&src.path().join("a"), b"aaa");
    write(&src.path().join("b"), b"bbb");
    let client = quick_client().await;

    let _a = client.seed(src.path().join("a")).await.unwrap();
    let _b = client.seed(src.path().join("b")).await.unwrap();
    assert!(matches!(client.seed(src.path().join("a")).await, Err(Error::AlreadySharing)));
}

#[test]
fn garbage_share_codes_are_rejected() {
    for bad in ["not a share code", "chr2", "chr2!!!!", "AAAA"] {
        assert!(matches!(bad.parse::<ShareCode>(), Err(Error::InvalidShareCode(_))), "{bad}");
    }
}
