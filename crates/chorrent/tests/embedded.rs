//! Chorrent embedded the way EchoIt will use it: the app owns the iroh
//! endpoint and its accept loop, and everything received is stored
//! encrypted. Needs internet access (iroh relays).

use chorrent::{Client, DownloadOptions, Endpoint, Error, SeedOptions, PIECE_SIZE};
use iroh::endpoint::presets;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const APP_ALPN: &[u8] = b"dicsussion/1";

/// An "app": its own endpoint (with its own protocol plus chorrent's), its
/// own accept loop, and a chorrent client attached to it.
struct App {
    endpoint: Endpoint,
    client: Arc<Client>,
    data: tempfile::TempDir,
    accept_loop: tokio::task::JoinHandle<()>,
    /// The app's identity, to restart it as the same peer (like EchoIt does).
    secret: iroh::SecretKey,
}

async fn app(key: [u8; 32]) -> App {
    let data = tempfile::tempdir().unwrap();
    app_in(key, data).await
}

async fn app_in(key: [u8; 32], data: tempfile::TempDir) -> App {
    app_with(key, data, None).await
}

async fn app_with(key: [u8; 32], data: tempfile::TempDir, download_limit: Option<u64>) -> App {
    app_as(key, data, download_limit, iroh::SecretKey::generate()).await
}

async fn app_as(key: [u8; 32], data: tempfile::TempDir, download_limit: Option<u64>, secret: iroh::SecretKey) -> App {
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret.clone())
        .alpns(vec![APP_ALPN.to_vec(), chorrent::ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let client = Arc::new(
        Client::builder()
            .endpoint(endpoint.clone())
            .data_dir(data.path())
            .encrypted_storage(key)
            .discovery_timeout(Duration::from_secs(10))
            .download_limit(download_limit)
            .build()
            .await
            .unwrap(),
    );
    assert_eq!(client.alpns(), vec![chorrent::ALPN.to_vec()], "gossip is off when embedded");
    let accept_loop = tokio::spawn({
        let (endpoint, client) = (endpoint.clone(), Arc::clone(&client));
        async move {
            while let Some(incoming) = endpoint.accept().await {
                // Finish each handshake in its own task: awaiting it here
                // would let one slow or abandoned handshake block every
                // connection after it.
                let client = Arc::clone(&client);
                tokio::spawn(async move {
                    let Ok(connection) = incoming.await else { return };
                    // Route by ALPN before touching the connection.
                    if client.alpns().iter().any(|a| a.as_slice() == connection.alpn()) {
                        client.handle_connection(connection).await;
                    }
                });
            }
        }
    });
    App { endpoint, client, data, accept_loop, secret }
}

fn content(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 13) % 251) as u8).collect()
}

/// Every file under `dir`, read raw.
fn all_bytes_under(dir: &Path) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(all_bytes_under(&path));
        } else if let Ok(bytes) = std::fs::read(&path) {
            out.push(bytes);
        }
    }
    out
}

fn leaks(dir: &Path, secret: &[u8]) -> bool {
    // Any 64-byte stretch of the plaintext showing up anywhere counts.
    let probes: Vec<&[u8]> = secret.chunks(PIECE_SIZE as usize).map(|c| &c[..64.min(c.len())]).collect();
    all_bytes_under(dir).iter().any(|raw| probes.iter().any(|p| raw.windows(p.len()).any(|w| w == *p)))
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_encrypted_transfer_never_writes_plaintext() {
    let alice = app([1; 32]).await;
    let bob = app([2; 32]).await;
    assert_eq!(alice.client.id(), alice.endpoint.id().to_string(), "one identity");

    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("holiday.mp4");
    let secret = content(3 * PIECE_SIZE as usize + 999);
    std::fs::write(&file, &secret).unwrap();

    let seed = alice
        .client
        .seed_with(&file, SeedOptions::default().private(true).allow_peers([bob.endpoint.id()]))
        .await
        .unwrap();
    let code = seed.share_code().clone();

    let download = bob
        .client
        .download_with(&code, DownloadOptions::default().allow_peers([alice.endpoint.id()]))
        .await
        .unwrap();
    let done = tokio::time::timeout(Duration::from_secs(90), download.finished()).await.unwrap().unwrap();
    assert!(done.path.is_none(), "encrypted downloads have no plaintext path");

    // Readable through the API...
    let manifest = bob.client.manifest(&done.id).await.unwrap();
    assert_eq!(manifest.files()[0].size(), secret.len() as u64);
    assert_eq!(bob.client.read_range(&done.id, 0, 0, secret.len()).await.unwrap(), secret);
    assert_eq!(bob.client.read_range(&done.id, 0, 100, 10).await.unwrap(), &secret[100..110]);

    // ...but nowhere on disk in the clear, on either side.
    assert!(!leaks(bob.data.path(), &secret), "plaintext found in the receiver's data dir");
    assert!(!leaks(alice.data.path(), &secret), "plaintext found in the sender's data dir");

    // Plaintext appears only when asked for.
    let out = tempfile::tempdir().unwrap();
    let saved = out.path().join("holiday.mp4");
    bob.client.export_file(&done.id, 0, &saved).await.unwrap();
    assert_eq!(std::fs::read(&saved).unwrap(), secret);
    assert!(bob.client.export_file(&done.id, 0, &saved).await.is_err(), "never overwrites");
}

#[tokio::test(flavor = "multi_thread")]
async fn only_allowlisted_peers_can_download() {
    let alice = app([1; 32]).await;
    let bob = app([2; 32]).await;
    let mallory = app([3; 32]).await;

    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("photo.jpg");
    std::fs::write(&file, content(50_000)).unwrap();
    let seed = alice
        .client
        .seed_with(&file, SeedOptions::default().private(true).allow_peers([bob.endpoint.id()]))
        .await
        .unwrap();

    // Mallory has the complete, real share code but isn't on the list.
    let result = mallory.client.download(&seed.share_code(), None).await.unwrap().finished().await;
    assert!(matches!(result, Err(Error::NoPeersFound(_))), "got {:?}", result.err());

    // Membership changes take effect immediately.
    seed.set_allowed_peers(Some([bob.endpoint.id(), mallory.endpoint.id()])).unwrap();
    let done = mallory.client.download(&seed.share_code(), None).await.unwrap().finished().await.unwrap();
    assert_eq!(mallory.client.read_range(&done.id, 0, 0, 50_000).await.unwrap(), content(50_000));
}

#[tokio::test(flavor = "multi_thread")]
async fn streams_can_be_shared_and_everything_survives_a_restart() {
    let alice = app([1; 32]).await;
    let bob = app([2; 32]).await;

    // Like an Android content URI: a reader plus a size, no file path.
    let secret = content(PIECE_SIZE as usize + 5);
    let seed = alice
        .client
        .seed_reader("note.pdf", secret.len() as u64, std::io::Cursor::new(secret.clone()), SeedOptions::default())
        .await
        .unwrap();
    assert!(!leaks(alice.data.path(), &secret));

    let done = bob.client.download(&seed.share_code(), None).await.unwrap().finished().await.unwrap();
    let id = done.id;
    drop(done);

    // Restart bob with the same data dir and key.
    // Stop the accept loop first: it holds the client, which holds the data dir.
    let App { client, data, accept_loop, endpoint, .. } = bob;
    accept_loop.abort();
    let _ = accept_loop.await;
    Arc::into_inner(client).expect("nothing else holds the client").shutdown().await;
    endpoint.close().await;
    let bob = app_in([2; 32], data).await;
    assert!(matches!(bob.client.saved().unwrap().as_slice(), [chorrent::SavedTransfer::Seed { .. }]));
    assert_eq!(bob.client.read_range(&id, 0, 0, secret.len()).await.unwrap(), secret);
    assert_eq!(bob.client.manifest(&id).await.unwrap().name(), "note.pdf");

    // Removing deletes the stored copy.
    bob.client.remove(&id).await.unwrap();
    assert!(bob.client.saved().unwrap().is_empty());
    assert!(bob.client.read_range(&id, 0, 0, 10).await.is_err());
    assert!(all_bytes_under(&bob.data.path().join("blobs")).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_dir_refuses_the_wrong_key() {
    let data = tempfile::tempdir().unwrap();
    drop(Client::builder().data_dir(data.path()).encrypted_storage([5; 32]).build().await.unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let wrong = Client::builder().data_dir(data.path()).encrypted_storage([6; 32]).build().await;
    assert!(matches!(wrong, Err(Error::Storage(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_encrypted_downloads_resume() {
    let alice = app([1; 32]).await;
    let secret = content(24 * PIECE_SIZE as usize);
    let seed = alice
        .client
        .seed_reader("video.mp4", secret.len() as u64, std::io::Cursor::new(secret.clone()), SeedOptions::default())
        .await
        .unwrap();

    // First attempt: slow, cancelled after a few pieces.
    let bob = app_with([2; 32], tempfile::tempdir().unwrap(), Some(256 * 1024)).await;
    let download = bob.client.download(&seed.share_code(), None).await.unwrap();
    let mut events = download.events();
    let mut verified = 0;
    while verified < 6 {
        if let Ok(chorrent::Event::PieceVerified { .. }) = events.recv().await {
            verified += 1;
        }
    }
    download.cancel();
    assert!(matches!(download.finished().await, Err(Error::Cancelled)));

    // Second attempt, same client: picks up the encrypted pieces already there.
    let download = bob.client.download(&seed.share_code(), None).await.unwrap();
    let mut events = download.events();
    let done = tokio::time::timeout(Duration::from_secs(120), download.finished()).await.unwrap().unwrap();
    let mut resumed = None;
    while let Ok(event) = events.try_recv() {
        if let chorrent::Event::Resumed { pieces, .. } = event {
            resumed = Some(pieces);
        }
    }
    assert!(resumed.is_some_and(|p| p >= 6), "expected to resume with >= 6 pieces, got {resumed:?}");
    assert_eq!(bob.client.read_range(&done.id, 0, 0, secret.len()).await.unwrap(), secret);
    assert!(!leaks(bob.data.path(), &secret));
}

/// Every byte under `dir`: file contents, plus every file and folder name.
fn raw_disk_image(dir: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        out.extend(path.file_name().unwrap().to_string_lossy().as_bytes());
        out.push(b'\n');
        if path.is_dir() {
            out.extend(raw_disk_image(&path));
        } else {
            // Never skip a file: a locked database that can't be read would
            // make this check pass without looking at it.
            out.extend(std::fs::read(&path).unwrap_or_else(|e| panic!("can't read {}: {e}", path.display())));
        }
    }
    out
}

/// Shut an app down completely (accept loop, client, endpoint) so its data
/// dir is closed and can be read, and hand back the data dir.
async fn stop(app: App) -> (tempfile::TempDir, iroh::SecretKey) {
    let App { client, data, accept_loop, endpoint, secret } = app;
    accept_loop.abort();
    let _ = accept_loop.await;
    Arc::into_inner(client).expect("nothing else holds the client").shutdown().await;
    endpoint.close().await;
    (data, secret)
}

/// Which of the named byte strings appear anywhere under `dir`.
fn found_on_disk(dir: &Path, needles: &[(&str, Vec<u8>)]) -> Vec<String> {
    let image = raw_disk_image(dir);
    needles
        .iter()
        .filter(|(_, needle)| image.windows(needle.len()).any(|w| w == needle.as_slice()))
        .map(|(what, _)| what.to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_identifying_is_readable_on_disk() {
    let alice = app([1; 32]).await;
    let bob = app_with([2; 32], tempfile::tempdir().unwrap(), Some(256 * 1024)).await;

    let src = tempfile::tempdir().unwrap();
    let name = "Quarterly Secret Plans.pdf";
    let file = src.path().join(name);
    std::fs::write(&file, content(24 * PIECE_SIZE as usize)).unwrap();
    let seed = alice
        .client
        .seed_with(&file, SeedOptions::default().private(true).allow_peers([bob.endpoint.id()]))
        .await
        .unwrap();
    let code = seed.share_code();

    // The secret is the last 32 bytes of the code's postcard form.
    let code_bytes = postcard::to_allocvec(&code).unwrap();
    let secret = code_bytes[code_bytes.len() - 32..].to_vec();
    let id = code.id();
    let needles = [
        ("file name", name.as_bytes().to_vec()),
        ("file name stem", b"Quarterly Secret".to_vec()),
        ("private secret", secret),
        ("share id", id.as_bytes().to_vec()),
        ("share id (hex)", id.to_string().into_bytes()),
        ("bob's id", bob.endpoint.id().as_bytes().to_vec()),
        ("bob's id (hex)", bob.endpoint.id().to_string().into_bytes()),
        ("alice's id", alice.endpoint.id().as_bytes().to_vec()),
        ("alice's id (hex)", alice.endpoint.id().to_string().into_bytes()),
    ];

    // Mid-download: a pending download, saved progress and an allowlist exist.
    let download = bob
        .client
        .download_with(&code, DownloadOptions::default().allow_peers([alice.endpoint.id()]))
        .await
        .unwrap();
    let mut events = download.events();
    let mut verified = 0;
    while verified < 6 {
        if let Ok(chorrent::Event::PieceVerified { .. }) = events.recv().await {
            verified += 1;
        }
    }
    download.cancel();
    assert!(matches!(download.finished().await, Err(Error::Cancelled)));
    assert!(!bob.client.saved().unwrap().is_empty(), "the unfinished download is saved");
    let (bob_data, bob_secret) = stop(bob).await;
    assert_eq!(found_on_disk(bob_data.path(), &needles), Vec::<String>::new(), "receiver, mid-download");

    // Restart bob (same identity, same sealed data dir) and finish: it's now a saved seed.
    let bob = app_as([2; 32], bob_data, None, bob_secret).await;
    let saved = bob.client.saved().unwrap();
    let chorrent::Transfer::Download(download) = bob.client.resume(&saved[0]).await.unwrap() else {
        panic!("expected the saved download");
    };
    let done = download.finished().await.unwrap();
    assert_eq!(bob.client.manifest(&done.id).await.unwrap().name(), name, "readable through the API");
    drop(done);
    let (bob_data, _) = stop(bob).await;
    assert_eq!(found_on_disk(bob_data.path(), &needles), Vec::<String>::new(), "receiver, finished");

    drop(seed);
    let (alice_data, _) = stop(alice).await;
    assert_eq!(found_on_disk(alice_data.path(), &needles), Vec::<String>::new(), "sender");
}

#[tokio::test(flavor = "multi_thread")]
async fn sealed_and_plain_data_dirs_are_never_mixed() {
    // A plain data dir with records (like one written by chorrent 0.5.0).
    let plain = tempfile::tempdir().unwrap();
    {
        let client = Client::builder().data_dir(plain.path()).build().await.unwrap();
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        let _seed = client.seed(src.path().join("a.txt")).await.unwrap();
        client.shutdown().await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let with_key = Client::builder().data_dir(plain.path()).encrypted_storage([4; 32]).build().await;
    assert!(matches!(with_key, Err(Error::Storage(ref m)) if m.contains("without encryption")), "plain dir + key");

    // A sealed data dir can't be opened without its key.
    let sealed = tempfile::tempdir().unwrap();
    drop(Client::builder().data_dir(sealed.path()).encrypted_storage([4; 32]).build().await.unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let without_key = Client::builder().data_dir(sealed.path()).build().await;
    assert!(matches!(without_key, Err(Error::Storage(ref m)) if m.contains("encrypted")), "sealed dir, no key");
}
