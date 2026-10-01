use crate::chunker;
use crate::discovery::{self, Announcement};
use crate::error::{Error, Result};
use crate::event::{Event, EVENT_CAPACITY};
use crate::handler::{ChorrentProtocol, ServedFile, ServingSlot};
use crate::node::ChorrentNode;
use crate::protocol::{self, PieceRequest};
use crate::scheduler::{self, SwarmState};
use crate::share::ShareCode;
use iroh::endpoint::Connection;
use iroh_gossip::api::GossipReceiver;
use iroh_tickets::endpoint::EndpointTicket;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{broadcast, Mutex as TokioMutex};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

/// How long a download waits for the first peer to show up on gossip.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// Parallel piece requests per connected peer.
const WORKERS_PER_PEER: usize = 8;
/// Pieces just past the first missing one that are fetched strictly in order.
const URGENT_WINDOW: usize = 3;

/// A chorrent node: one network identity that can seed and download files.
///
/// Must be created and used inside a tokio runtime.
pub struct Client {
    node: Arc<ChorrentNode>,
    serving: ServingSlot,
}

/// Extra settings for [`Client::seed_with`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SeedOptions {
    /// Join the swarm another seeder of the same file started, so both
    /// seeders find each other's downloaders.
    pub join: Option<ShareCode>,
}

impl SeedOptions {
    pub fn join(mut self, share: ShareCode) -> Self {
        self.join = Some(share);
        self
    }
}

impl Client {
    /// Bind a new node to the network.
    pub async fn new() -> Result<Self> {
        let serving = ServingSlot::default();
        let node = ChorrentNode::bind(ChorrentProtocol { serving: Arc::clone(&serving) }).await?;
        Ok(Self { node: Arc::new(node), serving })
    }

    /// Start seeding a file. It stays available until the returned handle
    /// is stopped or dropped.
    pub async fn seed(&self, path: impl AsRef<Path>) -> Result<SeedHandle> {
        self.seed_with(path, SeedOptions::default()).await
    }

    pub async fn seed_with(&self, path: impl AsRef<Path>, options: SeedOptions) -> Result<SeedHandle> {
        if self.serving.read().unwrap().is_some() {
            return Err(Error::AlreadySeeding);
        }
        let path = path.as_ref().to_path_buf();

        // Hashing reads the whole file, so keep it off the async worker threads.
        let (hashed, total_size) = {
            let path = path.clone();
            tokio::task::spawn_blocking(move || -> Result<_> {
                let hashed = chunker::hash_file(&path)?;
                let total_size = std::fs::metadata(&path)
                    .map_err(|source| Error::Io { path: path.clone(), source })?
                    .len();
                Ok((hashed, total_size))
            })
            .await
            .map_err(|e| Error::Task(e.to_string()))??
        };
        let root_hash = hashed.root_hash;

        if let Some(join) = &options.join
            && join.root_hash != root_hash
        {
            return Err(Error::InvalidShareCode(
                "the --join share code is for a different file".to_string(),
            ));
        }

        let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
        let total_pieces = total_size.div_ceil(chunk_size) as usize;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);

        let served = Arc::new(ServedFile {
            path: path.clone(),
            root_hash,
            outboard: hashed.outboard,
            held: vec![true; total_pieces],
            events: events.clone(),
        });
        {
            let mut slot = self.serving.write().unwrap();
            if slot.is_some() {
                return Err(Error::AlreadySeeding);
            }
            *slot = Some(Arc::clone(&served));
        }
        // Built now so that any early return below clears the slot again.
        let mut handle = SeedHandle {
            share: ShareCode {
                ticket: EndpointTicket::new(self.node.addr()),
                root_hash,
                total_size,
                file_name: path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "download".to_string()),
            },
            events: events.clone(),
            serving: Arc::clone(&self.serving),
            served,
            tasks: JoinSet::new(),
        };

        let bootstrap = options
            .join
            .map(|share| vec![share.ticket.endpoint_addr().id])
            .unwrap_or_default();
        let (sender, receiver) = self
            .node
            .gossip()
            .subscribe(discovery::topic_for(&root_hash), bootstrap)
            .await
            .map_err(|e| Error::Gossip(e.to_string()))?
            .split();

        // Seeders don't use the peer list yet, but the receiver has to be
        // drained to stay subscribed.
        handle.tasks.spawn(discovery::listen_for_peers(receiver, Arc::default(), events));
        handle.tasks.spawn(discovery::announce_periodically(
            sender,
            Announcement { ticket: handle.share.ticket.to_string() },
        ));

        Ok(handle)
    }

    /// Start downloading the file behind `share`. With no `dest`, it's saved
    /// in the current directory under the seeder's file name.
    pub async fn download(&self, share: &ShareCode, dest: Option<PathBuf>) -> Result<DownloadHandle> {
        let dest = dest.unwrap_or_else(|| PathBuf::from(share.safe_file_name()));
        let bootstrap = vec![share.ticket.endpoint_addr().id];
        let (_sender, receiver) = self
            .node
            .gossip()
            .subscribe(discovery::topic_for(&share.root_hash), bootstrap)
            .await
            .map_err(|e| Error::Gossip(e.to_string()))?
            .split();

        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let task = tokio::spawn(run_download(
            Arc::clone(&self.node),
            share.clone(),
            dest,
            receiver,
            events.clone(),
        ));
        Ok(DownloadHandle { events, abort: task.abort_handle(), task: Some(task) })
    }

    /// Close every connection and stop the node.
    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }
}

/// A file being seeded. Seeding stops when this is stopped or dropped.
pub struct SeedHandle {
    share: ShareCode,
    events: broadcast::Sender<Event>,
    serving: ServingSlot,
    served: Arc<ServedFile>,
    tasks: JoinSet<()>,
}

impl SeedHandle {
    /// The code downloaders need. Share its `Display` form.
    pub fn share_code(&self) -> &ShareCode {
        &self.share
    }

    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub fn stop(self) {}
}

impl Drop for SeedHandle {
    fn drop(&mut self) {
        // Background gossip tasks stop when `tasks` drops; we only have to
        // stop serving pieces, and only if the slot still holds our file.
        let mut slot = self.serving.write().unwrap();
        if slot.as_ref().is_some_and(|s| Arc::ptr_eq(s, &self.served)) {
            *slot = None;
        }
    }
}

/// A download in progress. Dropping it cancels the download.
pub struct DownloadHandle {
    events: broadcast::Sender<Event>,
    abort: AbortHandle,
    task: Option<JoinHandle<Result<PathBuf>>>,
}

impl DownloadHandle {
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub fn cancel(&self) {
        self.abort.abort();
    }

    /// Wait for the download to end. On success, returns the path of the
    /// complete file, already verified against the share's root hash.
    pub async fn finished(mut self) -> Result<PathBuf> {
        let task = self.task.take().expect("task is only taken here");
        match task.await {
            Ok(result) => result,
            Err(e) if e.is_cancelled() => Err(Error::Cancelled),
            Err(e) => Err(Error::Task(e.to_string())),
        }
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

async fn run_download(
    node: Arc<ChorrentNode>,
    share: ShareCode,
    dest: PathBuf,
    receiver: GossipReceiver,
    events: broadcast::Sender<Event>,
) -> Result<PathBuf> {
    let root_hash = share.root_hash;
    let total_size = share.total_size;
    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;

    // Everything spawned into a JoinSet is aborted when it drops, so
    // cancelling this task also stops discovery and every worker.
    let mut background = JoinSet::new();
    let discovered: Arc<StdMutex<HashSet<String>>> = Arc::default();
    background.spawn(discovery::listen_for_peers(receiver, Arc::clone(&discovered), events.clone()));

    let deadline = tokio::time::Instant::now() + DISCOVERY_TIMEOUT;
    let discovered_peers = loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let found = discovered.lock().unwrap().clone();
        if !found.is_empty() {
            break found;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::NoPeersFound(DISCOVERY_TIMEOUT));
        }
    };

    let mut connections: HashMap<String, Connection> = HashMap::new();
    let mut peer_bitfields: HashMap<String, Vec<bool>> = HashMap::new();

    for peer_ticket_str in discovered_peers {
        let Ok(peer_ticket) = peer_ticket_str.parse::<EndpointTicket>() else { continue };
        let label = peer_ticket.endpoint_addr().id.to_string();

        let Ok(conn) = node.connect(peer_ticket.endpoint_addr().clone()).await else { continue };
        let Ok((mut send, mut recv)) = conn.open_bi().await else { continue };
        if protocol::request_bitfield(&mut send).await.is_err() {
            continue;
        }
        let Ok(bitfield) = protocol::receive_bitfield(&mut recv, total_pieces).await else { continue };

        let _ = events.send(Event::PeerConnected {
            peer: label.clone(),
            pieces: bitfield.iter().filter(|&&b| b).count(),
            total: total_pieces,
        });
        connections.insert(label.clone(), conn);
        peer_bitfields.insert(label, bitfield);
    }

    if connections.is_empty() {
        return Err(Error::NoPeersConnected);
    }

    let connections = Arc::new(connections);
    let state = Arc::new(TokioMutex::new(SwarmState {
        total_pieces,
        // `have` means "claimed by a worker or already on disk".
        have: vec![false; total_pieces],
        peer_bitfields,
    }));

    let mut workers = JoinSet::new();
    for _ in 0..(connections.len() * WORKERS_PER_PEER) {
        let state = Arc::clone(&state);
        let connections = Arc::clone(&connections);
        let dest = dest.clone();
        let events = events.clone();

        workers.spawn(async move {
            loop {
                let claimed = {
                    let mut state = state.lock().await;
                    if state.have.iter().all(|&b| b) {
                        return;
                    }
                    let playhead = state.have.iter().take_while(|&&b| b).count();
                    let next = scheduler::next_piece_to_request(&state, playhead, URGENT_WINDOW);
                    if let Some((piece, _)) = next {
                        state.have[piece] = true;
                    }
                    next
                };

                let Some((piece, peer_id)) = claimed else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };

                let start = piece as u64 * chunk_size;
                let end = std::cmp::min(start + chunk_size, total_size);
                let conn = &connections[&peer_id];

                let result: Result<Option<Vec<u8>>> = async {
                    let (mut send, mut recv) = conn
                        .open_bi()
                        .await
                        .map_err(|e| crate::error::ProtocolError::Send { message: e.to_string() })?;
                    protocol::send_piece_request(&mut send, &PieceRequest { start, end }).await?;
                    Ok(protocol::receive_piece_response(&mut recv).await?)
                }
                .await;

                match result {
                    Ok(Some(encoded)) => {
                        match chunker::receive_range(&dest, root_hash, total_size, start, end, &encoded) {
                            Ok(()) => {
                                let _ = events.send(Event::PieceVerified { index: piece, bytes: end - start });
                            }
                            Err(e) => {
                                // Bad data from this peer: don't ask it for this piece again.
                                let mut state = state.lock().await;
                                state.have[piece] = false;
                                if let Some(bf) = state.peer_bitfields.get_mut(&peer_id) {
                                    bf[piece] = false;
                                }
                                let _ = events.send(Event::PieceFailed { index: piece, reason: e.to_string() });
                            }
                        }
                    }
                    Ok(None) => {
                        let mut state = state.lock().await;
                        state.have[piece] = false;
                        if let Some(bf) = state.peer_bitfields.get_mut(&peer_id) {
                            bf[piece] = false;
                        }
                    }
                    Err(e) => {
                        state.lock().await.have[piece] = false;
                        let _ = events.send(Event::PieceFailed {
                            index: piece,
                            reason: format!("fetching from {peer_id}: {e}"),
                        });
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });
    }

    while let Some(joined) = workers.join_next().await {
        joined.map_err(|e| Error::Task(e.to_string()))?;
    }
    for conn in connections.values() {
        conn.close(0u32.into(), b"done");
    }

    let verify = {
        let dest = dest.clone();
        tokio::task::spawn_blocking(move || chunker::hash_file(&dest))
            .await
            .map_err(|e| Error::Task(e.to_string()))??
    };
    if verify.root_hash != root_hash {
        return Err(Error::HashMismatch);
    }

    let _ = events.send(Event::Completed);
    Ok(dest)
}
