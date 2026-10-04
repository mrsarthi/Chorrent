//! The download engine: find peers, fetch and verify the manifest, then pull
//! pieces from every peer at once while (optionally) serving what we already
//! have to others.

use crate::discovery::{self, Swarm};
use crate::error::{Error, ProtocolError, Result};
use crate::event::Event;
use crate::handler::Registry;
use crate::limits::RateLimiter;
use crate::local::{LocalShare, SharedShare};
use crate::manifest::{Manifest, MAX_MANIFEST_BYTES};
use crate::node::ChorrentNode;
use crate::protocol::{self, unpack_bits, Request, Response};
use crate::registration::Registration;
use crate::scheduler::Scheduler;
use crate::share::{access_token, ShareCode};
use crate::storage::MasterKey;
use crate::store::Store;
use iroh::endpoint::Connection;
use iroh::{EndpointAddr, EndpointId};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch, Notify};
use tokio::task::JoinSet;
use tokio::time::Instant;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// The first connection attempt to a peer while looking for the manifest;
/// later attempts get longer.
const FIRST_ATTEMPT: Duration = Duration::from_secs(4);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Don't redial a peer we just tried more often than this.
const RETRY_PEER_AFTER: Duration = Duration::from_secs(20);
/// Requests kept in flight to each peer.
const REQUESTS_PER_PEER: usize = 8;
/// More requests in flight for distant peers: with a fixed number, a long
/// round trip caps speed (8 pieces per 400 ms is only ~1.3 MB/s). Each peer
/// gets enough to keep about this much data per second on the way...
const TARGET_BYTES_PER_SEC: u64 = 12 * 1024 * 1024;
/// ...but never more than this many requests (QUIC allows 100 streams).
const MAX_REQUESTS_PER_PEER: usize = 48;
const URGENT_WINDOW: usize = 3;
/// Consecutive failures before we give up on a peer.
const MAX_STRIKES: u32 = 3;
/// Most peers we download from (or are connecting to) at once.
const MAX_PEERS: usize = 32;
/// How often partial progress is written to the store.
const SAVE_EVERY: Duration = Duration::from_secs(5);

pub(crate) struct DownloadCtx {
    pub node: Arc<ChorrentNode>,
    pub registry: Registry,
    pub reseed: bool,
    pub download_rate: Option<Arc<RateLimiter>>,
    pub store: Option<Arc<Store>>,
    /// How long to look for a first peer that can give us the manifest.
    pub discovery_timeout: Duration,
    /// Download into the encrypted store with this key (`dest_dir` is then
    /// the share's store folder).
    pub key: Option<MasterKey>,
    /// Who may fetch from us while we reseed (None: anyone with the code).
    pub allow: Option<HashSet<EndpointId>>,
}

pub(crate) struct DownloadOutcome {
    pub path: PathBuf,
    pub encrypted: bool,
    pub share: Option<SharedShare>,
    /// Present when reseeding: keeps the share served and announced.
    pub seeding: Option<(Registration, JoinSet<()>)>,
}

pub(crate) async fn run(
    ctx: DownloadCtx,
    code: ShareCode,
    dest_dir: PathBuf,
    events: broadcast::Sender<Event>,
    mut cancel: watch::Receiver<bool>,
) -> Result<DownloadOutcome> {
    let me = ctx.node.id();
    let auth = code.secret.map(|s| access_token(&s, &me));

    // Everything here is aborted if the download is cancelled.
    let mut background = JoinSet::new();
    let (found_tx, mut found_rx) = mpsc::channel(256);
    for addr in &code.peers {
        let _ = found_tx.try_send(addr.clone());
    }
    // Find more peers through the share's swarm. We start announcing
    // ourselves only once we have something to serve.
    let announce = Arc::new(AtomicBool::new(false));
    if ctx.node.gossip().is_some() {
        let swarm = Swarm {
            node: Arc::clone(&ctx.node),
            topic: code.topic(),
            bootstrap: code.peers.clone(),
            announce: Arc::clone(&announce),
            found: Some(found_tx.clone()),
            events: events.clone(),
        };
        background.spawn(async move {
            if let Ok((sender, receiver, warm)) = discovery::join(&swarm).await {
                discovery::keep(swarm, sender, receiver, warm).await;
            }
        });
    }
    let mut tracker = PeerTracker::new(me, events.clone());

    // 1. Get the manifest from the first peer that has it. Keep retrying
    // every known peer until the deadline: in a 1:1 transfer there is only
    // one, and a single stalled attempt mustn't sink the download.
    let deadline = Instant::now() + ctx.discovery_timeout;
    let mut candidates: Vec<EndpointAddr> = Vec::new();
    let mut attempts: HashMap<EndpointId, u32> = HashMap::new();
    let mut next = 0;
    let (manifest, manifest_bytes, first_conn) = loop {
        while let Ok(addr) = found_rx.try_recv() {
            tracker.add_candidate(&mut candidates, addr);
        }
        if candidates.is_empty() {
            tokio::select! {
                found = tokio::time::timeout_at(deadline, found_rx.recv()) => match found {
                    Ok(Some(addr)) => tracker.add_candidate(&mut candidates, addr),
                    _ => return Err(Error::NoPeersFound(ctx.discovery_timeout)),
                },
                _ = cancel.wait_for(|&c| c) => return Err(Error::Cancelled),
            }
            continue;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::NoPeersFound(ctx.discovery_timeout));
        }
        let addr = candidates[next % candidates.len()].clone();
        next += 1;
        // Short first tries, longer later ones (a relayed connection can be slow).
        let tries = attempts.entry(addr.id).or_insert(0);
        let limit = (FIRST_ATTEMPT * 2u32.pow((*tries).min(4))).min(CONNECT_TIMEOUT).min(remaining);
        *tries += 1;
        let attempt = async {
            let conn = tokio::time::timeout(limit, ctx.node.connect(addr)).await.ok()?.ok()?;
            match fetch_manifest(&conn, &code, auth).await {
                Ok((manifest, bytes)) => Some((manifest, bytes, conn)),
                Err(_) => {
                    conn.close(0u32.into(), b"no manifest");
                    None
                }
            }
        };
        tokio::select! {
            got = attempt => if let Some(found) = got { break found },
            _ = cancel.wait_for(|&c| c) => return Err(Error::Cancelled),
        }
        let pause = Duration::from_millis(500).min(deadline.saturating_duration_since(Instant::now()));
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = cancel.wait_for(|&c| c) => return Err(Error::Cancelled),
        }
    };
    // Every other peer we already know of (e.g. other seeders listed in the
    // share code) gets downloaded from too, without waiting for gossip.
    for addr in candidates.iter().filter(|c| c.id != first_conn.remote_id()) {
        let _ = found_tx.try_send(addr.clone());
    }
    let _ = events.send(Event::ManifestReceived {
        name: manifest.name().to_string(),
        files: manifest.files().len(),
        total_size: manifest.total_size(),
        total_pieces: crate::manifest::Layout::new(&manifest).total_pieces,
    });

    // 2. Lay out the files, picking up where an earlier attempt left off.
    let share = {
        let (dest_dir, events, store, key) = (dest_dir.clone(), events.clone(), ctx.store.clone(), ctx.key.clone());
        let secret = code.secret;
        tokio::task::spawn_blocking(move || -> Result<LocalShare> {
            let share = LocalShare::for_download(manifest, manifest_bytes, &dest_dir, key.as_ref(), secret, events)?;
            if let Some(saved) = store.as_ref().and_then(|s| s.load_progress(&share.id, &dest_dir)) {
                share.restore_progress(saved)?;
            }
            Ok(share)
        })
        .await
        .map_err(|e| Error::Task(e.to_string()))??
    };
    share.set_allowed(ctx.allow.clone());
    let share = Arc::new(share);
    let restored = share.have_count();
    if restored > 0 {
        let bytes = (0..share.total_pieces()).filter(|&i| share.has(i)).map(|i| share.piece_len(i)).sum();
        let _ = events.send(Event::Resumed { pieces: restored, bytes });
    }
    if let Some(store) = &ctx.store {
        store.remember_download(&code, &dest_dir)?;
        store.remember_allowed(&share.id, ctx.allow.as_ref())?;
    }

    // Serve what we have to others while we download.
    let registration = if ctx.reseed {
        let registration = Registration::new(&ctx.registry, Arc::clone(&share))?;
        announce.store(true, Ordering::Relaxed);
        Some(registration)
    } else {
        None
    };

    // 3. Swarm until every piece is here.
    let engine = Arc::new(Engine {
        sched: Mutex::new(Scheduler::new(&share.have_snapshot(), URGENT_WINDOW)),
        share: Arc::clone(&share),
        wake: Notify::new(),
        finished: Notify::new(),
        connected: Mutex::new(HashSet::new()),
        open: Mutex::new(Vec::new()),
        events: events.clone(),
        auth,
        rate: ctx.download_rate.clone(),
    });
    let mut peers = JoinSet::new();
    peers.spawn(Arc::clone(&engine).run_peer(first_conn));
    let mut save_tick = tokio::time::interval(SAVE_EVERY);

    while !engine.is_done() {
        tokio::select! {
            _ = engine.finished.notified() => {}
            Some(addr) = found_rx.recv() => {
                // Cap peers (connecting or connected), so a flood of announcements
                // in a public swarm can't make us dial endlessly.
                if peers.len() < MAX_PEERS && !engine.is_connected(&addr.id) && tracker.should_try(&addr) {
                    let (engine, node) = (Arc::clone(&engine), Arc::clone(&ctx.node));
                    peers.spawn(async move {
                        if let Some(conn) = connect(&node, addr).await {
                            engine.run_peer(conn).await;
                        }
                    });
                }
            }
            _ = cancel.wait_for(|&c| c) => {
                // Keep what we have so a later run can resume from here.
                if let Some(store) = &ctx.store {
                    store.save_progress(&share, &dest_dir);
                }
                for conn in engine.open.lock().unwrap().drain(..) {
                    conn.close(0u32.into(), b"cancelled");
                }
                return Err(Error::Cancelled);
            }
            _ = save_tick.tick() => {
                if let Some(store) = &ctx.store {
                    store.save_progress(&share, &dest_dir);
                }
            }
        }
        while peers.try_join_next().is_some() {}
    }
    drop(peers);
    // Tell peers we're done rather than letting connections just vanish.
    for conn in engine.open.lock().unwrap().drain(..) {
        conn.close(0u32.into(), b"done");
    }

    share.release_writers();
    // 4. Every piece was verified on arrival; check whole files anyway as a
    // final guard against bugs or the files being changed underneath us.
    {
        let share = Arc::clone(&share);
        tokio::task::spawn_blocking(move || share.verify())
        .await
        .map_err(|e| Error::Task(e.to_string()))??;
    }
    if let Some(store) = &ctx.store {
        store.save_progress(&share, &dest_dir);
        store.mark_complete(&share.id, &dest_dir)?;
    }

    let _ = events.send(Event::Completed);
    Ok(DownloadOutcome {
        path: share.root.clone(),
        encrypted: share.is_encrypted(),
        share: Some(Arc::clone(&share)),
        seeding: registration.map(|r| (r, background)),
    })
}

/// Connect to one peer from a share code and see how well that works.
pub(crate) async fn check_peer(node: &ChorrentNode, code: &ShareCode, addr: EndpointAddr) -> crate::client::PeerCheck {
    use futures_lite::StreamExt;
    let mut check = crate::client::PeerCheck {
        peer: addr.id.to_string(),
        listed_relay: addr.relay_urls().next().is_some(),
        reachable: false,
        direct: None,
        rtt_ms: None,
        serves_share: false,
        problem: None,
    };
    if addr.id == node.id() {
        check.problem = Some("that's this node".into());
        return check;
    }
    let conn = match tokio::time::timeout(CONNECT_TIMEOUT, node.connect(addr)).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => {
            check.problem = Some(e.to_string());
            return check;
        }
        Err(_) => {
            check.problem = Some(format!("no answer within {}s", CONNECT_TIMEOUT.as_secs()));
            return check;
        }
    };
    check.reachable = true;
    let auth = code.secret.map(|s| access_token(&s, &node.id()));
    match request(&conn, &Request::Manifest { share: code.id, auth }).await {
        Ok(Response::Manifest(_)) => check.serves_share = true,
        Ok(_) => check.problem = Some("reachable, but it doesn't serve this share to us".into()),
        Err(e) => check.problem = Some(format!("reachable, but the request failed: {e}")),
    }
    // Give hole punching a few seconds to upgrade from the relay.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut snapshots = conn.paths_stream();
    while let Ok(Some(paths)) = tokio::time::timeout_at(deadline, snapshots.next()).await {
        if let Some(selected) = paths.iter().find(|p| p.is_selected()) {
            check.direct = Some(selected.is_ip());
            check.rtt_ms = Some(selected.rtt().as_millis() as u64);
            if selected.is_ip() {
                break;
            }
        }
    }
    drop(snapshots);
    conn.close(0u32.into(), b"check done");
    check
}

/// The round-trip time of the path a connection is using right now.
fn selected_rtt(conn: &Connection) -> Option<Duration> {
    conn.paths().iter().find(|p| p.is_selected()).map(|p| p.rtt())
}

/// How many requests to keep in flight to a peer this far away.
fn requests_for(rtt: Option<Duration>) -> usize {
    let Some(rtt) = rtt else { return REQUESTS_PER_PEER };
    let bytes_in_flight = TARGET_BYTES_PER_SEC as f64 * rtt.as_secs_f64();
    let pieces = (bytes_in_flight / crate::chunker::BLOCK_SIZE.bytes() as f64).ceil() as usize;
    pieces.clamp(REQUESTS_PER_PEER, MAX_REQUESTS_PER_PEER)
}

async fn watch_path(conn: Connection, peer: EndpointId, events: broadcast::Sender<Event>) {
    use futures_lite::StreamExt;
    let mut snapshots = conn.paths_stream();
    let mut last = None;
    while let Some(paths) = snapshots.next().await {
        let Some(selected) = paths.iter().find(|p| p.is_selected()) else { continue };
        let direct = selected.is_ip();
        if last != Some(direct) {
            last = Some(direct);
            let rtt_ms = selected.rtt().as_millis() as u64;
            let _ = events.send(Event::PeerPath { peer: peer.to_string(), direct, rtt_ms });
        }
    }
}

async fn connect(node: &ChorrentNode, addr: EndpointAddr) -> Option<Connection> {
    tokio::time::timeout(CONNECT_TIMEOUT, node.connect(addr)).await.ok()?.ok()
}

async fn request(conn: &Connection, req: &Request) -> Result<Response> {
    let exchange = async {
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ProtocolError::Send { message: e.to_string() })?;
        protocol::write_frame(&mut send, req).await?;
        let _ = send.finish();
        Ok(protocol::expect_frame(&mut recv).await?)
    };
    tokio::time::timeout(REQUEST_TIMEOUT, exchange)
        .await
        .unwrap_or_else(|_| Err(ProtocolError::Receive { message: "request timed out".into() }.into()))
}

async fn fetch_manifest(conn: &Connection, code: &ShareCode, auth: Option<[u8; 32]>) -> Result<(Manifest, Vec<u8>)> {
    match request(conn, &Request::Manifest { share: code.id, auth }).await? {
        Response::Manifest(bytes) if bytes.len() <= MAX_MANIFEST_BYTES => {
            let manifest = Manifest::decode_verified(&bytes, code.id)?;
            // The share code is what the user saw before agreeing to download;
            // don't let it understate what actually arrives.
            if manifest.total_size() != code.total_size || manifest.name() != code.name {
                return Err(Error::BadManifest("it doesn't match the name or size in the share code".into()));
            }
            Ok((manifest, bytes))
        }
        _ => Err(Error::BadManifest("peer didn't send it".into())),
    }
}

/// Remembers which peers we've tried, so gossip repeats don't cause redials.
struct PeerTracker {
    me: EndpointId,
    /// Peers we've reported with `PeerDiscovered`.
    discovered: HashSet<EndpointId>,
    /// When we last dialed each peer while swarming.
    last_attempt: HashMap<EndpointId, Instant>,
    events: broadcast::Sender<Event>,
}

impl PeerTracker {
    fn new(me: EndpointId, events: broadcast::Sender<Event>) -> Self {
        Self { me, discovered: HashSet::new(), last_attempt: HashMap::new(), events }
    }

    fn discover(&mut self, id: EndpointId) {
        if self.discovered.insert(id) {
            let _ = self.events.send(Event::PeerDiscovered { peer: id.to_string() });
        }
    }

    /// Add a peer to try for the manifest (once per peer).
    fn add_candidate(&mut self, candidates: &mut Vec<EndpointAddr>, addr: EndpointAddr) {
        if addr.id == self.me || candidates.iter().any(|c| c.id == addr.id) {
            return;
        }
        self.discover(addr.id);
        candidates.push(addr);
    }

    fn should_try(&mut self, addr: &EndpointAddr) -> bool {
        if addr.id == self.me {
            return false;
        }
        self.discover(addr.id);
        let now = Instant::now();
        match self.last_attempt.insert(addr.id, now) {
            None => true,
            Some(previous) if now - previous < RETRY_PEER_AFTER => {
                self.last_attempt.insert(addr.id, previous);
                false
            }
            Some(_) => true,
        }
    }
}

struct Engine {
    share: SharedShare,
    sched: Mutex<Scheduler<EndpointId>>,
    /// Something changed that might give an idle worker a piece to fetch.
    wake: Notify,
    /// The last piece arrived.
    finished: Notify,
    connected: Mutex<HashSet<EndpointId>>,
    /// Every connection we've used, to close cleanly when we finish.
    open: Mutex<Vec<Connection>>,
    events: broadcast::Sender<Event>,
    auth: Option<[u8; 32]>,
    rate: Option<Arc<RateLimiter>>,
}

impl Engine {
    fn is_done(&self) -> bool {
        self.sched.lock().unwrap().is_done()
    }

    fn is_connected(&self, peer: &EndpointId) -> bool {
        self.connected.lock().unwrap().contains(peer)
    }

    async fn run_peer(self: Arc<Self>, conn: Connection) {
        let peer = conn.remote_id();
        if !self.connected.lock().unwrap().insert(peer) {
            return; // already downloading from this peer over another connection
        }
        self.open.lock().unwrap().push(conn.clone());
        let was_used = self.drive_peer(&conn, peer).await;
        conn.close(0u32.into(), b"done");
        self.sched.lock().unwrap().remove_peer(&peer);
        self.connected.lock().unwrap().remove(&peer);
        self.wake.notify_waiters();
        if was_used && !self.is_done() {
            let _ = self.events.send(Event::PeerDisconnected { peer: peer.to_string() });
        }
    }

    /// Subscribe to the peer's progress and keep requests flowing until it
    /// leaves, keeps failing, or we're done. Returns whether we got as far
    /// as using the peer.
    async fn drive_peer(self: &Arc<Self>, conn: &Connection, peer: EndpointId) -> bool {
        let share_id = self.share.id;
        let total = self.share.total_pieces();
        let Ok((mut send, mut recv)) = conn.open_bi().await else { return false };
        if protocol::write_frame(&mut send, &Request::Subscribe { share: share_id, auth: self.auth }).await.is_err() {
            return false;
        }
        let _ = send.finish();
        let bits = match protocol::expect_frame(&mut recv).await {
            Ok(Response::Bitfield(bytes)) => unpack_bits(&bytes, total),
            _ => return false,
        };
        let _ = self.events.send(Event::PeerConnected {
            peer: peer.to_string(),
            pieces: bits.iter().filter(|&&b| b).count(),
            total,
        });
        self.sched.lock().unwrap().add_peer(peer, bits);
        self.wake.notify_waiters();

        // Report whether we reach this peer directly or through a relay, and
        // when that changes (e.g. hole punching succeeds a moment later).
        // Dropped, and so stopped, when this function returns.
        let mut path_watch = JoinSet::new();
        path_watch.spawn(watch_path(conn.clone(), peer, self.events.clone()));

        let strikes = Arc::new(AtomicU32::new(0));
        let mut workers = JoinSet::new();
        for _ in 0..REQUESTS_PER_PEER {
            workers.spawn(Arc::clone(self).worker(conn.clone(), peer, Arc::clone(&strikes)));
        }
        let mut resize = tokio::time::interval(Duration::from_secs(2));

        let updates = async {
            loop {
                match protocol::read_frame::<Response>(&mut recv).await {
                    Ok(Some(Response::Have(piece))) => self.sched.lock().unwrap().set_peer_has(&peer, piece as usize),
                    Ok(Some(Response::Bitfield(bytes))) => {
                        self.sched.lock().unwrap().add_peer(peer, unpack_bits(&bytes, total));
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => break,
                }
                self.wake.notify_waiters();
            }
        };
        tokio::pin!(updates);
        loop {
            tokio::select! {
                _ = &mut updates => break,
                finished = workers.join_next() => if finished.is_none() { break },
                _ = resize.tick() => {
                    // Add requests for distant peers (never removes any).
                    let wanted = requests_for(selected_rtt(conn));
                    while workers.len() < wanted && !self.is_done() && strikes.load(Ordering::Relaxed) < MAX_STRIKES {
                        workers.spawn(Arc::clone(self).worker(conn.clone(), peer, Arc::clone(&strikes)));
                    }
                }
            }
        }
        true
    }

    async fn worker(self: Arc<Self>, conn: Connection, peer: EndpointId, strikes: Arc<AtomicU32>) {
        loop {
            if self.is_done() || strikes.load(Ordering::Relaxed) >= MAX_STRIKES {
                return;
            }
            // Register interest before checking, so a wake-up in between isn't lost.
            let notified = self.wake.notified();
            let picked = self.sched.lock().unwrap().pick(&peer);
            let Some(piece) = picked else {
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            };

            let req = Request::Piece { share: self.share.id, auth: self.auth, index: piece as u32 };
            match request(&conn, &req).await {
                Ok(Response::Piece(encoded)) => {
                    if let Some(rate) = &self.rate {
                        rate.acquire(encoded.len() as u64).await;
                    }
                    let share = Arc::clone(&self.share);
                    let stored = tokio::task::spawn_blocking(move || share.store_piece(piece, &encoded)).await;
                    match stored {
                        Ok(Ok(_)) => {
                            strikes.store(0, Ordering::Relaxed);
                            let (newly, done) = {
                                let mut sched = self.sched.lock().unwrap();
                                (sched.complete(piece), sched.is_done())
                            };
                            if newly {
                                let bytes = self.share.piece_len(piece);
                                let _ = self.events.send(Event::PieceVerified { index: piece, bytes });
                            }
                            if done {
                                self.finished.notify_one();
                                self.wake.notify_waiters();
                            }
                        }
                        Ok(Err(e)) => {
                            // Bad data: never ask this peer for this piece again.
                            strikes.fetch_add(1, Ordering::Relaxed);
                            self.give_back(piece, &peer, true);
                            let _ = self.events.send(Event::PieceFailed { index: piece, reason: e.to_string() });
                        }
                        Err(e) => {
                            self.give_back(piece, &peer, false);
                            let _ = self.events.send(Event::PieceFailed { index: piece, reason: e.to_string() });
                        }
                    }
                }
                Ok(Response::DontHave) => self.give_back(piece, &peer, true),
                Ok(Response::Busy) => {
                    self.give_back(piece, &peer, false);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Ok(Response::NotFound) => {
                    // The peer stopped sharing this.
                    strikes.store(MAX_STRIKES, Ordering::Relaxed);
                    self.give_back(piece, &peer, false);
                }
                Ok(_) | Err(_) => {
                    strikes.fetch_add(1, Ordering::Relaxed);
                    self.give_back(piece, &peer, false);
                    let _ = self.events.send(Event::PieceFailed {
                        index: piece,
                        reason: format!("request to {} failed", peer.fmt_short()),
                    });
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    fn give_back(&self, piece: usize, peer: &EndpointId, peer_lacks_it: bool) {
        self.sched.lock().unwrap().failed(piece, peer, peer_lacks_it);
        self.wake.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distant_peers_get_more_requests_in_flight() {
        let ms = |n| Some(Duration::from_millis(n));
        assert_eq!(requests_for(None), REQUESTS_PER_PEER);
        assert_eq!(requests_for(ms(1)), REQUESTS_PER_PEER, "nearby peers keep the minimum");
        // 12 MiB/s over 200 ms is 2.4 MiB in flight: 39 pieces of 64 KiB.
        assert_eq!(requests_for(ms(200)), 39);
        assert_eq!(requests_for(ms(900)), MAX_REQUESTS_PER_PEER, "capped");
    }
}
