use crate::discovery::{self, Swarm};
use crate::download::{self, DownloadCtx, DownloadOutcome};
use crate::error::{Error, Result};
use crate::event::{Event, EVENT_CAPACITY};
use crate::handler::{ChorrentProtocol, Registry};
use crate::limits::RateLimiter;
use crate::local::{LocalShare, SharedShare, StoreConfig};
use crate::manifest::{Manifest, ShareId};
use crate::node::ChorrentNode;
use crate::registration::Registration;
use crate::share::{new_secret, ShareCode};
use crate::storage::MasterKey;
use crate::store::{SavedTransfer, Store};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

/// Settings for a [`Client`]. Start from [`Client::builder`].
#[derive(Clone)]
pub struct ClientBuilder {
    data_dir: Option<PathBuf>,
    endpoint: Option<Endpoint>,
    storage_key: Option<[u8; 32]>,
    gossip: Option<bool>,
    reseed: bool,
    max_uploads: usize,
    upload_limit: Option<u64>,
    download_limit: Option<u64>,
    discovery_timeout: Duration,
    mainline_dht: bool,
    /// None: the default for the mode (see `ClientBuilder::wait_for_relay`).
    relay_wait: Option<Duration>,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("data_dir", &self.data_dir)
            .field("embedded", &self.endpoint.is_some())
            .field("encrypted", &self.storage_key.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            data_dir: None,
            endpoint: None,
            storage_key: None,
            gossip: None,
            reseed: true,
            max_uploads: 32,
            upload_limit: None,
            download_limit: None,
            discovery_timeout: Duration::from_secs(30),
            mainline_dht: false,
            relay_wait: None,
        }
    }
}

impl ClientBuilder {
    /// Keep state between runs in this folder: the node's identity (so share
    /// codes stay valid across restarts), seeds, and download progress (so
    /// downloads resume). Without it, everything is in memory only.
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }

    /// Run on an iroh endpoint your app already owns, instead of binding a
    /// new one: same identity, same relays, same connections to each peer.
    ///
    /// The endpoint must list [`Client::alpns`] among its ALPNs, and your
    /// accept loop must pass connections with those ALPNs to
    /// [`Client::handle_connection`]. Gossip is off by default here.
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Keep everything this client stores encrypted at rest with this key,
    /// e.g. one held in the OS keychain: downloads, copies of what it seeds,
    /// and its database (file names, share codes and secrets, progress, and
    /// who may download what). Even the names of saved records and folders
    /// are hidden. Plaintext only leaves the store through
    /// [`Client::read_range`] and [`Client::export_file`]. Needs a
    /// [`ClientBuilder::data_dir`].
    ///
    /// The data dir must be new, or one only ever used with this key: a
    /// data dir that already holds unencrypted records (from chorrent 0.5.0,
    /// or used without a key) is refused rather than mixed, as is opening an
    /// encrypted one without its key. Losing the key makes everything
    /// stored unreadable.
    pub fn encrypted_storage(mut self, key: [u8; 32]) -> Self {
        self.storage_key = Some(key);
        self
    }

    /// Find more peers through gossip (needed for swarms with peers the share
    /// code doesn't name). On by default, except with [`ClientBuilder::endpoint`].
    pub fn gossip(mut self, enabled: bool) -> Self {
        self.gossip = Some(enabled);
        self
    }

    /// Whether downloads serve the pieces they have to other peers, during
    /// and after the download. On by default.
    pub fn reseed(mut self, reseed: bool) -> Self {
        self.reseed = reseed;
        self
    }

    /// Most pieces uploaded at once across all peers; extra requests are
    /// told to retry. Default 32.
    pub fn max_concurrent_uploads(mut self, n: usize) -> Self {
        self.max_uploads = n.max(1);
        self
    }

    /// Cap upload bandwidth, in bytes per second. `None` (default) is unlimited.
    pub fn upload_limit(mut self, bytes_per_sec: Option<u64>) -> Self {
        self.upload_limit = bytes_per_sec;
        self
    }

    /// Cap download bandwidth, in bytes per second. `None` (default) is unlimited.
    pub fn download_limit(mut self, bytes_per_sec: Option<u64>) -> Self {
        self.download_limit = bytes_per_sec;
        self
    }

    /// How long a download looks for a first peer before failing with
    /// [`Error::NoPeersFound`]. Default 30 seconds.
    pub fn discovery_timeout(mut self, timeout: Duration) -> Self {
        self.discovery_timeout = timeout;
        self
    }

    /// Also publish and look up node addresses on the BitTorrent Mainline
    /// DHT, alongside n0's DNS service. Share codes then keep reaching a peer
    /// even if its address changed since the code was made. Only relay
    /// addresses are published, never IP addresses. Off by default. Has no
    /// effect with [`ClientBuilder::endpoint`] (configure your endpoint instead).
    #[cfg(feature = "mainline")]
    pub fn mainline_dht(mut self, enabled: bool) -> Self {
        self.mainline_dht = enabled;
        self
    }

    /// How long [`ClientBuilder::build`] waits for the relay connection, so
    /// the first share codes work from other networks. Waits only this once,
    /// at startup; seeding never waits.
    ///
    /// Default: 10 seconds when chorrent creates its own endpoint, none with
    /// [`ClientBuilder::endpoint`] (your app manages that connection; see
    /// [`Client::wait_for_relay`] and [`Client::network_status`]).
    pub fn wait_for_relay(mut self, timeout: Duration) -> Self {
        self.relay_wait = Some(timeout);
        self
    }

    /// Start the client: bind (or attach to) the endpoint and open the data dir.
    pub async fn build(self) -> Result<Client> {
        if self.storage_key.is_some() && self.data_dir.is_none() {
            return Err(Error::Storage("encrypted storage needs a data_dir".into()));
        }
        let store = match &self.data_dir {
            Some(dir) => Some(Arc::new(Store::open(dir, self.storage_key.as_ref())?)),
            None => None,
        };
        let store_cfg = self.data_dir.as_ref().map(|dir| StoreConfig {
            blobs: dir.join("blobs"),
            key: self.storage_key.map(MasterKey::new),
        });
        let registry = Registry::default();
        let handler = ChorrentProtocol {
            shares: Arc::clone(&registry),
            upload_slots: Arc::new(Semaphore::new(self.max_uploads)),
            upload_rate: self.upload_limit.map(|r| Arc::new(RateLimiter::new(r))),
        };
        let embedded = self.endpoint.is_some();
        let node = match self.endpoint {
            Some(endpoint) => ChorrentNode::attach(endpoint, handler, self.gossip.unwrap_or(false)),
            None => {
                let key = match &store {
                    Some(store) => store.node_key()?,
                    None => SecretKey::generate(),
                };
                ChorrentNode::bind(handler, key, self.gossip.unwrap_or(true), self.mainline_dht).await?
            }
        };
        let relay_wait = self.relay_wait.unwrap_or(if embedded { Duration::ZERO } else { Duration::from_secs(10) });
        if !relay_wait.is_zero() {
            // If it doesn't connect in time we carry on: share codes pick up
            // the relay when it does, and network_status() says so meanwhile.
            node.wait_for_relay(relay_wait).await;
        }
        Ok(Client {
            node: Arc::new(node),
            registry,
            reseed: self.reseed,
            download_rate: self.download_limit.map(|r| Arc::new(RateLimiter::new(r))),
            store,
            store_cfg,
            discovery_timeout: self.discovery_timeout,
        })
    }
}

/// A chorrent node: one network identity that can seed and download any
/// number of shares at once.
///
/// Must be created and used inside a tokio runtime.
pub struct Client {
    node: Arc<ChorrentNode>,
    registry: Registry,
    reseed: bool,
    download_rate: Option<Arc<RateLimiter>>,
    store: Option<Arc<Store>>,
    store_cfg: Option<StoreConfig>,
    discovery_timeout: Duration,
}

/// Extra settings for [`Client::seed_with`] and [`Client::seed_reader`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SeedOptions {
    /// Only people with the share code can find or download it.
    pub private: bool,
    /// Join the swarm another seeder of the same content started, so both
    /// seeders find each other's downloaders. A private share stays private.
    pub join: Option<ShareCode>,
    /// Only these peers may download, even if others get hold of the code.
    pub allow_peers: Option<HashSet<EndpointId>>,
}

impl SeedOptions {
    /// See [`SeedOptions::private`].
    pub fn private(mut self, private: bool) -> Self {
        self.private = private;
        self
    }

    /// See [`SeedOptions::join`].
    pub fn join(mut self, share: ShareCode) -> Self {
        self.join = Some(share);
        self
    }

    /// See [`SeedOptions::allow_peers`].
    pub fn allow_peers(mut self, peers: impl IntoIterator<Item = EndpointId>) -> Self {
        self.allow_peers = Some(peers.into_iter().collect());
        self
    }
}

/// Extra settings for [`Client::download_with`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DownloadOptions {
    /// Folder to download into (default: the current directory). Must be
    /// `None` with an encrypted store, where downloads always go into the store.
    pub dest_dir: Option<PathBuf>,
    /// While reseeding, only these peers may fetch from us.
    pub allow_peers: Option<HashSet<EndpointId>>,
}

impl DownloadOptions {
    /// See [`DownloadOptions::dest_dir`].
    pub fn dest_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dest_dir = Some(dir.into());
        self
    }

    /// See [`DownloadOptions::allow_peers`].
    pub fn allow_peers(mut self, peers: impl IntoIterator<Item = EndpointId>) -> Self {
        self.allow_peers = Some(peers.into_iter().collect());
        self
    }
}

/// How this node is connected to the network; see [`Client::network_status`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct NetworkStatus {
    /// This node's id.
    pub id: String,
    /// The relay server we're connected to. `None` means peers on other
    /// networks probably can't reach us: only direct connections work.
    pub relay: Option<String>,
    /// Our own IP addresses and ports, as other peers would be told them.
    pub direct_addresses: Vec<String>,
}

/// The result of trying one peer from a share code; see [`Client::check_peers`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct PeerCheck {
    /// The peer's id.
    pub peer: String,
    /// Whether the code gave a relay address for this peer.
    pub listed_relay: bool,
    /// We connected to it.
    pub reachable: bool,
    /// Directly (hole punched) rather than through a relay; `None` if unreachable.
    pub direct: Option<bool>,
    /// Round-trip time, if reachable.
    pub rtt_ms: Option<u64>,
    /// It answered that it serves this share (to us).
    pub serves_share: bool,
    /// What went wrong, if anything.
    pub problem: Option<String>,
}

/// Either kind of transfer, as returned by [`Client::resume`].
pub enum Transfer {
    /// A seed, serving again.
    Seed(SeedHandle),
    /// A download, continuing from the pieces already on disk.
    Download(DownloadHandle),
}

impl Client {
    /// A client with default settings and no saved state.
    pub async fn new() -> Result<Self> {
        Self::builder().build().await
    }

    /// Start configuring a client; see [`ClientBuilder`].
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// This node's id: how other peers (and events) refer to it.
    pub fn id(&self) -> String {
        self.node.id().to_string()
    }

    /// Wait until this node is connected to a relay, for at most `timeout`,
    /// and return whether it is. Share codes made before that may only work
    /// for devices that can connect directly. Returns at once if already
    /// connected.
    pub async fn wait_for_relay(&self, timeout: Duration) -> bool {
        self.node.wait_for_relay(timeout).await
    }

    /// Whether we're connected to a relay, and our addresses. Without a
    /// relay, peers on other networks usually can't reach us.
    pub fn network_status(&self) -> NetworkStatus {
        let addr = self.node.addr();
        NetworkStatus {
            id: self.id(),
            relay: self.node.relay(),
            direct_addresses: addr.ip_addrs().map(|a| a.to_string()).collect(),
        }
    }

    /// Try to reach every peer a share code lists, and ask each whether it
    /// serves the share. For diagnosing "why can't I download this".
    pub async fn check_peers(&self, code: &ShareCode) -> Vec<PeerCheck> {
        let mut checks = JoinSet::new();
        for addr in code.peers.clone() {
            let (node, code) = (Arc::clone(&self.node), code.clone());
            checks.spawn(async move { download::check_peer(&node, &code, addr).await });
        }
        checks.join_all().await
    }

    /// The ALPNs to add to your endpoint when using [`ClientBuilder::endpoint`].
    pub fn alpns(&self) -> Vec<Vec<u8>> {
        self.node.alpns()
    }

    /// Serve an incoming connection whose ALPN is one of [`Client::alpns`]
    /// (with [`ClientBuilder::endpoint`]). Returns when the connection ends,
    /// so spawn it.
    pub async fn handle_connection(&self, connection: Connection) {
        let _ = self.node.handle(connection).await;
    }

    /// Start seeding a file or folder. It stays available until the
    /// returned handle is stopped or dropped.
    pub async fn seed(&self, path: impl AsRef<Path>) -> Result<SeedHandle> {
        self.seed_with(path, SeedOptions::default()).await
    }

    /// Like [`Client::seed`], with options such as making the share private.
    /// With an encrypted store, the content is copied into the store first.
    pub async fn seed_with(&self, path: impl AsRef<Path>, options: SeedOptions) -> Result<SeedHandle> {
        let path = absolute(path.as_ref())?;
        let secret = self.secret_for(&options);
        let (events, _) = broadcast::channel(EVENT_CAPACITY);

        // Hashing reads everything, so keep it off the async worker threads.
        let share = {
            let (events, store_cfg) = (events.clone(), self.store_cfg.clone());
            tokio::task::spawn_blocking(move || LocalShare::from_disk(&path, secret, events, store_cfg.as_ref()))
                .await
                .map_err(|e| Error::Task(e.to_string()))??
        };
        self.finish_seed(share, options, events).await
    }

    /// Seed a single file given as a stream of exactly `size` bytes (for
    /// example an Android content URI), named `name`. It's copied into the
    /// client's store (encrypted if configured), so needs a data dir.
    pub async fn seed_reader(
        &self,
        name: &str,
        size: u64,
        reader: impl std::io::Read + Send + 'static,
        options: SeedOptions,
    ) -> Result<SeedHandle> {
        let store_cfg = self.store_cfg.clone().ok_or_else(|| Error::Storage("seed_reader needs a data_dir".into()))?;
        let secret = self.secret_for(&options);
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let share = {
            let (events, name) = (events.clone(), name.to_string());
            tokio::task::spawn_blocking(move || LocalShare::from_reader(&name, size, reader, secret, events, &store_cfg))
                .await
                .map_err(|e| Error::Task(e.to_string()))??
        };
        self.finish_seed(share, options, events).await
    }

    fn secret_for(&self, options: &SeedOptions) -> Option<[u8; 32]> {
        match &options.join {
            Some(join) => join.secret,
            None => options.private.then(new_secret),
        }
    }

    async fn finish_seed(&self, share: LocalShare, options: SeedOptions, events: broadcast::Sender<Event>) -> Result<SeedHandle> {
        if let Some(join) = &options.join
            && join.id != share.id
        {
            return Err(Error::InvalidShareCode("the --join share code is for different content".into()));
        }
        share.set_allowed(options.allow_peers.clone());
        if let Some(store) = &self.store {
            store.remember_allowed(&share.id, options.allow_peers.as_ref())?;
        }
        // Joining seeders know the other seeders' addresses: use them to join
        // the swarm, and list them in our own share code too.
        let known = options.join.map(|j| j.peers).unwrap_or_default();
        self.start_seed(share, known, events).await
    }

    async fn start_seed(
        &self,
        share: LocalShare,
        known: Vec<EndpointAddr>,
        events: broadcast::Sender<Event>,
    ) -> Result<SeedHandle> {
        let share = Arc::new(share);
        let registration = Registration::new(&self.registry, Arc::clone(&share))?;
        if let Some(store) = &self.store {
            store.remember_seed(&share)?;
        }
        // No waiting for the relay here: that happens once, at startup (see
        // ClientBuilder::wait_for_relay). share_code() always uses our
        // current address, so it includes the relay as soon as it connects.
        let code = ShareCode {
            id: share.id,
            name: share.manifest.name().to_string(),
            total_size: share.manifest.total_size(),
            peers: Vec::new(),
            secret: share.secret,
        };
        let mut tasks = JoinSet::new();
        if self.node.gossip().is_some() {
            let swarm = Swarm {
                node: Arc::clone(&self.node),
                topic: code.topic(),
                bootstrap: known.clone(),
                announce: Arc::new(AtomicBool::new(true)),
                found: None,
                events: events.clone(),
            };
            tasks.spawn(async move {
                if let Ok((sender, receiver, warm)) = discovery::join(&swarm).await {
                    discovery::keep(swarm, sender, receiver, warm).await;
                }
            });
        }
        Ok(SeedHandle {
            code,
            known,
            node: Arc::clone(&self.node),
            events,
            store: self.store.clone(),
            _registration: registration,
            _tasks: tasks,
        })
    }

    /// Start downloading a share into `dest_dir` (default: the current
    /// directory; must be `None` with an encrypted store). The share's file
    /// or folder is created inside it.
    pub async fn download(&self, share: &ShareCode, dest_dir: Option<PathBuf>) -> Result<DownloadHandle> {
        self.download_with(share, DownloadOptions { dest_dir, ..Default::default() }).await
    }

    /// Like [`Client::download`], with options such as an allowlist for reseeding.
    pub async fn download_with(&self, share: &ShareCode, options: DownloadOptions) -> Result<DownloadHandle> {
        let key = self.store_cfg.as_ref().and_then(|s| s.key.clone());
        let dest_dir = match (&key, options.dest_dir) {
            (Some(_), Some(_)) => {
                return Err(Error::Storage("with encrypted storage, downloads go into the store; pass no dest_dir".into()));
            }
            (Some(_), None) => self.store_cfg.as_ref().expect("key implies a store").download_dir(&share.id),
            (None, dest) => absolute(&dest.unwrap_or_else(|| PathBuf::from(".")))?,
        };

        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let ctx = DownloadCtx {
            node: Arc::clone(&self.node),
            registry: Arc::clone(&self.registry),
            reseed: self.reseed,
            download_rate: self.download_rate.clone(),
            store: self.store.clone(),
            discovery_timeout: self.discovery_timeout,
            key,
            allow: options.allow_peers,
        };
        let (cancel, cancelled) = watch::channel(false);
        let task = tokio::spawn(download::run(ctx, share.clone(), dest_dir, events.clone(), cancelled));
        Ok(DownloadHandle {
            code: share.clone(),
            node: Arc::clone(&self.node),
            store: self.store.clone(),
            events,
            cancel,
            task: Some(task),
        })
    }

    /// Seeds and unfinished downloads saved by an earlier run (needs
    /// [`ClientBuilder::data_dir`]; empty otherwise).
    pub fn saved(&self) -> Result<Vec<SavedTransfer>> {
        match &self.store {
            Some(store) => store.saved(),
            None => Ok(Vec::new()),
        }
    }

    /// Pick a saved transfer back up. Seeds skip re-hashing when their files
    /// haven't changed; downloads keep every piece already on disk.
    pub async fn resume(&self, saved: &SavedTransfer) -> Result<Transfer> {
        match saved {
            SavedTransfer::Seed { id, .. } => {
                let (events, _) = broadcast::channel(EVENT_CAPACITY);
                let share = self.load_saved(*id, events.clone()).await?;
                Ok(Transfer::Seed(self.start_seed(share, Vec::new(), events).await?))
            }
            SavedTransfer::Download { code, dest_dir } => {
                let encrypted = self.store_cfg.as_ref().is_some_and(|s| s.key.is_some());
                let options = DownloadOptions {
                    dest_dir: (!encrypted).then(|| dest_dir.clone()),
                    allow_peers: self.store.as_ref().and_then(|s| s.load_allowed(&code.id)),
                };
                Ok(Transfer::Download(self.download_with(code, options).await?))
            }
        }
    }

    async fn load_saved(&self, id: ShareId, events: broadcast::Sender<Event>) -> Result<LocalShare> {
        let store = self.store.clone().ok_or_else(|| Error::Storage("no data_dir configured".into()))?;
        let store_cfg = self.store_cfg.clone();
        tokio::task::spawn_blocking(move || store.load_seed(&id, events, store_cfg.as_ref()))
            .await
            .map_err(|e| Error::Task(e.to_string()))?
    }

    /// A share that's being served now, or a finished one in saved state.
    async fn find(&self, id: &ShareId) -> Result<SharedShare> {
        if let Some(share) = self.registry.read().unwrap().get(id) {
            return Ok(Arc::clone(share));
        }
        let (events, _) = broadcast::channel(1);
        Ok(Arc::new(self.load_saved(*id, events).await?))
    }

    /// The file list of a share we hold (serving it, or finished and saved).
    pub async fn manifest(&self, id: &ShareId) -> Result<Manifest> {
        Ok(self.find(id).await?.manifest.clone())
    }

    /// Read `len` bytes at `offset` of file number `file` (in
    /// [`Manifest::files`] order) of a share we hold completely, decrypting
    /// if needed. Nothing is written to disk.
    pub async fn read_range(&self, id: &ShareId, file: usize, offset: u64, len: usize) -> Result<Vec<u8>> {
        let share = self.find(id).await?;
        tokio::task::spawn_blocking(move || share.read_range(file, offset, len))
            .await
            .map_err(|e| Error::Task(e.to_string()))?
    }

    /// Write file number `file` of a share we hold completely to `dest` as an
    /// ordinary (plaintext) file. `dest` must not exist yet.
    pub async fn export_file(&self, id: &ShareId, file: usize, dest: impl Into<PathBuf>) -> Result<()> {
        let share = self.find(id).await?;
        let dest = dest.into();
        tokio::task::spawn_blocking(move || share.export_file(file, &dest))
            .await
            .map_err(|e| Error::Task(e.to_string()))?
    }

    /// Remove a share from saved state, so it isn't offered by [`Client::saved`]
    /// again. Doesn't touch any files or stop a running transfer.
    pub fn forget(&self, id: &ShareId) -> Result<()> {
        match &self.store {
            Some(store) => store.forget(id),
            None => Ok(()),
        }
    }

    /// Stop serving a share, forget it, and delete its data if it lives in
    /// this client's store (files you seeded from elsewhere are never deleted).
    pub async fn remove(&self, id: &ShareId) -> Result<()> {
        let served = self.registry.write().unwrap().remove(id);
        let root = served.map(|s| s.root.clone()).or_else(|| self.store.as_ref().and_then(|s| s.seed_root(id)));
        self.forget(id)?;
        // The share's own folder in the store (its root may be a file inside it).
        let in_store = root.zip(self.store_cfg.as_ref()).and_then(|(root, cfg)| {
            let first = root.strip_prefix(&cfg.blobs).ok()?.components().next()?;
            Some(cfg.blobs.join(first))
        });
        if let Some(dir) = in_store {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(Error::Io { path: dir, source }),
            }
        }
        Ok(())
    }

    /// Close every connection and stop the node. With
    /// [`ClientBuilder::endpoint`], your endpoint is left running.
    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).map_err(|source| Error::Io { path: path.to_path_buf(), source })
}

/// A share being seeded. Seeding stops when this is stopped or dropped.
pub struct SeedHandle {
    /// The code without addresses; see [`SeedHandle::share_code`].
    code: ShareCode,
    /// Other seeders we know of, listed in our code after ourselves.
    known: Vec<EndpointAddr>,
    node: Arc<ChorrentNode>,
    events: broadcast::Sender<Event>,
    store: Option<Arc<Store>>,
    _registration: Registration,
    _tasks: JoinSet<()>,
}

impl SeedHandle {
    /// The code downloaders need; share its `Display` form. It lists our
    /// current address (so ask again after the network changes, e.g. once
    /// [`Client::network_status`] shows the relay connected) followed by the
    /// other seeders we know of.
    pub fn share_code(&self) -> ShareCode {
        let mut code = self.code.clone();
        let me = self.node.addr();
        code.peers = std::iter::once(me.clone())
            .chain(self.known.iter().filter(|p| p.id != me.id).cloned())
            .collect();
        code
    }

    /// Where the shared file or folder is on disk (for an encrypted store,
    /// the share's folder inside it).
    pub fn path(&self) -> &Path {
        &self._registration.share.root
    }

    /// Change who may download (`None`: anyone with the share code), e.g.
    /// when a group's membership changes. Saved with the share.
    pub fn set_allowed_peers(&self, peers: Option<impl IntoIterator<Item = EndpointId>>) -> Result<()> {
        let peers: Option<HashSet<EndpointId>> = peers.map(|p| p.into_iter().collect());
        if let Some(store) = &self.store {
            store.remember_allowed(&self.code.id, peers.as_ref())?;
        }
        self._registration.share.set_allowed(peers);
        Ok(())
    }

    /// Upload activity for this share. See [`Event`] for the caveats.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Stop serving this share (same as dropping the handle).
    pub fn stop(self) {}
}

/// A download in progress. Dropping it cancels the download (pieces already
/// on disk are kept, and resumable with a data dir).
pub struct DownloadHandle {
    code: ShareCode,
    node: Arc<ChorrentNode>,
    store: Option<Arc<Store>>,
    events: broadcast::Sender<Event>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<Result<DownloadOutcome>>>,
}

/// A completed, verified download.
#[non_exhaustive]
pub struct Finished {
    /// The share that was downloaded.
    pub id: ShareId,
    /// The downloaded file or folder, or `None` when it's in an encrypted
    /// store (read it with [`Client::read_range`] / [`Client::export_file`]).
    pub path: Option<PathBuf>,
    /// When reseeding is on, the download keeps being served from here
    /// until this is dropped or stopped.
    pub seed: Option<SeedHandle>,
}

impl DownloadHandle {
    /// The share code this download was started with.
    pub fn share_code(&self) -> &ShareCode {
        &self.code
    }

    /// Progress for this download. See [`Event`] for the caveats.
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Stop the download, saving progress first. [`DownloadHandle::finished`]
    /// then returns [`Error::Cancelled`].
    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    /// Wait for the download to end. On success, every file has been
    /// verified against the share's hashes.
    pub async fn finished(mut self) -> Result<Finished> {
        let task = self.task.take().expect("task is only taken here");
        let outcome = match task.await {
            Ok(result) => result?,
            Err(e) if e.is_cancelled() => return Err(Error::Cancelled),
            Err(e) => return Err(Error::Task(e.to_string())),
        };
        let encrypted = outcome.encrypted;
        let seed = match outcome.seeding {
            None => None,
            Some((registration, tasks)) => {
                if let Some(store) = &self.store {
                    store.remember_seed(&registration.share)?;
                }
                Some(SeedHandle {
                    code: self.code.clone(),
                    known: self.code.peers.clone(),
                    node: Arc::clone(&self.node),
                    events: self.events.clone(),
                    store: self.store.clone(),
                    _registration: registration,
                    _tasks: tasks,
                })
            }
        };
        if seed.is_none()
            && encrypted
            && let Some((store, share)) = self.store.as_ref().zip(outcome.share.as_ref())
        {
            // Not reseeding, but it only exists inside the store: keep it
            // findable for read_range/export_file.
            store.remember_seed(share)?;
        }
        Ok(Finished { id: self.code.id, path: (!encrypted).then_some(outcome.path), seed })
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        // The task notices, saves progress and exits on its own.
        let _ = self.cancel.send(true);
    }
}
