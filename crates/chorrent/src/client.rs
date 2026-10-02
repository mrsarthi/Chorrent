use crate::discovery::{self, Announcement};
use crate::download::{self, DownloadCtx, DownloadOutcome};
use crate::error::{Error, Result};
use crate::event::{Event, EVENT_CAPACITY};
use crate::handler::{ChorrentProtocol, Registry};
use crate::limits::RateLimiter;
use crate::local::LocalShare;
use crate::manifest::ShareId;
use crate::node::ChorrentNode;
use crate::registration::Registration;
use crate::share::{new_secret, ShareCode};
use crate::store::{SavedTransfer, Store};
use iroh::{EndpointId, SecretKey};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

/// Settings for a [`Client`]. Start from [`Client::builder`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    data_dir: Option<PathBuf>,
    reseed: bool,
    max_uploads: usize,
    upload_limit: Option<u64>,
    download_limit: Option<u64>,
    discovery_timeout: Duration,
    mainline_dht: bool,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            data_dir: None,
            reseed: true,
            max_uploads: 32,
            upload_limit: None,
            download_limit: None,
            discovery_timeout: Duration::from_secs(30),
            mainline_dht: false,
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
    /// addresses are published, never IP addresses. Off by default.
    #[cfg(feature = "mainline")]
    pub fn mainline_dht(mut self, enabled: bool) -> Self {
        self.mainline_dht = enabled;
        self
    }

    /// Bind the node and open the data dir (if any).
    pub async fn build(self) -> Result<Client> {
        let store = match &self.data_dir {
            Some(dir) => Some(Arc::new(Store::open(dir)?)),
            None => None,
        };
        let key = match &store {
            Some(store) => store.node_key()?,
            None => SecretKey::generate(),
        };
        let registry = Registry::default();
        let handler = ChorrentProtocol {
            shares: Arc::clone(&registry),
            upload_slots: Arc::new(Semaphore::new(self.max_uploads)),
            upload_rate: self.upload_limit.map(|r| Arc::new(RateLimiter::new(r))),
        };
        let node = ChorrentNode::bind(handler, key, self.mainline_dht).await?;
        Ok(Client {
            node: Arc::new(node),
            registry,
            reseed: self.reseed,
            download_rate: self.download_limit.map(|r| Arc::new(RateLimiter::new(r))),
            store,
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
    discovery_timeout: Duration,
}

/// Extra settings for [`Client::seed_with`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SeedOptions {
    /// Only people with the share code can find or download it.
    pub private: bool,
    /// Join the swarm another seeder of the same content started, so both
    /// seeders find each other's downloaders. A private share stays private.
    pub join: Option<ShareCode>,
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

    /// Start seeding a file or folder. It stays available until the
    /// returned handle is stopped or dropped.
    pub async fn seed(&self, path: impl AsRef<Path>) -> Result<SeedHandle> {
        self.seed_with(path, SeedOptions::default()).await
    }

    /// Like [`Client::seed`], with options such as making the share private.
    pub async fn seed_with(&self, path: impl AsRef<Path>, options: SeedOptions) -> Result<SeedHandle> {
        let path = absolute(path.as_ref())?;
        let secret = match &options.join {
            Some(join) => join.secret,
            None => options.private.then(new_secret),
        };
        let (events, _) = broadcast::channel(EVENT_CAPACITY);

        // Hashing reads everything, so keep it off the async worker threads.
        let share = {
            let events = events.clone();
            tokio::task::spawn_blocking(move || LocalShare::from_disk(&path, secret, events))
                .await
                .map_err(|e| Error::Task(e.to_string()))??
        };
        if let Some(join) = &options.join
            && join.id != share.id
        {
            return Err(Error::InvalidShareCode("the --join share code is for different content".into()));
        }
        let bootstrap = options.join.map(|j| j.bootstrap_ids()).unwrap_or_default();
        self.start_seed(share, bootstrap, events).await
    }

    async fn start_seed(
        &self,
        share: LocalShare,
        bootstrap: Vec<EndpointId>,
        events: broadcast::Sender<Event>,
    ) -> Result<SeedHandle> {
        let share = Arc::new(share);
        let registration = Registration::new(&self.registry, Arc::clone(&share))?;
        if let Some(store) = &self.store {
            store.remember_seed(&share)?;
        }
        let code = ShareCode {
            id: share.id,
            name: share.manifest.name().to_string(),
            total_size: share.manifest.total_size(),
            peers: vec![self.node.reachable_addr().await],
            secret: share.secret,
        };
        let (sender, receiver) = self
            .node
            .gossip()
            .subscribe(code.topic(), bootstrap)
            .await
            .map_err(|e| Error::Gossip(e.to_string()))?
            .split();

        let mut tasks = JoinSet::new();
        // Seeders don't need the peer list, but the receiver has to be
        // drained to stay subscribed.
        tasks.spawn(discovery::listen_for_peers(receiver, None));
        tasks.spawn(discovery::announce_periodically(sender, Announcement { addr: self.node.addr() }));
        Ok(SeedHandle { code, path: share.root_path(), events, _registration: registration, _tasks: tasks })
    }

    /// Start downloading a share into `dest_dir` (default: the current
    /// directory). The share's file or folder is created inside it.
    pub async fn download(&self, share: &ShareCode, dest_dir: Option<PathBuf>) -> Result<DownloadHandle> {
        let dest_dir = absolute(&dest_dir.unwrap_or_else(|| PathBuf::from(".")))?;
        let gossip = self
            .node
            .gossip()
            .subscribe(share.topic(), share.bootstrap_ids())
            .await
            .map_err(|e| Error::Gossip(e.to_string()))?
            .split();

        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let ctx = DownloadCtx {
            node: Arc::clone(&self.node),
            registry: Arc::clone(&self.registry),
            reseed: self.reseed,
            download_rate: self.download_rate.clone(),
            store: self.store.clone(),
            discovery_timeout: self.discovery_timeout,
        };
        let (cancel, cancelled) = watch::channel(false);
        let task = tokio::spawn(download::run(ctx, share.clone(), dest_dir, gossip, events.clone(), cancelled));
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
                let store = self.store.clone().ok_or_else(|| Error::Storage("no data_dir configured".into()))?;
                let (events, _) = broadcast::channel(EVENT_CAPACITY);
                let share = {
                    let (id, events) = (*id, events.clone());
                    tokio::task::spawn_blocking(move || store.load_seed(&id, events))
                        .await
                        .map_err(|e| Error::Task(e.to_string()))??
                };
                Ok(Transfer::Seed(self.start_seed(share, Vec::new(), events).await?))
            }
            SavedTransfer::Download { code, dest_dir } => {
                Ok(Transfer::Download(self.download(code, Some(dest_dir.clone())).await?))
            }
        }
    }

    /// Remove a share from saved state, so it isn't offered by [`Client::saved`]
    /// again. Doesn't touch any files or stop a running transfer.
    pub fn forget(&self, id: &ShareId) -> Result<()> {
        match &self.store {
            Some(store) => store.forget(id),
            None => Ok(()),
        }
    }

    /// Close every connection and stop the node.
    pub async fn shutdown(self) {
        self.node.shutdown().await;
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).map_err(|source| Error::Io { path: path.to_path_buf(), source })
}

/// A share being seeded. Seeding stops when this is stopped or dropped.
pub struct SeedHandle {
    code: ShareCode,
    path: PathBuf,
    events: broadcast::Sender<Event>,
    _registration: Registration,
    _tasks: JoinSet<()>,
}

impl SeedHandle {
    /// The code downloaders need. Share its `Display` form.
    pub fn share_code(&self) -> &ShareCode {
        &self.code
    }

    /// Where the shared file or folder is on disk.
    pub fn path(&self) -> &Path {
        &self.path
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
    /// The downloaded file or folder.
    pub path: PathBuf,
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
        let seed = match outcome.seeding {
            None => None,
            Some((registration, tasks)) => {
                if let Some(store) = &self.store {
                    store.remember_seed(&registration.share)?;
                }
                let mut code = self.code.clone();
                code.peers.insert(0, self.node.addr());
                Some(SeedHandle {
                    code,
                    path: outcome.path.clone(),
                    events: self.events.clone(),
                    _registration: registration,
                    _tasks: tasks,
                })
            }
        };
        Ok(Finished { path: outcome.path, seed })
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        // The task notices, saves progress and exits on its own.
        let _ = self.cancel.send(true);
    }
}
