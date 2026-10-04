//! On-disk state in `<data_dir>/chorrent.redb`: the node's identity, the
//! shares we seed, and partial downloads.
//!
//! Download progress is saved as each file's outboard (the proof hashes we
//! have verified so far). On resume, those are checked against what is
//! actually on disk, so a stale or tampered state file can't make us serve
//! or keep bad data.
//!
//! ## Sealed databases
//!
//! With a storage key (`ClientBuilder::encrypted_storage`), nothing readable
//! is left in the database: every value is encrypted (XChaCha20-Poly1305,
//! bound to its table and record), and record names, which would otherwise
//! contain share ids and paths, are replaced by keyed hashes. The real name
//! travels inside the sealed value. Both keys are derived from the storage
//! key, never the storage key itself.
//!
//! A database is either sealed or plain, never a mix: opening a plain one
//! that has records with a key, or a sealed one without the key, is refused.

use crate::chunker::FileOutboard;
use crate::error::{Error, Result};
use crate::local::{LocalFile, LocalShare, StoreConfig};
use crate::storage::FileData;
use iroh::EndpointId;
use std::collections::HashSet;
use crate::manifest::{Manifest, ShareId};
use crate::share::ShareCode;
use iroh::SecretKey;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// share id hex → SeedRecord
const SEEDS: TableDefinition<&str, &[u8]> = TableDefinition::new("seeds");
/// "share id hex|dest dir" → DownloadRecord
const DOWNLOADS: TableDefinition<&str, &[u8]> = TableDefinition::new("downloads");
/// same key as DOWNLOADS → per-file outboards
const PROGRESS: TableDefinition<&str, &[u8]> = TableDefinition::new("progress");
/// share id hex → allowlisted peer ids (absent: no allowlist)
const ACCESS: TableDefinition<&str, &[u8]> = TableDefinition::new("access");
/// The tables holding records (everything but the format markers in META).
const RECORD_TABLES: [TableDefinition<&str, &[u8]>; 4] = [SEEDS, DOWNLOADS, PROGRESS, ACCESS];

/// META entries stored unsealed: they're needed before any key is checked,
/// and neither reveals anything.
const FORMAT: &str = "format";
const SEALED_V1: &[u8] = b"sealed-v1";
const FINGERPRINT: &str = "storage_key_fingerprint";

#[derive(Serialize, Deserialize)]
struct SeedRecord {
    root: PathBuf,
    secret: Option<[u8; 32]>,
    manifest: Vec<u8>,
    /// (path, size, modified-time) per file, to tell whether re-hashing is needed.
    files: Vec<(PathBuf, u64, u128)>,
    outboards: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct DownloadRecord {
    code: String,
    dest_dir: PathBuf,
}

/// Something from a previous run that can be picked up again with
/// [`Client::resume`](crate::Client::resume).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SavedTransfer {
    /// A share we were seeding, from the file or folder at `path`.
    #[allow(missing_docs)]
    Seed { id: ShareId, path: PathBuf, private: bool },
    /// An unfinished download into `dest_dir`.
    #[allow(missing_docs)]
    Download { code: ShareCode, dest_dir: PathBuf },
}

pub(crate) struct Store {
    db: Database,
    /// Serializes progress writes; redb allows one writer at a time anyway.
    write_lock: Mutex<()>,
    /// Present when the database is sealed.
    sealer: Option<Sealer>,
}

/// Encrypts values and hides record names for a sealed database.
struct Sealer {
    cipher: XChaCha20Poly1305,
    names: [u8; 32],
}

impl Sealer {
    fn new(storage_key: &[u8; 32]) -> Self {
        let values = blake3::derive_key("chorrent database values v1", storage_key);
        Self {
            cipher: XChaCha20Poly1305::new(&values.into()),
            names: blake3::derive_key("chorrent database names v1", storage_key),
        }
    }

    /// What a record is stored under: a keyed hash, so ids and paths in
    /// record names don't show.
    fn name(&self, table: &str, name: &str) -> String {
        let mut hasher = blake3::Hasher::new_keyed(&self.names);
        hasher.update(table.as_bytes());
        hasher.update(&[0]);
        hasher.update(name.as_bytes());
        hasher.finalize().to_hex().to_string()
    }

    /// Nonce | ciphertext of (real name, value), bound to table and stored name.
    fn seal(&self, table: &str, stored_name: &str, name: &str, value: &[u8]) -> Result<Vec<u8>> {
        let plain = postcard::to_allocvec(&(name, value)).map_err(storage_err)?;
        let nonce: [u8; 24] = rand::random();
        let aad = format!("{table}\0{stored_name}");
        let sealed = self
            .cipher
            .encrypt(&XNonce::from(nonce), Payload { msg: &plain, aad: aad.as_bytes() })
            .map_err(|_| storage_err("could not encrypt a database record"))?;
        let mut out = nonce.to_vec();
        out.extend(sealed);
        Ok(out)
    }

    /// The real name and value of a sealed record.
    fn open(&self, table: &str, stored_name: &str, sealed: &[u8]) -> Result<(String, Vec<u8>)> {
        let unreadable = || storage_err("a database record is damaged or was written with another key");
        if sealed.len() < 24 {
            return Err(unreadable());
        }
        let (nonce, ciphertext) = sealed.split_at(24);
        let nonce = XNonce::try_from(nonce).map_err(|_| unreadable())?;
        let aad = format!("{table}\0{stored_name}");
        let plain = self
            .cipher
            .decrypt(&nonce, Payload { msg: ciphertext, aad: aad.as_bytes() })
            .map_err(|_| unreadable())?;
        postcard::from_bytes::<(String, Vec<u8>)>(&plain).map_err(|_| unreadable())
    }
}

fn storage_err(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

fn download_key(id: &ShareId, dest_dir: &Path) -> String {
    format!("{id}|{}", dest_dir.display())
}

fn modified(path: &Path) -> Option<(u64, u128)> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_nanos();
    Some((meta.len(), mtime))
}

impl Store {
    /// Open the data dir's database. With `storage_key`, it's sealed (see the
    /// module docs); a plain database that already has records is refused.
    pub fn open(data_dir: &Path, storage_key: Option<&[u8; 32]>) -> Result<Self> {
        std::fs::create_dir_all(data_dir).map_err(|source| Error::Io { path: data_dir.to_path_buf(), source })?;
        let db = Database::create(data_dir.join("chorrent.redb")).map_err(|e| match e {
            redb::DatabaseError::DatabaseAlreadyOpen => Error::DataDirInUse(data_dir.to_path_buf()),
            e => storage_err(e),
        })?;
        let tx = db.begin_write().map_err(storage_err)?;
        for table in [META, SEEDS, DOWNLOADS, PROGRESS, ACCESS] {
            tx.open_table(table).map_err(storage_err)?;
        }
        tx.commit().map_err(storage_err)?;
        let store = Self { db, write_lock: Mutex::new(()), sealer: storage_key.map(Sealer::new) };
        store.check_format(storage_key)?;
        Ok(store)
    }

    /// Make sure the database is sealed exactly when we have a key, and that
    /// it's the right key. Only a one-way fingerprint of the key is kept.
    fn check_format(&self, storage_key: Option<&[u8; 32]>) -> Result<()> {
        let format = self.raw_get(META, FORMAT)?;
        let saved_fingerprint = self.raw_get(META, FINGERPRINT)?;
        match storage_key {
            Some(key) => {
                let fingerprint = blake3::derive_key("chorrent storage key fingerprint v1", key);
                if saved_fingerprint.as_deref().is_some_and(|saved| saved != fingerprint) {
                    return Err(Error::Storage("this data dir was set up with a different storage key".into()));
                }
                match format.as_deref() {
                    Some(SEALED_V1) => Ok(()),
                    Some(_) => Err(Error::Storage("this data dir was written by a newer chorrent".into())),
                    None if self.has_records()? => Err(Error::Storage(
                        "this data dir already has records stored without encryption (written by chorrent \
                         0.5.0, or without a storage key). Encrypted storage needs a data dir of its own: \
                         use a new folder, or delete this one to start over"
                            .into(),
                    )),
                    None => {
                        self.raw_put(META, FINGERPRINT, &fingerprint)?;
                        self.raw_put(META, FORMAT, SEALED_V1)
                    }
                }
            }
            None if format.is_some() || saved_fingerprint.is_some() => Err(Error::Storage(
                "this data dir is encrypted; open it with its storage key (ClientBuilder::encrypted_storage)".into(),
            )),
            None => Ok(()),
        }
    }

    /// Whether anything besides format markers was ever stored.
    fn has_records(&self) -> Result<bool> {
        let tx = self.db.begin_read().map_err(storage_err)?;
        for table in RECORD_TABLES {
            if !tx.open_table(table).map_err(storage_err)?.is_empty().map_err(storage_err)? {
                return Ok(true);
            }
        }
        let meta = tx.open_table(META).map_err(storage_err)?;
        for entry in meta.iter().map_err(storage_err)? {
            let (name, _) = entry.map_err(storage_err)?;
            if name.value() != FORMAT && name.value() != FINGERPRINT {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn raw_get(&self, table: TableDefinition<&str, &[u8]>, name: &str) -> Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read().map_err(storage_err)?;
        let t = tx.open_table(table).map_err(storage_err)?;
        Ok(t.get(name).map_err(storage_err)?.map(|v| v.value().to_vec()))
    }

    fn raw_put(&self, table: TableDefinition<&str, &[u8]>, name: &str, value: &[u8]) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap();
        let tx = self.db.begin_write().map_err(storage_err)?;
        tx.open_table(table).map_err(storage_err)?.insert(name, value).map_err(storage_err)?;
        tx.commit().map_err(storage_err)
    }

    /// The name a record is stored under (hidden when sealed).
    fn stored_name(&self, table: TableDefinition<&str, &[u8]>, name: &str) -> String {
        match &self.sealer {
            Some(sealer) => sealer.name(table.name(), name),
            None => name.to_string(),
        }
    }

    fn get(&self, table: TableDefinition<&str, &[u8]>, name: &str) -> Result<Option<Vec<u8>>> {
        let stored = self.stored_name(table, name);
        let Some(value) = self.raw_get(table, &stored)? else { return Ok(None) };
        match &self.sealer {
            None => Ok(Some(value)),
            Some(sealer) => {
                let (real_name, value) = sealer.open(table.name(), &stored, &value)?;
                if real_name != name {
                    return Err(storage_err("a database record doesn't belong where it's stored"));
                }
                Ok(Some(value))
            }
        }
    }

    fn put(&self, table: TableDefinition<&str, &[u8]>, name: &str, value: &[u8]) -> Result<()> {
        let stored = self.stored_name(table, name);
        match &self.sealer {
            None => self.raw_put(table, &stored, value),
            Some(sealer) => self.raw_put(table, &stored, &sealer.seal(table.name(), &stored, name, value)?),
        }
    }

    fn remove(&self, tables: &[TableDefinition<&str, &[u8]>], name: &str) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap();
        let tx = self.db.begin_write().map_err(storage_err)?;
        for table in tables {
            let stored = self.stored_name(*table, name);
            tx.open_table(*table).map_err(storage_err)?.remove(stored.as_str()).map_err(storage_err)?;
        }
        tx.commit().map_err(storage_err)
    }

    /// Every record in a table, by its real name.
    fn entries(&self, table: TableDefinition<&str, &[u8]>) -> Result<Vec<(String, Vec<u8>)>> {
        let tx = self.db.begin_read().map_err(storage_err)?;
        let t = tx.open_table(table).map_err(storage_err)?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(storage_err)? {
            let (k, v) = entry.map_err(storage_err)?;
            out.push(match &self.sealer {
                None => (k.value().to_string(), v.value().to_vec()),
                Some(sealer) => sealer.open(table.name(), k.value(), v.value())?,
            });
        }
        Ok(out)
    }

    /// The node's secret key, created on first use. Keeping it stable means
    /// share codes handed out earlier still reach us after a restart.
    pub fn node_key(&self) -> Result<SecretKey> {
        if let Some(bytes) = self.get(META, "node_key")?
            && let Ok(bytes) = <[u8; 32]>::try_from(bytes.as_slice())
        {
            return Ok(SecretKey::from_bytes(&bytes));
        }
        let key = SecretKey::generate();
        self.put(META, "node_key", &key.to_bytes())?;
        Ok(key)
    }

    // ---- seeds ----

    pub fn remember_seed(&self, share: &LocalShare) -> Result<()> {
        let record = SeedRecord {
            root: share.root.clone(),
            secret: share.secret,
            manifest: share.manifest_bytes.clone(),
            files: share
                .files
                .iter()
                .map(|f| {
                    let path = f.data.path().to_path_buf();
                    let (size, mtime) = modified(&path).unwrap_or((0, 0));
                    (path, size, mtime)
                })
                .collect(),
            outboards: share.outboard_snapshots(),
        };
        let bytes = postcard::to_allocvec(&record).map_err(storage_err)?;
        self.put(SEEDS, &share.id.to_string(), &bytes)
    }

    /// Rebuild a seed from saved state. Plain files are re-hashed only if
    /// they changed on disk since; files in the encrypted store are checked
    /// against their hashes if they changed (they shouldn't).
    pub fn load_seed(
        &self,
        id: &ShareId,
        events: tokio::sync::broadcast::Sender<crate::event::Event>,
        store: Option<&StoreConfig>,
    ) -> Result<LocalShare> {
        let bytes = self.get(SEEDS, &id.to_string())?.ok_or_else(|| Error::NotAvailable(format!("no saved share {id}")))?;
        let record: SeedRecord = postcard::from_bytes(&bytes).map_err(storage_err)?;
        let unchanged = record.files.iter().all(|(path, size, mtime)| modified(path) == Some((*size, *mtime)));
        let manifest = Manifest::decode_verified(&record.manifest, *id)?;
        let key = store.and_then(|s| s.key.as_ref());
        let mut files = Vec::with_capacity(record.files.len());
        for ((path, _, _), entry) in record.files.iter().zip(manifest.files()) {
            let data = FileData::detect(path.clone(), key, entry.size())
                .map_err(|source| Error::Io { path: path.clone(), source })?;
            files.push(data);
        }
        let encrypted = files.iter().any(FileData::is_encrypted);
        if !unchanged && !encrypted {
            let share = LocalShare::from_disk(&record.root, record.secret, events, None)?;
            if share.id != *id {
                return Err(Error::BadManifest(format!("{} changed since it was shared", record.root.display())));
            }
            share.set_allowed(self.load_allowed(id));
            return Ok(share);
        }
        if record.outboards.len() != files.len() {
            return Err(storage_err("saved share is incomplete"));
        }
        let files = files
            .into_iter()
            .zip(record.outboards)
            .zip(manifest.files())
            .map(|((data, saved), entry)| {
                let mut outboard: FileOutboard = crate::chunker::empty_outboard(entry.root_hash(), entry.size());
                outboard.data = saved;
                LocalFile::new(data, outboard)
            })
            .collect();
        let share = LocalShare::from_parts(manifest, record.manifest, files, record.secret, events, true, record.root);
        if !unchanged {
            share.verify()?;
        }
        share.set_allowed(self.load_allowed(id));
        Ok(share)
    }

    /// The saved location of a share, if we have it.
    pub fn seed_root(&self, id: &ShareId) -> Option<PathBuf> {
        let bytes = self.get(SEEDS, &id.to_string()).ok()??;
        postcard::from_bytes::<SeedRecord>(&bytes).ok().map(|r| r.root)
    }

    pub fn remember_allowed(&self, id: &ShareId, peers: Option<&HashSet<EndpointId>>) -> Result<()> {
        match peers {
            None => self.remove(&[ACCESS], &id.to_string()),
            Some(peers) => {
                let raw: Vec<[u8; 32]> = peers.iter().map(|p| *p.as_bytes()).collect();
                let bytes = postcard::to_allocvec(&raw).map_err(storage_err)?;
                self.put(ACCESS, &id.to_string(), &bytes)
            }
        }
    }

    pub fn load_allowed(&self, id: &ShareId) -> Option<HashSet<EndpointId>> {
        let bytes = self.get(ACCESS, &id.to_string()).ok()??;
        let raw: Vec<[u8; 32]> = postcard::from_bytes(&bytes).ok()?;
        Some(raw.iter().filter_map(|b| EndpointId::from_bytes(b).ok()).collect())
    }

    pub fn forget(&self, id: &ShareId) -> Result<()> {
        self.remove(&[SEEDS, ACCESS], &id.to_string())?;
        for (key, _) in self.entries(DOWNLOADS)? {
            if key.starts_with(&format!("{id}|")) {
                self.remove(&[DOWNLOADS, PROGRESS], &key)?;
            }
        }
        Ok(())
    }

    // ---- downloads ----

    pub fn remember_download(&self, code: &ShareCode, dest_dir: &Path) -> Result<()> {
        let record = DownloadRecord { code: code.to_string(), dest_dir: dest_dir.to_path_buf() };
        let bytes = postcard::to_allocvec(&record).map_err(storage_err)?;
        self.put(DOWNLOADS, &download_key(&code.id, dest_dir), &bytes)
    }

    /// Best effort: losing a progress save only means re-downloading a bit.
    pub fn save_progress(&self, share: &LocalShare, dest_dir: &Path) {
        if let Ok(bytes) = postcard::to_allocvec(&share.outboard_snapshots()) {
            let _ = self.put(PROGRESS, &download_key(&share.id, dest_dir), &bytes);
        }
    }

    pub fn load_progress(&self, id: &ShareId, dest_dir: &Path) -> Option<Vec<Vec<u8>>> {
        let bytes = self.get(PROGRESS, &download_key(id, dest_dir)).ok()??;
        postcard::from_bytes(&bytes).ok()
    }

    pub fn mark_complete(&self, id: &ShareId, dest_dir: &Path) -> Result<()> {
        self.remove(&[DOWNLOADS, PROGRESS], &download_key(id, dest_dir))
    }

    pub fn saved(&self) -> Result<Vec<SavedTransfer>> {
        let mut out = Vec::new();
        for (key, bytes) in self.entries(SEEDS)? {
            let (Ok(id), Ok(record)) = (key.parse::<ShareId>(), postcard::from_bytes::<SeedRecord>(&bytes)) else {
                continue;
            };
            out.push(SavedTransfer::Seed { id, path: record.root, private: record.secret.is_some() });
        }
        for (_, bytes) in self.entries(DOWNLOADS)? {
            let Ok(record) = postcard::from_bytes::<DownloadRecord>(&bytes) else { continue };
            let Ok(code) = record.code.parse() else { continue };
            out.push(SavedTransfer::Download { code, dest_dir: record.dest_dir });
        }
        Ok(out)
    }
}
