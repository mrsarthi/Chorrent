//! On-disk state in `<data_dir>/chorrent.redb`: the node's identity, the
//! shares we seed, and partial downloads.
//!
//! Download progress is saved as each file's outboard (the proof hashes we
//! have verified so far). On resume, those are checked against what is
//! actually on disk, so a stale or tampered state file can't make us serve
//! or keep bad data.

use crate::chunker::FileOutboard;
use crate::error::{Error, Result};
use crate::local::{LocalFile, LocalShare};
use crate::manifest::{Manifest, ShareId};
use crate::share::ShareCode;
use iroh::SecretKey;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
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
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir).map_err(|source| Error::Io { path: data_dir.to_path_buf(), source })?;
        let db = Database::create(data_dir.join("chorrent.redb")).map_err(|e| match e {
            redb::DatabaseError::DatabaseAlreadyOpen => Error::DataDirInUse(data_dir.to_path_buf()),
            e => storage_err(e),
        })?;
        let tx = db.begin_write().map_err(storage_err)?;
        for table in [META, SEEDS, DOWNLOADS, PROGRESS] {
            tx.open_table(table).map_err(storage_err)?;
        }
        tx.commit().map_err(storage_err)?;
        Ok(Self { db, write_lock: Mutex::new(()) })
    }

    fn get(&self, table: TableDefinition<&str, &[u8]>, key: &str) -> Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read().map_err(storage_err)?;
        let t = tx.open_table(table).map_err(storage_err)?;
        Ok(t.get(key).map_err(storage_err)?.map(|v| v.value().to_vec()))
    }

    fn put(&self, table: TableDefinition<&str, &[u8]>, key: &str, value: &[u8]) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap();
        let tx = self.db.begin_write().map_err(storage_err)?;
        tx.open_table(table).map_err(storage_err)?.insert(key, value).map_err(storage_err)?;
        tx.commit().map_err(storage_err)
    }

    fn remove(&self, tables: &[TableDefinition<&str, &[u8]>], key: &str) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap();
        let tx = self.db.begin_write().map_err(storage_err)?;
        for table in tables {
            tx.open_table(*table).map_err(storage_err)?.remove(key).map_err(storage_err)?;
        }
        tx.commit().map_err(storage_err)
    }

    fn entries(&self, table: TableDefinition<&str, &[u8]>) -> Result<Vec<(String, Vec<u8>)>> {
        let tx = self.db.begin_read().map_err(storage_err)?;
        let t = tx.open_table(table).map_err(storage_err)?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(storage_err)? {
            let (k, v) = entry.map_err(storage_err)?;
            out.push((k.value().to_string(), v.value().to_vec()));
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
            root: share.root_path(),
            secret: share.secret,
            manifest: share.manifest_bytes.clone(),
            files: share
                .files
                .iter()
                .map(|f| {
                    let (size, mtime) = modified(&f.path).unwrap_or((0, 0));
                    (f.path.clone(), size, mtime)
                })
                .collect(),
            outboards: share.outboard_snapshots(),
        };
        let bytes = postcard::to_allocvec(&record).map_err(storage_err)?;
        self.put(SEEDS, &share.id.to_string(), &bytes)
    }

    /// Rebuild a seed from saved state, re-hashing only if any file changed
    /// on disk since it was saved.
    pub fn load_seed(
        &self,
        id: &ShareId,
        events: tokio::sync::broadcast::Sender<crate::event::Event>,
    ) -> Result<LocalShare> {
        let bytes = self.get(SEEDS, &id.to_string())?.ok_or_else(|| storage_err("no such saved seed"))?;
        let record: SeedRecord = postcard::from_bytes(&bytes).map_err(storage_err)?;
        let unchanged = record.files.iter().all(|(path, size, mtime)| modified(path) == Some((*size, *mtime)));
        if unchanged && record.outboards.len() == record.files.len() {
            let manifest = Manifest::decode_verified(&record.manifest, *id)?;
            let files = record
                .files
                .iter()
                .zip(record.outboards)
                .zip(manifest.files())
                .map(|(((path, _, _), data), entry)| {
                    let mut outboard: FileOutboard = crate::chunker::empty_outboard(entry.root_hash(), entry.size());
                    outboard.data = data;
                    LocalFile { path: path.clone(), outboard: std::sync::Mutex::new(outboard) }
                })
                .collect();
            return Ok(LocalShare::from_parts(manifest, record.manifest, files, record.secret, events, true));
        }
        let share = LocalShare::from_disk(&record.root, record.secret, events)?;
        if share.id != *id {
            return Err(Error::BadManifest(format!("{} changed since it was shared", record.root.display())));
        }
        Ok(share)
    }

    pub fn forget(&self, id: &ShareId) -> Result<()> {
        self.remove(&[SEEDS], &id.to_string())?;
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
