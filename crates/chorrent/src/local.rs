//! A share as this node holds it: where its files live on disk, which
//! pieces we have and can prove, who may fetch them, and who to tell when
//! that changes.

use crate::chunker::{self, FileOutboard};
use crate::error::{ChunkerError, Error, Result};
use crate::event::Event;
use crate::manifest::{Layout, Manifest, ShareId};
use crate::protocol::pack_bits;
use crate::storage::{FileData, MasterKey};
use iroh::EndpointId;
use positioned_io::ReadAt;
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

pub(crate) struct LocalFile {
    pub data: FileData,
    pub outboard: Mutex<FileOutboard>,
    /// Kept open while pieces arrive: opening the file for every piece is
    /// slow (on Windows especially), and encrypted files must be opened for
    /// reading and writing.
    writer: Mutex<Option<crate::storage::Handle>>,
}

impl LocalFile {
    pub fn new(data: FileData, outboard: FileOutboard) -> Self {
        Self { data, outboard: Mutex::new(outboard), writer: Mutex::new(None) }
    }
}

/// Where new share data is kept when the client has a store.
#[derive(Debug, Clone)]
pub(crate) struct StoreConfig {
    /// Folder holding one subfolder per share.
    pub blobs: PathBuf,
    /// Present when the store is encrypted.
    pub key: Option<MasterKey>,
}

impl StoreConfig {
    /// A fresh folder for a share being imported (its id isn't known until
    /// it's hashed, so the name is random).
    fn new_share_dir(&self) -> PathBuf {
        self.blobs.join(format!("import-{}", hex(&rand::random::<[u8; 8]>())))
    }

    /// The folder a download of `id` goes to, the same every time so it resumes.
    pub fn download_dir(&self, id: &ShareId) -> PathBuf {
        self.blobs.join(id.to_string())
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) struct LocalShare {
    pub id: ShareId,
    pub manifest: Manifest,
    pub manifest_bytes: Vec<u8>,
    pub layout: Layout,
    pub files: Vec<LocalFile>,
    pub secret: Option<[u8; 32]>,
    /// The share's file or folder on disk, or its folder in the encrypted store.
    pub root: PathBuf,
    /// When set, only these peers may fetch anything (even with the code).
    allowed: RwLock<Option<HashSet<EndpointId>>>,
    have: RwLock<Vec<bool>>,
    /// Piece numbers as we verify them, for peers subscribed to our progress.
    pub have_tx: broadcast::Sender<u32>,
    pub events: broadcast::Sender<Event>,
}

impl std::fmt::Debug for LocalShare {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalShare").field("id", &self.id).field("name", &self.manifest.name()).finish()
    }
}

impl LocalShare {
    /// Share a file or folder. With an encrypted store, its bytes are copied
    /// into the store (read once, hashed on the way); otherwise they're
    /// hashed and served in place. Blocking: reads everything.
    pub fn from_disk(
        root: &Path,
        secret: Option<[u8; 32]>,
        events: broadcast::Sender<Event>,
        store: Option<&StoreConfig>,
    ) -> Result<Self> {
        let encrypted = store.and_then(|s| s.key.clone().map(|key| (s.new_share_dir(), key)));
        let (manifest, hashed) = match &encrypted {
            None => Manifest::from_disk(root, |_, path, _| {
                Ok((FileData::Plain(path.to_path_buf()), chunker::hash_file(path)?))
            })?,
            Some((dir, key)) => Manifest::from_disk(root, |index, path, size| {
                let data = FileData::Encrypted { path: dir.join(format!("{index}.enc")), key: key.clone(), size };
                let source = std::fs::File::open(path).map_err(|source| Error::Io { path: path.to_path_buf(), source })?;
                let outboard = chunker::import(source, size, &data)?;
                Ok((data, outboard))
            })?,
        };
        let share_root = encrypted.map(|(dir, _)| dir).unwrap_or_else(|| root.to_path_buf());
        Ok(Self::assemble(manifest, hashed, secret, events, share_root))
    }

    /// Share one file given as a stream of `size` bytes (e.g. an Android
    /// content URI), stored under `store`. Blocking: reads everything.
    pub fn from_reader(
        name: &str,
        size: u64,
        reader: impl Read,
        secret: Option<[u8; 32]>,
        events: broadcast::Sender<Event>,
        store: &StoreConfig,
    ) -> Result<Self> {
        let dir = store.new_share_dir();
        let data = match &store.key {
            Some(key) => FileData::Encrypted { path: dir.join("0.enc"), key: key.clone(), size },
            None => FileData::Plain(dir.join(name)),
        };
        let outboard = chunker::import(reader, size, &data)?;
        let manifest = Manifest::single(name, &outboard)?;
        let root = match &data {
            FileData::Plain(path) => path.clone(),
            FileData::Encrypted { .. } => dir,
        };
        Ok(Self::assemble(manifest, vec![(data, outboard)], secret, events, root))
    }

    fn assemble(
        manifest: Manifest,
        hashed: Vec<(FileData, FileOutboard)>,
        secret: Option<[u8; 32]>,
        events: broadcast::Sender<Event>,
        root: PathBuf,
    ) -> Self {
        let manifest_bytes = manifest.encode();
        let files = hashed
            .into_iter()
            .map(|(data, outboard)| LocalFile::new(data, outboard))
            .collect();
        Self::from_parts(manifest, manifest_bytes, files, secret, events, true, root)
    }

    /// Prepare to download `manifest`. Plain: into `dest_dir`, under the
    /// share's own file and folder names; files already there in full count
    /// as downloaded. Encrypted (`key` given): `dest_dir` is the share's store
    /// folder and files are named by index. Blocking: may hash existing files.
    pub fn for_download(
        manifest: Manifest,
        manifest_bytes: Vec<u8>,
        dest_dir: &Path,
        key: Option<&MasterKey>,
        secret: Option<[u8; 32]>,
        events: broadcast::Sender<Event>,
    ) -> Result<Self> {
        let mut files = Vec::with_capacity(manifest.files().len());
        let mut already_complete = Vec::new();
        for (index, entry) in manifest.files().iter().enumerate() {
            let data = match key {
                Some(key) => FileData::Encrypted {
                    path: dest_dir.join(format!("{index}.enc")),
                    key: key.clone(),
                    size: entry.size(),
                },
                None => FileData::Plain(dest_dir.join(entry.path())),
            };
            let path = data.path().to_path_buf();
            let existing = !data.is_encrypted()
                && std::fs::metadata(&path).is_ok_and(|m| m.is_file() && m.len() == entry.size());
            let outboard = match existing.then(|| chunker::hash_file(&path)) {
                Some(Ok(full)) if full.root == entry.root_hash() => {
                    already_complete.push(index);
                    full
                }
                _ => {
                    chunker::preallocate(&data, entry.size()).map_err(|source| Error::Io { path, source })?;
                    chunker::empty_outboard(entry.root_hash(), entry.size())
                }
            };
            files.push(LocalFile::new(data, outboard));
        }
        let root = match key {
            Some(_) => dest_dir.to_path_buf(),
            None => dest_dir.join(manifest.name()),
        };
        let share = Self::from_parts(manifest, manifest_bytes, files, secret, events, false, root);
        for index in already_complete {
            for piece in share.layout.pieces_of(index) {
                share.mark_have(piece);
            }
        }
        Ok(share)
    }

    /// `complete`: whether every piece is already on disk and in the outboards.
    pub fn from_parts(
        manifest: Manifest,
        manifest_bytes: Vec<u8>,
        files: Vec<LocalFile>,
        secret: Option<[u8; 32]>,
        events: broadcast::Sender<Event>,
        complete: bool,
        root: PathBuf,
    ) -> Self {
        let (have_tx, _) = broadcast::channel(4096);
        let layout = Layout::new(&manifest);
        Self {
            id: Manifest::id_of(&manifest_bytes),
            have: RwLock::new(vec![complete; layout.total_pieces]),
            manifest,
            manifest_bytes,
            layout,
            files,
            secret,
            root,
            allowed: RwLock::new(None),
            have_tx,
            events,
        }
    }

    pub fn is_encrypted(&self) -> bool {
        self.files.iter().any(|f| f.data.is_encrypted())
    }

    pub fn total_pieces(&self) -> usize {
        self.layout.total_pieces
    }

    pub fn has(&self, piece: usize) -> bool {
        self.have.read().unwrap().get(piece).copied().unwrap_or(false)
    }

    pub fn have_snapshot(&self) -> Vec<bool> {
        self.have.read().unwrap().clone()
    }

    pub fn have_count(&self) -> usize {
        self.have.read().unwrap().iter().filter(|&&h| h).count()
    }

    pub fn packed_bitfield(&self) -> Vec<u8> {
        pack_bits(&self.have.read().unwrap())
    }

    pub fn piece_len(&self, piece: usize) -> u64 {
        let loc = self.layout.locate(piece);
        loc.end - loc.start
    }

    pub fn set_allowed(&self, peers: Option<HashSet<EndpointId>>) {
        *self.allowed.write().unwrap() = peers;
    }

    /// Whether `requester` may see this share: on the allowlist (if there is
    /// one), and holding the secret (if the share is private).
    pub fn authorized(&self, requester: &EndpointId, token: Option<&[u8; 32]>) -> bool {
        if let Some(allowed) = &*self.allowed.read().unwrap()
            && !allowed.contains(requester)
        {
            return false;
        }
        match &self.secret {
            None => true,
            Some(secret) => token == Some(&crate::share::access_token(secret, requester)),
        }
    }

    /// Encode a piece we hold, with its proof. Blocking (disk read).
    pub fn encode_piece(&self, piece: usize) -> Result<Vec<u8>, ChunkerError> {
        let loc = self.layout.locate(piece);
        let file = &self.files[loc.file];
        let outboard = file.outboard.lock().unwrap();
        chunker::encode_range(&file.data, &outboard, loc.start, loc.end)
    }

    /// Verify and store a piece. Returns false if we already had it (e.g.
    /// a duplicate request during endgame). Blocking (disk write).
    pub fn store_piece(&self, piece: usize, encoded: &[u8]) -> Result<bool, ChunkerError> {
        if self.has(piece) {
            return Ok(false);
        }
        let loc = self.layout.locate(piece);
        let file = &self.files[loc.file];
        let entry = &self.manifest.files()[loc.file];
        let nodes = {
            let mut writer = file.writer.lock().unwrap();
            if writer.is_none() {
                let handle = file.data.open(true).map_err(|source| ChunkerError::Receive {
                    path: file.data.path().to_path_buf(),
                    source,
                })?;
                *writer = Some(handle);
            }
            let handle = writer.as_mut().expect("opened above");
            chunker::decode_range(handle, file.data.path(), entry.root_hash(), entry.size(), loc.start, loc.end, encoded)?
        };
        chunker::apply_proof(&mut file.outboard.lock().unwrap(), &nodes)
            .map_err(|source| ChunkerError::Receive { path: file.data.path().to_path_buf(), source })?;
        self.mark_have(piece);
        Ok(true)
    }

    fn mark_have(&self, piece: usize) {
        let newly = {
            let mut have = self.have.write().unwrap();
            !std::mem::replace(&mut have[piece], true)
        };
        if newly {
            let _ = self.have_tx.send(piece as u32);
        }
    }

    /// After a restart: given a saved outboard per file, work out which
    /// pieces on disk are really there and intact. Blocking (reads files).
    pub fn restore_progress(&self, outboards: Vec<Vec<u8>>) -> Result<()> {
        for (index, (file, data)) in self.files.iter().zip(outboards).enumerate() {
            if self.layout.pieces_of(index).all(|p| self.has(p)) {
                continue; // already found complete on disk
            }
            let mut outboard = file.outboard.lock().unwrap();
            if data.len() != outboard.data.len() {
                continue; // saved state doesn't fit this file; start it over
            }
            outboard.data = data;
            let ranges = chunker::valid_byte_ranges(&file.data, &outboard)
                .map_err(|source| Error::Io { path: file.data.path().to_path_buf(), source })?;
            drop(outboard);
            let size = self.manifest.files()[index].size();
            for piece in self.layout.pieces_of(index) {
                let loc = self.layout.locate(piece);
                let end = loc.end.min(size);
                if ranges.iter().any(|r| r.start <= loc.start && r.end.min(size) >= end) {
                    self.mark_have(piece);
                }
            }
        }
        Ok(())
    }

    /// Close the files kept open for writing (once nothing more will arrive).
    pub fn release_writers(&self) {
        for file in &self.files {
            if let Some(mut handle) = file.writer.lock().unwrap().take() {
                let _ = positioned_io::WriteAt::flush(&mut handle);
            }
        }
    }

    /// Check every file against its hash, reading it back from wherever it's
    /// stored. Blocking (reads everything).
    pub fn verify(&self) -> Result<()> {
        for (file, entry) in self.files.iter().zip(self.manifest.files()) {
            if chunker::hash_data(&file.data, entry.size())?.root != entry.root_hash() {
                return Err(Error::HashMismatch(file.data.path().to_path_buf()));
            }
        }
        Ok(())
    }

    fn check_file(&self, index: usize) -> Result<&LocalFile> {
        let file = self.files.get(index).ok_or_else(|| Error::NotAvailable(format!("no file number {index}")))?;
        if !self.layout.pieces_of(index).all(|p| self.has(p)) {
            return Err(Error::NotAvailable("that file hasn't finished downloading".into()));
        }
        Ok(file)
    }

    /// Read bytes of a fully downloaded file (decrypting if needed), e.g. to
    /// show an image without writing it out. Blocking (disk read).
    pub fn read_range(&self, index: usize, offset: u64, len: usize) -> Result<Vec<u8>> {
        let file = self.check_file(index)?;
        let size = self.manifest.files()[index].size();
        let len = len.min(size.saturating_sub(offset) as usize);
        let mut buf = vec![0u8; len];
        let path = file.data.path().to_path_buf();
        let handle = file.data.open_strict().map_err(|source| Error::Io { path: path.clone(), source })?;
        handle.read_exact_at(offset, &mut buf).map_err(|source| Error::Io { path, source })?;
        Ok(buf)
    }

    /// Write a fully downloaded file out as an ordinary file at `dest`
    /// (which must not exist yet). Blocking (copies the file).
    pub fn export_file(&self, index: usize, dest: &Path) -> Result<()> {
        let file = self.check_file(index)?;
        let io_err = |source| Error::Io { path: dest.to_path_buf(), source };
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
        let mut out = std::fs::OpenOptions::new().write(true).create_new(true).open(dest).map_err(io_err)?;
        let mut reader = file.data.reader().map_err(io_err)?;
        let copied = std::io::copy(&mut (&mut reader).take(self.manifest.files()[index].size()), &mut out).map_err(io_err)?;
        if copied != self.manifest.files()[index].size() {
            let _ = std::fs::remove_file(dest);
            return Err(io_err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "file shorter than expected")));
        }
        Ok(())
    }

    pub fn outboard_snapshots(&self) -> Vec<Vec<u8>> {
        self.files.iter().map(|f| f.outboard.lock().unwrap().data.clone()).collect()
    }
}

pub(crate) type SharedShare = Arc<LocalShare>;
