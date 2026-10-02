//! A share as this node holds it: where its files live on disk, which
//! pieces we have and can prove, and who to tell when that changes.

use crate::chunker::{self, FileOutboard};
use crate::error::{ChunkerError, Error, Result};
use crate::event::Event;
use crate::manifest::{Layout, Manifest, ShareId};
use crate::protocol::pack_bits;
use iroh::EndpointId;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

pub(crate) struct LocalFile {
    pub path: PathBuf,
    pub outboard: Mutex<FileOutboard>,
}

pub(crate) struct LocalShare {
    pub id: ShareId,
    pub manifest: Manifest,
    pub manifest_bytes: Vec<u8>,
    pub layout: Layout,
    pub files: Vec<LocalFile>,
    pub secret: Option<[u8; 32]>,
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
    /// Hash a file or folder and hold all of it. Blocking: reads everything.
    pub fn from_disk(root: &Path, secret: Option<[u8; 32]>, events: broadcast::Sender<Event>) -> Result<Self> {
        let (manifest, hashed) = Manifest::from_disk(root)?;
        let manifest_bytes = manifest.encode();
        let files = hashed
            .into_iter()
            .map(|(path, outboard)| LocalFile { path, outboard: Mutex::new(outboard) })
            .collect();
        Ok(Self::from_parts(manifest, manifest_bytes, files, secret, events, true))
    }

    /// Prepare to download `manifest` into `dest_dir`: create every file at
    /// its final size. Files already there in full (same size and hash) count
    /// as downloaded. Blocking: may hash existing files.
    pub fn for_download(
        manifest: Manifest,
        manifest_bytes: Vec<u8>,
        dest_dir: &Path,
        secret: Option<[u8; 32]>,
        events: broadcast::Sender<Event>,
    ) -> Result<Self> {
        let mut files = Vec::with_capacity(manifest.files().len());
        let mut already_complete = Vec::new();
        for (index, entry) in manifest.files().iter().enumerate() {
            let path = dest_dir.join(entry.path());
            let existing = std::fs::metadata(&path).is_ok_and(|m| m.is_file() && m.len() == entry.size());
            let outboard = match existing.then(|| chunker::hash_file(&path)) {
                Some(Ok(full)) if full.root == entry.root_hash() => {
                    already_complete.push(index);
                    full
                }
                _ => {
                    chunker::preallocate(&path, entry.size())
                        .map_err(|source| Error::Io { path: path.clone(), source })?;
                    chunker::empty_outboard(entry.root_hash(), entry.size())
                }
            };
            files.push(LocalFile { path, outboard: Mutex::new(outboard) });
        }
        let share = Self::from_parts(manifest, manifest_bytes, files, secret, events, false);
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
            have_tx,
            events,
        }
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

    /// Whether `requester` may see this share.
    pub fn authorized(&self, requester: &EndpointId, token: Option<&[u8; 32]>) -> bool {
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
        chunker::encode_range(&file.path, &outboard, loc.start, loc.end)
    }

    /// Verify and store a piece. Returns false if we already had it (e.g.
    /// a duplicate request during endgame). Blocking (disk write).
    pub fn store_piece(&self, piece: usize, encoded: &[u8]) -> Result<bool, ChunkerError> {
        if self.has(piece) {
            return Ok(false);
        }
        let loc = self.layout.locate(piece);
        let file = &self.files[loc.file];
        let root = self.manifest.files()[loc.file].root_hash();
        let size = self.manifest.files()[loc.file].size();
        let nodes = chunker::decode_range(&file.path, root, size, loc.start, loc.end, encoded)?;
        chunker::apply_proof(&mut file.outboard.lock().unwrap(), &nodes)
            .map_err(|source| ChunkerError::Receive { path: file.path.clone(), source })?;
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
            let ranges = chunker::valid_byte_ranges(&file.path, &outboard)
                .map_err(|source| Error::Io { path: file.path.clone(), source })?;
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

    pub fn outboard_snapshots(&self) -> Vec<Vec<u8>> {
        self.files.iter().map(|f| f.outboard.lock().unwrap().data.clone()).collect()
    }

    /// Where the share's top-level file or folder lives on disk.
    pub fn root_path(&self) -> PathBuf {
        match self.files.first() {
            // Strip the file's own components back to the share's first one.
            Some(file) => {
                let depth = self.manifest.files()[0].path().components().count();
                let mut root = file.path.clone();
                for _ in 1..depth {
                    root.pop();
                }
                root
            }
            None => PathBuf::from(self.manifest.name()),
        }
    }
}

pub(crate) type SharedShare = Arc<LocalShare>;
