//! What a share contains: one file or a folder tree, each file with its own
//! BLAKE3/Bao root hash. The share's id is the hash of its encoded manifest,
//! so a downloader can fetch the manifest from any peer and check it.

use crate::chunker::BLOCK_SIZE;
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

const MANIFEST_VERSION: u8 = 1;
/// Upper bound when fetching a manifest from a peer.
pub(crate) const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;

/// Identifies a share: the BLAKE3 hash of its manifest.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShareId(pub(crate) [u8; 32]);

impl ShareId {
    /// The raw 32-byte hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ShareId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&blake3::Hash::from_bytes(self.0).to_hex())
    }
}

impl fmt::Debug for ShareId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ShareId({})", &self.to_string()[..12])
    }
}

impl FromStr for ShareId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        blake3::Hash::from_hex(s)
            .map(|h| ShareId(*h.as_bytes()))
            .map_err(|e| Error::InvalidShareCode(format!("bad share id: {e}")))
    }
}

/// One file in a share.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path components relative to the download folder; the first is the
    /// share's name.
    path: Vec<String>,
    size: u64,
    root_hash: [u8; 32],
}

impl FileEntry {
    /// Relative path where this file is saved inside the download folder.
    pub fn path(&self) -> PathBuf {
        self.path.iter().collect()
    }

    /// Size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn root_hash(&self) -> blake3::Hash {
        blake3::Hash::from_bytes(self.root_hash)
    }

    pub(crate) fn pieces(&self) -> usize {
        self.size.div_ceil(BLOCK_SIZE.bytes() as u64) as usize
    }
}

/// The list of files in a share.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    name: String,
    files: Vec<FileEntry>,
}

impl Manifest {
    /// Name of the shared file or top-level folder.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Every file in the share, in a stable order.
    pub fn files(&self) -> &[FileEntry] {
        &self.files
    }

    /// Combined size of all files, in bytes.
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![MANIFEST_VERSION];
        bytes.extend(postcard::to_allocvec(self).expect("manifest always serializes"));
        bytes
    }

    pub(crate) fn id_of(encoded: &[u8]) -> ShareId {
        ShareId(*blake3::hash(encoded).as_bytes())
    }

    /// Decode a manifest a peer sent us, checking it is the one we asked for
    /// and that none of its paths can escape the download folder.
    pub(crate) fn decode_verified(encoded: &[u8], expected: ShareId) -> Result<Manifest> {
        if Self::id_of(encoded) != expected {
            return Err(Error::BadManifest("hash does not match the share code".into()));
        }
        let (&version, rest) = encoded
            .split_first()
            .ok_or_else(|| Error::BadManifest("empty".into()))?;
        if version != MANIFEST_VERSION {
            return Err(Error::BadManifest(format!("unsupported manifest version {version}")));
        }
        let manifest: Manifest =
            postcard::from_bytes(rest).map_err(|e| Error::BadManifest(e.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for file in &self.files {
            if file.path.is_empty() || file.path[0] != self.name {
                return Err(Error::BadManifest("file outside the share's folder".into()));
            }
            if let Some((name, why)) = file.path.iter().find_map(|c| name_problem(c).map(|why| (c, why))) {
                return Err(Error::BadManifest(format!("unsafe file name {name:?}: {why}")));
            }
            // Case-insensitive: these would collide on Windows and macOS.
            if !seen.insert(file.path.iter().map(|c| c.to_lowercase()).collect::<Vec<_>>()) {
                return Err(Error::BadManifest("duplicate file path".into()));
            }
        }
        Ok(())
    }

    /// Build the manifest for a file or folder on disk, hashing every file.
    /// Returns the manifest plus each file's location and full outboard.
    pub(crate) fn from_disk(root: &Path) -> Result<(Manifest, Vec<(PathBuf, crate::chunker::FileOutboard)>)> {
        let name = portable_name(root, root.file_name())?;

        let mut found = Vec::new();
        collect(root, vec![name.clone()], &mut found)?;
        let mut seen = std::collections::HashSet::new();
        for (components, path) in &found {
            if !seen.insert(components.iter().map(|c| c.to_lowercase()).collect::<Vec<_>>()) {
                return Err(Error::UnsupportedName {
                    path: path.clone(),
                    reason: "matches another file's name apart from upper/lower case".into(),
                });
            }
        }

        let mut files = Vec::new();
        let mut hashed = Vec::new();
        for (components, path) in found {
            let outboard = crate::chunker::hash_file(&path)?;
            files.push(FileEntry {
                path: components,
                size: outboard.tree.size(),
                root_hash: *outboard.root.as_bytes(),
            });
            hashed.push((path, outboard));
        }
        Ok((Manifest { name, files }, hashed))
    }
}

/// Recursively list regular files, in a stable order. Symlinks are skipped
/// so a share can never pull in files from outside the chosen folder. A name
/// that couldn't be recreated on every OS is an error rather than silently
/// left out, so the sender knows what the receiver won't get.
fn collect(path: &Path, components: Vec<String>, out: &mut Vec<(Vec<String>, PathBuf)>) -> Result<()> {
    let io_err = |source| Error::Io { path: path.to_path_buf(), source };
    let meta = std::fs::symlink_metadata(path).map_err(io_err)?;
    if meta.is_file() {
        out.push((components, path.to_path_buf()));
    } else if meta.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(path)
            .map_err(io_err)?
            .collect::<std::io::Result<_>>()
            .map_err(io_err)?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = portable_name(&entry.path(), Some(&entry.file_name()))?;
            let mut child = components.clone();
            child.push(name);
            collect(&entry.path(), child, out)?;
        }
    }
    Ok(())
}

fn portable_name(path: &Path, name: Option<&std::ffi::OsStr>) -> Result<String> {
    let unsupported = |reason: &str| Error::UnsupportedName { path: path.to_path_buf(), reason: reason.into() };
    let name = name.ok_or_else(|| unsupported("path has no file name"))?;
    let name = name.to_str().ok_or_else(|| unsupported("name isn't valid Unicode"))?;
    match name_problem(name) {
        Some(reason) => Err(unsupported(reason)),
        None => Ok(name.to_string()),
    }
}

/// Why `name` can't be used as a file name on every OS we support, if it can't.
fn name_problem(name: &str) -> Option<&'static str> {
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if name.is_empty() || name == "." || name == ".." {
        return Some("is not a file name");
    }
    if name.chars().any(|c| c < ' ' || r#"<>:"/\|?*"#.contains(c)) {
        return Some(r#"contains a character Windows doesn't allow (< > : " / \ | ? * or a control character)"#);
    }
    if name.ends_with(['.', ' ']) {
        return Some("ends with a dot or space, which Windows doesn't allow");
    }
    let stem = name.split('.').next().unwrap_or(name).trim_end();
    if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(stem)) {
        return Some("is a reserved device name on Windows");
    }
    None
}

/// Maps piece numbers (counted across all files, in manifest order) to
/// byte ranges within a particular file.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    /// First global piece of each file.
    first_piece: Vec<usize>,
    sizes: Vec<u64>,
    pub total_pieces: usize,
}

pub(crate) struct PieceLocation {
    pub file: usize,
    pub start: u64,
    pub end: u64,
}

impl Layout {
    pub fn new(manifest: &Manifest) -> Self {
        let mut first_piece = Vec::with_capacity(manifest.files.len());
        let mut total = 0;
        for file in &manifest.files {
            first_piece.push(total);
            total += file.pieces();
        }
        Layout { first_piece, sizes: manifest.files.iter().map(|f| f.size).collect(), total_pieces: total }
    }

    pub fn locate(&self, piece: usize) -> PieceLocation {
        // Last file whose first piece is <= `piece`, skipping empty files
        // (which share their first_piece with the next file).
        let file = self.first_piece.partition_point(|&first| first <= piece) - 1;
        let file = (0..=file).rev().find(|&f| self.sizes[f] > 0).unwrap_or(file);
        let block = BLOCK_SIZE.bytes() as u64;
        let start = (piece - self.first_piece[file]) as u64 * block;
        PieceLocation { file, start, end: (start + block).min(self.sizes[file]) }
    }

    pub fn pieces_of(&self, file: usize) -> std::ops::Range<usize> {
        let first = self.first_piece[file];
        first..first + self.sizes[file].div_ceil(BLOCK_SIZE.bytes() as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &[&str], size: u64) -> FileEntry {
        FileEntry { path: path.iter().map(|s| s.to_string()).collect(), size, root_hash: [0; 32] }
    }

    #[test]
    fn layout_maps_pieces_across_files_and_skips_empty_ones() {
        let block = BLOCK_SIZE.bytes() as u64;
        let m = Manifest {
            name: "d".into(),
            files: vec![entry(&["d", "a"], block + 5), entry(&["d", "empty"], 0), entry(&["d", "b"], 10)],
        };
        let layout = Layout::new(&m);
        assert_eq!(layout.total_pieces, 3);
        let p = layout.locate(1);
        assert_eq!((p.file, p.start, p.end), (0, block, block + 5));
        let p = layout.locate(2);
        assert_eq!((p.file, p.start, p.end), (2, 0, 10));
        assert_eq!(layout.pieces_of(1), 2..2);
    }

    #[test]
    fn manifests_that_escape_the_folder_are_rejected() {
        for path in [
            vec!["x", "..", "evil"],
            vec!["other", "f"],
            vec!["x", "C:"],
            vec!["x", "a/b"],
            vec!["x", "nul.txt"],
            vec!["x", "Com1"],
            vec!["x", "what?"],
            vec!["x", "trailing."],
            vec!["x", "tab\there"],
        ] {
            let m = Manifest { name: "x".into(), files: vec![entry(&path, 1)] };
            let bytes = m.encode();
            assert!(Manifest::decode_verified(&bytes, Manifest::id_of(&bytes)).is_err(), "{path:?}");
        }
        let ok = Manifest { name: "x".into(), files: vec![entry(&["x", "sub", "f.txt"], 1)] };
        let bytes = ok.encode();
        assert_eq!(Manifest::decode_verified(&bytes, Manifest::id_of(&bytes)).unwrap(), ok);
    }

    #[test]
    fn names_differing_only_in_case_are_rejected() {
        let m = Manifest { name: "x".into(), files: vec![entry(&["x", "a.txt"], 1), entry(&["x", "A.TXT"], 1)] };
        let bytes = m.encode();
        assert!(Manifest::decode_verified(&bytes, Manifest::id_of(&bytes)).is_err());
    }

    /// Windows can't even create these names, so this only runs elsewhere.
    #[cfg(unix)]
    #[test]
    fn seeding_names_other_systems_cant_store_is_an_error() {
        for bad in ["what?.txt", "aux.txt", "trailing."] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("share");
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join(bad), b"x").unwrap();
            assert!(matches!(Manifest::from_disk(&root), Err(Error::UnsupportedName { .. })), "{bad}");
        }
    }

    #[test]
    fn manifest_must_match_requested_id() {
        let m = Manifest { name: "x".into(), files: vec![entry(&["x"], 1)] };
        assert!(Manifest::decode_verified(&m.encode(), ShareId([9; 32])).is_err());
    }
}
