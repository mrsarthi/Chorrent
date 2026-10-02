//! Hashing, encoding and verifying pieces of a single file with BLAKE3/Bao.
//!
//! Every file gets a pre-order outboard: the Merkle tree's inner hashes,
//! stored separately from the data. A seeder computes it in full; a
//! downloader starts with an all-zero one and fills in the hashes along each
//! piece's proof path as verified pieces arrive, which is exactly what it
//! needs to serve those pieces onward.

use crate::error::ChunkerError;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::io::sync::{
    decode_ranges, encode_ranges_validated, valid_ranges, CreateOutboard, Outboard, OutboardMut,
};
use bao_tree::{blake3, BaoTree, BlockSize, ByteRanges, ChunkNum, TreeNode};
use blake3::Hash;
use std::fs::{File, OpenOptions};
use std::io::{self, Cursor};
use std::path::Path;

pub(crate) const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(6); // 64 KiB

pub(crate) type FileOutboard = PreOrderOutboard<Vec<u8>>;

/// Proof hashes learned while verifying one piece, to merge into the file's outboard.
pub(crate) type ProofNodes = Vec<(TreeNode, (Hash, Hash))>;

/// Hash a whole file, producing its complete outboard.
pub(crate) fn hash_file(path: &Path) -> Result<FileOutboard, ChunkerError> {
    let file = File::open(path).map_err(|source| ChunkerError::Open { path: path.to_path_buf(), source })?;
    let mut outboard = FileOutboard::create(file, BLOCK_SIZE)
        .map_err(|source| ChunkerError::Hash { path: path.to_path_buf(), source })?;
    let full = outboard.tree.outboard_size() as usize;
    outboard.data.resize(full, 0);
    Ok(outboard)
}

/// An outboard with no inner hashes known yet, for a file we're about to download.
pub(crate) fn empty_outboard(root: Hash, size: u64) -> FileOutboard {
    let tree = BaoTree::new(size, BLOCK_SIZE);
    PreOrderOutboard { root, tree, data: vec![0; tree.outboard_size() as usize] }
}

/// Cut out bytes `start..end` of a file plus the proof needed to verify them.
pub(crate) fn encode_range(
    path: &Path,
    outboard: &FileOutboard,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, ChunkerError> {
    let file = File::open(path).map_err(|source| ChunkerError::Open { path: path.to_path_buf(), source })?;
    let ranges = round_up_to_chunks(&ByteRanges::from(start..end));
    let mut encoded = Vec::new();
    encode_ranges_validated(&file, outboard, &ranges, &mut encoded)
        .map_err(|source| ChunkerError::Serve { path: path.to_path_buf(), source: source.into() })?;
    Ok(encoded)
}

/// Verify an encoded range against the file's root hash and write the data
/// into place. Returns the proof hashes that were verified on the way.
pub(crate) fn decode_range(
    path: &Path,
    root: Hash,
    size: u64,
    start: u64,
    end: u64,
    encoded: &[u8],
) -> Result<ProofNodes, ChunkerError> {
    let mut dest = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| ChunkerError::Receive { path: path.to_path_buf(), source })?;
    let ranges = round_up_to_chunks(&ByteRanges::from(start..end));
    let mut recorder = RecordingOutboard { root, tree: BaoTree::new(size, BLOCK_SIZE), nodes: Vec::new() };
    decode_ranges(Cursor::new(encoded), &ranges, &mut dest, &mut recorder)
        .map_err(|source| ChunkerError::Receive { path: path.to_path_buf(), source: source.into() })?;
    Ok(recorder.nodes)
}

pub(crate) fn apply_proof(outboard: &mut FileOutboard, nodes: &ProofNodes) -> io::Result<()> {
    for (node, pair) in nodes {
        outboard.save(*node, pair)?;
    }
    Ok(())
}

/// Which bytes of a (possibly partial) file are present and verified by the
/// outboard. Used to resume downloads without trusting any saved bitfield.
pub(crate) fn valid_byte_ranges(path: &Path, outboard: &FileOutboard) -> io::Result<Vec<std::ops::Range<u64>>> {
    let file = File::open(path)?;
    let all = bao_tree::ChunkRanges::from(ChunkNum(0)..);
    let mut out = Vec::new();
    for range in valid_ranges(outboard, &file, &all) {
        let range = range?;
        out.push(range.start.to_bytes()..range.end.to_bytes());
    }
    Ok(out)
}

/// Create (or keep) a file at its final length, so pieces can be written
/// anywhere in it in any order.
pub(crate) fn preallocate(path: &Path, size: u64) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().write(true).create(true).truncate(false).open(path)?;
    file.set_len(size)
}

/// Outboard that only remembers what the decoder verified, so decoding needs
/// no lock on the shared outboard.
struct RecordingOutboard {
    root: Hash,
    tree: BaoTree,
    nodes: ProofNodes,
}

impl Outboard for RecordingOutboard {
    fn root(&self) -> Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, _node: TreeNode) -> io::Result<Option<(Hash, Hash)>> {
        Ok(None)
    }
}

impl OutboardMut for RecordingOutboard {
    fn save(&mut self, node: TreeNode, pair: &(Hash, Hash)) -> io::Result<()> {
        self.nodes.push((node, *pair));
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn source(len: usize) -> (tempfile::NamedTempFile, Vec<u8>) {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let content: Vec<u8> = (0..len as u32).map(|i| (i % 251) as u8).collect();
        file.write_all(&content).unwrap();
        (file, content)
    }

    #[test]
    fn same_content_gives_same_root_hash() {
        let (a, _) = source(5000);
        let (b, _) = source(5000);
        assert_eq!(hash_file(a.path()).unwrap().root, hash_file(b.path()).unwrap().root);
    }

    #[test]
    fn downloaded_pieces_can_be_served_onward() {
        let piece = BLOCK_SIZE.bytes() as u64;
        let (src, content) = source(3 * piece as usize + 100);
        let size = content.len() as u64;
        let full = hash_file(src.path()).unwrap();

        // Peer B receives only piece 1 from the seeder...
        let dir = tempfile::tempdir().unwrap();
        let b_path = dir.path().join("b");
        preallocate(&b_path, size).unwrap();
        let mut b_outboard = empty_outboard(full.root, size);
        let encoded = encode_range(src.path(), &full, piece, 2 * piece).unwrap();
        let nodes = decode_range(&b_path, full.root, size, piece, 2 * piece, &encoded).unwrap();
        apply_proof(&mut b_outboard, &nodes).unwrap();

        // ...and can serve it to peer C, who verifies it against the same root.
        let c_path = dir.path().join("c");
        preallocate(&c_path, size).unwrap();
        let onward = encode_range(&b_path, &b_outboard, piece, 2 * piece).unwrap();
        decode_range(&c_path, full.root, size, piece, 2 * piece, &onward).unwrap();
        let c = std::fs::read(&c_path).unwrap();
        assert_eq!(&c[piece as usize..2 * piece as usize], &content[piece as usize..2 * piece as usize]);

        // B's file only verifies where it actually has data.
        assert_eq!(valid_byte_ranges(&b_path, &b_outboard).unwrap(), vec![piece..2 * piece]);
    }

    #[test]
    fn tampered_piece_is_rejected() {
        let (src, content) = source(2 * BLOCK_SIZE.bytes() + 10);
        let size = content.len() as u64;
        let full = hash_file(src.path()).unwrap();
        let mut encoded = encode_range(src.path(), &full, 0, 1000).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("d");
        preallocate(&dest, size).unwrap();
        assert!(decode_range(&dest, full.root, size, 0, 1000, &encoded).is_err());
    }
}
