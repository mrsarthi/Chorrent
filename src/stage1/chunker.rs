use crate::error::ChunkerError;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::io::sync::{decode_ranges, encode_ranges_validated, CreateOutboard};
use bao_tree::{blake3, BaoTree, BlockSize, ByteRanges};
use blake3::Hash;
use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::path::Path;

pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(6); // 64 KiB

pub struct HashResult {
    pub root_hash: Hash,
    pub outboard: PreOrderOutboard<Vec<u8>>,
}

pub fn hash_file(path: &Path) -> Result<HashResult, ChunkerError> {
    let file = File::open(path).map_err(|source| ChunkerError::Open {
        path: path.to_path_buf(),
        source,
    })?;

    let outboard = PreOrderOutboard::<Vec<u8>>::create(file, BLOCK_SIZE)
        .map_err(|source| ChunkerError::Hash { path: path.to_path_buf(), source })?;

    Ok(HashResult { root_hash: outboard.root, outboard })
}

/// Cut out one verified piece of the file, ready to hand to a peer.
pub fn serve_range(
    path: &Path,
    outboard: &PreOrderOutboard<Vec<u8>>,
    start_byte: u64,
    end_byte: u64,
) -> Result<Vec<u8>, ChunkerError> {
    let file = File::open(path).map_err(|source| ChunkerError::Open {
        path: path.to_path_buf(),
        source,
    })?;

    let ranges = ByteRanges::from(start_byte..end_byte);
    let ranges = round_up_to_chunks(&ranges); // align to real tree boundaries

    let mut encoded = Vec::new();
    encode_ranges_validated(&file, outboard, &ranges, &mut encoded)
        .map_err(|source| ChunkerError::Serve { path: path.to_path_buf(), source: source.into() })?;

    Ok(encoded)
}

/// Check an incoming piece against the root hash, and save it if genuine.
pub fn receive_range(
    dest_path: &Path,
    root_hash: Hash,
    total_size: u64,
    start_byte: u64,
    end_byte: u64,
    encoded: &[u8],
) -> Result<(), ChunkerError> {
    let mut dest = OpenOptions::new()
        .write(true)
        .create(true)
        .open(dest_path)
        .map_err(|source| ChunkerError::Receive { path: dest_path.to_path_buf(), source })?;

    // Set the file to its final size right away, so writing into the
    // middle of it (before earlier pieces arrive) is always valid.
    dest.set_len(total_size)
        .map_err(|source| ChunkerError::Receive { path: dest_path.to_path_buf(), source })?;

    let ranges = ByteRanges::from(start_byte..end_byte);
    let ranges = round_up_to_chunks(&ranges);

    let tree = BaoTree::new(total_size, BLOCK_SIZE);
    let mut outboard = PreOrderOutboard { root: root_hash, tree, data: Vec::new() };

    decode_ranges(Cursor::new(encoded), &ranges, &mut dest, &mut outboard)
        .map_err(|source| ChunkerError::Receive { path: dest_path.to_path_buf(), source: source.into() })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn same_content_gives_same_root_hash() {
        let mut tmp1 = tempfile::NamedTempFile::new().unwrap();
        tmp1.write_all(b"hello world").unwrap();
        let mut tmp2 = tempfile::NamedTempFile::new().unwrap();
        tmp2.write_all(b"hello world").unwrap();

        assert_eq!(
            hash_file(tmp1.path()).unwrap().root_hash,
            hash_file(tmp2.path()).unwrap().root_hash
        );
    }

    #[test]
    fn serve_and_receive_a_range_round_trips() {
        // Make a source file with known, predictable content.
        let mut source = tempfile::NamedTempFile::new().unwrap();
        let content: Vec<u8> = (0..200_000u32).map(|i| (i % 256) as u8).collect();
        source.write_all(&content).unwrap();

        // Hash it, same as before.
        let hashed = hash_file(source.path()).unwrap();

        // "Serve" bytes 1000..66536 (a bit over 64 KiB, to test rounding too).
        let encoded = serve_range(source.path(), &hashed.outboard, 1000, 66_536).unwrap();

        // "Receive" that piece into a brand new destination file.
        let dest = tempfile::NamedTempFile::new().unwrap();
        receive_range(
            dest.path(),
            hashed.root_hash,
            content.len() as u64,
            1000,
            66_536,
            &encoded,
        )
        .unwrap();

        // The bytes that arrived should exactly match the original,
        // in the same position in the file.
        let written = std::fs::read(dest.path()).unwrap();
        assert_eq!(&written[1000..66_536], &content[1000..66_536]);
    }
}