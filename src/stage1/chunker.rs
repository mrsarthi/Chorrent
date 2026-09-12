use crate::error::ChunkerError;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::CreateOutboard;
use bao_tree::{blake3, BlockSize};
use blake3::Hash;
use std::fs::File;
use std::path::Path;

pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
// This defines the math for tree geometry. 
// 4 here means 2^4 = 16 and these are each 1024 bytes
// They are grouped into a single tree node, creating 16kib blocks 

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
        .map_err(|source| ChunkerError::Hash {
            path: path.to_path_buf(),
            source,
        })?;

    Ok(HashResult { root_hash: outboard.root, outboard })
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
}