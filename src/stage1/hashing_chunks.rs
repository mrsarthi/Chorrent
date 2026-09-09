use anyhow::{Context, Result};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::CreateOutboard;
use bao_tree::{BlockSize, blake3};
use blake3::Hash;
use std::fs::File;
use std::path::Path;

pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

pub struct HashResult {
    pub root_hash: Hash,
    pub outboard: PreOrderOutboard<Vec<u8>>,
}

pub fn hash_file(path: &Path) -> Result<HashResult> {
    let file = File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let outboard = PreOrderOutboard::<Vec<u8>>::create(file, BLOCK_SIZE)
        .with_context(|| format!("failed to hash {}", path.display()))?;

    let root_hash = outboard.root;

    Ok(HashResult { root_hash, outboard })
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

        let r1 = hash_file(tmp1.path()).unwrap();
        let r2 = hash_file(tmp2.path()).unwrap();

        assert_eq!(r1.root_hash, r2.root_hash);
    }
}