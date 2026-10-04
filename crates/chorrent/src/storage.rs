//! Where a share's bytes live: ordinary files, or an encrypted store.
//!
//! ## The encrypted format
//!
//! One `.enc` file per shared file, named by index so nothing about the
//! content shows on disk:
//!
//! ```text
//! header:  "CHRENC01" | salt (16 bytes)
//! slot i:  nonce (24) | ciphertext of piece i | tag (16)     (fixed size)
//! ```
//!
//! Each 64 KiB piece is sealed on its own with XChaCha20-Poly1305 under a
//! fresh random nonce, with the piece number as associated data (so slots
//! can't be swapped). The key is derived from the caller's master key and
//! the file's random salt, so every file has its own key. Pieces can arrive
//! in any order. A slot that was never written, or fails to decrypt, reads
//! as zeros: hash verification then treats it as missing, so tampering costs
//! a re-download, never bad data.

use crate::chunker::BLOCK_SIZE;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use positioned_io::{ReadAt, WriteAt};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: &[u8; 8] = b"CHRENC01";
const HEADER: u64 = 8 + 16;
const NONCE: usize = 24;
const TAG: usize = 16;

fn block() -> u64 {
    BLOCK_SIZE.bytes() as u64
}

fn slot() -> u64 {
    NONCE as u64 + block() + TAG as u64
}

/// The caller's key for the encrypted store (e.g. from the OS keychain).
#[derive(Clone)]
pub(crate) struct MasterKey(Arc<[u8; 32]>);

impl MasterKey {
    pub fn new(key: [u8; 32]) -> Self {
        Self(Arc::new(key))
    }

    fn file_key(&self, salt: &[u8; 16]) -> XChaCha20Poly1305 {
        let mut material = Vec::with_capacity(48);
        material.extend_from_slice(&self.0[..]);
        material.extend_from_slice(salt);
        let key = blake3::derive_key("chorrent encrypted store v1 file key", &material);
        XChaCha20Poly1305::new(&key.into())
    }
}

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterKey(..)")
    }
}

/// One file's bytes, wherever they live.
#[derive(Debug, Clone)]
pub(crate) enum FileData {
    Plain(PathBuf),
    Encrypted { path: PathBuf, key: MasterKey, size: u64 },
}

impl FileData {
    pub fn path(&self) -> &Path {
        match self {
            FileData::Plain(path) | FileData::Encrypted { path, .. } => path,
        }
    }

    pub fn is_encrypted(&self) -> bool {
        matches!(self, FileData::Encrypted { .. })
    }

    /// An existing file: encrypted if it carries our header, else plain.
    pub fn detect(path: PathBuf, key: Option<&MasterKey>, size: u64) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        let is_ours = File::open(&path).and_then(|mut f| f.read_exact(&mut magic)).is_ok() && &magic == MAGIC;
        match (is_ours, key) {
            (false, _) => Ok(FileData::Plain(path)),
            (true, Some(key)) => Ok(FileData::Encrypted { path, key: key.clone(), size }),
            (true, None) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "this file is in the encrypted store, but no storage key was given",
            )),
        }
    }

    /// Create the file (if needed) so pieces can be written anywhere in it.
    pub fn preallocate(&self) -> io::Result<()> {
        if let Some(parent) = self.path().parent() {
            std::fs::create_dir_all(parent)?;
        }
        match self {
            FileData::Plain(path) => {
                let file = OpenOptions::new().write(true).create(true).truncate(false).open(path)?;
                // Size is set by the caller for plain files (they know it).
                drop(file);
                Ok(())
            }
            FileData::Encrypted { path, size, .. } => {
                if std::fs::metadata(path).is_ok_and(|m| m.len() >= HEADER) {
                    return Ok(()); // already set up, maybe partly downloaded
                }
                let mut header = Vec::with_capacity(HEADER as usize);
                header.extend_from_slice(MAGIC);
                header.extend_from_slice(&rand::random::<[u8; 16]>());
                let file = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
                (&file).write_all(&header)?;
                file.set_len(HEADER + size.div_ceil(block()) * slot())
            }
        }
    }

    /// Open for piece-level access. Encrypted pieces that are missing or fail
    /// to decrypt read as zeros, which hash checks then treat as missing.
    pub fn open(&self, write: bool) -> io::Result<Handle> {
        self.open_with(write, false)
    }

    /// Open for reading finished data: an encrypted piece that is missing or
    /// fails to decrypt is an error rather than zeros.
    pub fn open_strict(&self) -> io::Result<Handle> {
        self.open_with(false, true)
    }

    fn open_with(&self, write: bool, strict: bool) -> io::Result<Handle> {
        match self {
            FileData::Plain(path) => {
                // Write-only when writing: on Windows, opening a big file for
                // read as well made every piece's open ~10x slower.
                let file = OpenOptions::new().read(!write).write(write).open(path)?;
                Ok(Handle::Plain(file))
            }
            FileData::Encrypted { path, key, size } => {
                let file = OpenOptions::new().read(true).write(write).open(path)?;
                let mut header = [0u8; HEADER as usize];
                file.read_exact_at(0, &mut header)?;
                if &header[..8] != MAGIC {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "not an encrypted store file"));
                }
                let salt: [u8; 16] = header[8..].try_into().unwrap();
                Ok(Handle::Encrypted(Encrypted { file, cipher: key.file_key(&salt), size: *size, strict }))
            }
        }
    }

    /// Read every byte in order (for verifying or exporting); strict.
    /// Buffered in whole pieces: each small read would otherwise cost a disk
    /// read, and for encrypted files a whole piece's decryption.
    pub fn reader(&self) -> io::Result<io::BufReader<SequentialReader>> {
        let inner = SequentialReader { handle: self.open_strict()?, pos: 0 };
        Ok(io::BufReader::with_capacity(16 * block() as usize, inner))
    }
}

/// An open file, plain or encrypted, with positioned reads and writes.
pub(crate) enum Handle {
    Plain(File),
    Encrypted(Encrypted),
}

pub(crate) struct Encrypted {
    file: File,
    cipher: XChaCha20Poly1305,
    size: u64,
    strict: bool,
}

impl Encrypted {
    fn block_len(&self, index: u64) -> usize {
        (self.size - index * block()).min(block()) as usize
    }

    /// The plaintext of one piece. If it's absent or not authentic: zeros,
    /// or an error in strict mode.
    fn read_block(&self, index: u64) -> io::Result<Vec<u8>> {
        let len = self.block_len(index);
        let unreadable = |what: &str| {
            if self.strict {
                Err(io::Error::new(io::ErrorKind::InvalidData, format!("encrypted piece {index} {what}")))
            } else {
                Ok(vec![0; len])
            }
        };
        let mut sealed = vec![0u8; NONCE + len + TAG];
        let at = HEADER + index * slot();
        if self.file.read_exact_at(at, &mut sealed).is_err() {
            return unreadable("is missing");
        }
        let (nonce, ciphertext) = sealed.split_at(NONCE);
        if nonce.iter().all(|&b| b == 0) {
            return unreadable("was never written");
        }
        let nonce = XNonce::try_from(nonce).expect("24 bytes");
        let aad = index.to_le_bytes();
        match self.cipher.decrypt(&nonce, Payload { msg: ciphertext, aad: &aad }) {
            Ok(plain) => Ok(plain),
            Err(_) => unreadable("failed to decrypt (wrong key, or damaged)"),
        }
    }

    fn write_block(&mut self, index: u64, plain: &[u8]) -> io::Result<()> {
        let nonce: [u8; NONCE] = rand::random();
        let aad = index.to_le_bytes();
        let ciphertext = self
            .cipher
            .encrypt(&XNonce::from(nonce), Payload { msg: plain, aad: &aad })
            .map_err(|_| io::Error::other("encryption failed"))?;
        let mut sealed = Vec::with_capacity(NONCE + ciphertext.len());
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        self.file.write_all_at(HEADER + index * slot(), &sealed)
    }
}

impl ReadAt for Handle {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Handle::Plain(file) => file.read_at(pos, buf),
            Handle::Encrypted(enc) => {
                if pos >= enc.size || buf.is_empty() {
                    return Ok(0);
                }
                let index = pos / block();
                let within = (pos % block()) as usize;
                let plain = enc.read_block(index)?;
                let n = buf.len().min(plain.len() - within);
                buf[..n].copy_from_slice(&plain[within..within + n]);
                Ok(n)
            }
        }
    }
}

impl WriteAt for Handle {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> io::Result<usize> {
        match self {
            Handle::Plain(file) => file.write_at(pos, buf),
            Handle::Encrypted(enc) => {
                if pos >= enc.size || buf.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "write past the end of the file"));
                }
                let index = pos / block();
                let within = (pos % block()) as usize;
                let len = enc.block_len(index);
                let n = buf.len().min(len - within);
                if within == 0 && n == len {
                    enc.write_block(index, &buf[..n])?;
                } else {
                    // Partial piece: read, patch, re-seal. Downloads always
                    // write whole pieces, so this is the rare path.
                    let mut plain = enc.read_block(index)?;
                    plain[within..within + n].copy_from_slice(&buf[..n]);
                    enc.write_block(index, &plain)?;
                }
                Ok(n)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Handle::Plain(file) => WriteAt::flush(file),
            Handle::Encrypted(enc) => WriteAt::flush(&mut enc.file),
        }
    }
}

pub(crate) struct SequentialReader {
    handle: Handle,
    pos: u64,
}

impl Read for SequentialReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.handle.read_at(self.pos, buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

/// Copies a source into the store in whole pieces while passing every byte
/// through to whoever reads it (the hasher), so a file is read only once.
pub(crate) struct ImportingReader<R> {
    source: R,
    dest: Handle,
    pending: Vec<u8>,
    written: u64,
}

impl<R: Read> ImportingReader<R> {
    pub fn new(source: R, dest: &FileData) -> io::Result<Self> {
        dest.preallocate()?;
        Ok(Self { source, dest: dest.open(true)?, pending: Vec::new(), written: 0 })
    }

    /// Write out the last partial piece. Call once the hasher has read everything.
    pub fn finish(mut self) -> io::Result<u64> {
        if !self.pending.is_empty() {
            self.dest.write_all_at(self.written, &self.pending)?;
            self.written += self.pending.len() as u64;
        }
        Ok(self.written)
    }
}

impl<R: Read> Read for ImportingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.source.read(buf)?;
        self.pending.extend_from_slice(&buf[..n]);
        let whole = self.pending.len() / block() as usize * block() as usize;
        if whole > 0 {
            let chunk: Vec<u8> = self.pending.drain(..whole).collect();
            self.dest.write_all_at(self.written, &chunk)?;
            self.written += whole as u64;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    fn encrypted(dir: &Path, size: u64) -> FileData {
        FileData::Encrypted { path: dir.join("0.enc"), key: MasterKey::new([7; 32]), size }
    }

    #[test]
    fn round_trips_out_of_order_and_hides_plaintext() {
        let dir = tempfile::tempdir().unwrap();
        let data = content(2 * block() as usize + 500);
        let file = encrypted(dir.path(), data.len() as u64);
        file.preallocate().unwrap();
        let mut h = file.open(true).unwrap();
        // Last piece first, then the others.
        let b = block() as usize;
        h.write_all_at(2 * b as u64, &data[2 * b..]).unwrap();
        h.write_all_at(0, &data[..b]).unwrap();
        h.write_all_at(b as u64, &data[b..2 * b]).unwrap();

        let mut back = Vec::new();
        file.reader().unwrap().read_to_end(&mut back).unwrap();
        assert_eq!(back, data);

        let raw = std::fs::read(file.path()).unwrap();
        assert!(!raw.windows(64).any(|w| data.windows(64).next() == Some(w)), "plaintext leaked");
    }

    #[test]
    fn missing_tampered_or_wrong_key_reads_as_zeros() {
        let dir = tempfile::tempdir().unwrap();
        let data = content(block() as usize * 2);
        let file = encrypted(dir.path(), data.len() as u64);
        file.preallocate().unwrap();
        file.open(true).unwrap().write_all_at(0, &data[..block() as usize]).unwrap();

        let mut buf = vec![1u8; 100];
        let h = file.open(false).unwrap();
        h.read_exact_at(block(), &mut buf).unwrap(); // never written
        assert!(buf.iter().all(|&b| b == 0));

        let wrong = FileData::Encrypted { path: file.path().to_path_buf(), key: MasterKey::new([8; 32]), size: data.len() as u64 };
        wrong.open(false).unwrap().read_exact_at(0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
        // Reading finished data must not pass zeros off as content.
        assert!(wrong.open_strict().unwrap().read_exact_at(0, &mut buf).is_err());
        assert!(file.open_strict().unwrap().read_exact_at(block(), &mut buf).is_err());

        let mut raw = std::fs::read(file.path()).unwrap();
        raw[HEADER as usize + NONCE + 10] ^= 1;
        std::fs::write(file.path(), &raw).unwrap();
        file.open(false).unwrap().read_exact_at(0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn importing_copies_while_passing_bytes_through() {
        let dir = tempfile::tempdir().unwrap();
        let data = content(3 * block() as usize + 77);
        let file = encrypted(dir.path(), data.len() as u64);
        let mut importer = ImportingReader::new(std::io::Cursor::new(data.clone()), &file).unwrap();
        let mut seen = Vec::new();
        importer.read_to_end(&mut seen).unwrap();
        assert_eq!(importer.finish().unwrap(), data.len() as u64);
        assert_eq!(seen, data);
        let mut back = Vec::new();
        file.reader().unwrap().read_to_end(&mut back).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn plain_files_are_detected_as_plain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        std::fs::write(&path, b"hello").unwrap();
        assert!(!FileData::detect(path, None, 5).unwrap().is_encrypted());
    }
}
