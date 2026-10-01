# Chorrent (vers. 0.2.0)

Chorrent is a Rust-based p2p file transfer engine, built on Iroh and QUIC streams to move files directly between peers.
No central server required.

> **Now supports swarm as well**

## What makes it different

- **Verified, chunked transfer** — files are split into 64 KiB pieces and hashed with a BLAKE3 Merkle tree (via `bao-tree`). Every piece is independently verified against the file's root hash as it arrives, so corrupted or tampered data is caught and rejected immediately, not just at the end.
- **Real NAT traversal** — connections use Iroh's QUIC-based hole punching with automatic relay fallback, so peers behind restrictive routers (including CGNAT) can still connect reliably.
- **Decentralized discovery** — peers find each other through Iroh Gossip, an epidemic broadcast protocol, rather than a central tracker or server.
- **Swarm downloading** — a custom piece scheduler pulls different chunks from multiple peers concurrently, favoring scarcer pieces to spread availability across the swarm faster than a simple sequential download.

## Usage

Install the CLI:
```bash
cargo install --path crates/chorrent-cli
```

Share a file:
```bash
chorrent seed <file_path>
```
Download a file using a share code from a seeder (share this):
```bash
chorrent get <share_code>
```

Seed the same file from a second machine, joining the first seeder's swarm:
```bash
chorrent seed <file_path> --join <share_code>
```

## As a library

The `chorrent` crate (`crates/chorrent`) is the engine; the CLI is a thin layer over it.

```rust
let client = chorrent::Client::new().await?;

// Seed
let seed = client.seed("movie.mp4").await?;
println!("share this: {}", seed.share_code());

// Download, watching progress events
let code: chorrent::ShareCode = share_text.parse()?;
let download = client.download(&code, None).await?;
let mut events = download.events();
let path = download.finished().await?; // verified against the root hash
```

## Status

Core pipeline (hashing, connectivity, transfer, discovery, swarming) is built and tested across real devices on separate networks. Actively in development — reseeding and resumable downloads are next on the roadmap.