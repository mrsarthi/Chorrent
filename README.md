# Chorrent (vers. 0.3.0)

Chorrent is a Rust-based p2p file transfer engine, built on Iroh and QUIC streams to move files and folders directly between peers.
No central server required. Use it as a command-line tool, or embed it as a library in your own app.

## What makes it different

- **Verified, chunked transfer** — files are split into 64 KiB pieces and hashed with a BLAKE3 Merkle tree (via `bao-tree`). Every piece is independently verified against the file's root hash as it arrives, so corrupted or tampered data is caught and rejected immediately, not just at the end.
- **Real NAT traversal** — connections use Iroh's QUIC-based hole punching with automatic relay fallback, so peers behind restrictive routers (including CGNAT) can still connect reliably.
- **Decentralized discovery** — peers find each other through Iroh Gossip, an epidemic broadcast protocol, rather than a central tracker or server.
- **Swarm downloading** — a piece scheduler pulls different pieces from every peer at once, rarest first, and requests the last few pieces from several peers in parallel so one slow peer can't stall the finish. Peers can join and leave mid-download.
- **Reseeding** — downloaders serve the pieces they already have to each other, during and after the download, so the swarm keeps going after the original seeder leaves.
- **Files and folders** — share a single file or a whole folder tree.
- **Private shares** — optionally, only people holding the share code can find the swarm or fetch anything from it.
- **Resumable** — progress is saved; interrupted downloads continue where they stopped, and pieces already on disk are re-verified rather than trusted.

## Command line

Install:
```bash
cargo install --path crates/chorrent-cli
```

Share a file or folder (prints a share code starting with `chr2`):
```bash
chorrent seed <path>
chorrent seed <path> --private          # only people with the code can find it
chorrent seed <path> --join <code>      # second seeder joining an existing swarm
```

Download:
```bash
chorrent get <code>                     # into the current folder
chorrent get <code> <folder>
chorrent get <code> --seed              # keep seeding after it finishes
chorrent get <code> --no-reseed         # don't upload to others while downloading
```

Saved state:
```bash
chorrent list                           # saved seeds and unfinished downloads
chorrent resume                         # pick them all back up
chorrent forget <share-id>
```

Options for any command: `--upload-limit 1M`, `--download-limit 500K`, `--max-uploads 32`,
`--data-dir <folder>` (default: your OS's app data folder), `--no-state`, and `--dht` (also find
peers' addresses through the BitTorrent Mainline DHT, so old share codes keep working after a
peer's address changes; only relay addresses are published, never IPs).

## As a library

The `chorrent` crate (`crates/chorrent`) is the engine; the CLI is a thin layer over it.

```rust
let client = chorrent::Client::builder()
    .data_dir("state")            // optional: stable identity + resume
    .upload_limit(Some(1 << 20))  // optional: 1 MiB/s
    .build()
    .await?;

// Seed a file or folder
let seed = client.seed("holiday-photos/").await?;
println!("share this: {}", seed.share_code());

// Download, watching progress events
let code: chorrent::ShareCode = share_text.parse()?;
let download = client.download(&code, None).await?;
let mut events = download.events();      // PeerConnected, PieceVerified, ...
let done = download.finished().await?;   // every file verified
println!("saved to {}", done.path.display());
```

`chorrent::Event` serializes with serde, so a UI in another process can consume it as JSON.
Enable the `mainline` cargo feature for `ClientBuilder::mainline_dht`.
See [`crates/chorrent/examples/share.rs`](crates/chorrent/examples/share.rs) for a complete program.

## Development

```bash
cargo test --workspace     # unit tests + end-to-end tests (these need internet: they use Iroh relays)
cargo run --release -p chorrent --example bench -- 500    # local throughput, size in MiB
```

## Status

Hashing, connectivity, transfer, discovery, swarming, reseeding, folders, private shares, resume and rate limiting are built and tested.
