# chorrent

Send files and folders directly between devices, from Rust. No server in between and no size
limit. If several devices have the same file, a download pulls different parts from all of
them at once, and every downloader helps pass it on.

Built on [iroh](https://iroh.computer) (encrypted QUIC connections that get through home
routers, with a relay server as a fallback).

- **Every piece is checked** (BLAKE3) as it arrives. A damaged or tampered piece is rejected
  and fetched again, so a finished download is exactly what was shared.
- **Files and folders**, any size.
- **Swarms:** download from many sharers at once. Sharers find each other automatically.
- **Resumes** interrupted downloads, and remembers shares across restarts.
- **Private shares** and **contact allowlists**: only the people you choose can download.
- **Encrypted storage** for apps that must not leave anything readable on disk: files,
  file names, share codes and who you shared with are all encrypted.
- **Runs on your app's own iroh endpoint**, if you already have one.
- Works on Windows, macOS, Linux and Android.

## Quick start

```rust
use chorrent::{Client, Event};

#[tokio::main]
async fn main() -> chorrent::Result<()> {
    let client = Client::new().await?;

    // Share a file or folder, and hand the code to someone.
    let seed = client.seed("holiday-photos/").await?;
    println!("share this: {}", seed.share_code());

    // On the other device: download it, watching progress.
    let code: chorrent::ShareCode = "chr2...".parse()?;
    let download = client.download(&code, None).await?;
    let mut events = download.events();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            if let Event::PieceVerified { bytes, .. } = event {
                println!("+{bytes} bytes");
            }
        }
    });
    let done = download.finished().await?;
    println!("saved to {:?}", done.path);
    Ok(())
}
```

A share code is a short piece of text starting with `chr2`. Send it any way you like.
Whoever has it can download the share (add allowlists if that's not enough).

## The main pieces

| | |
|---|---|
| `Client::builder()` | Settings: a folder to remember things in (`data_dir`), speed limits, encrypted storage, an existing iroh endpoint, how long to wait for the relay at startup |
| `client.seed(path)` / `seed_with(path, options)` | Share a file or folder. Options: `private`, `allow_peers`, `join` another sharer's swarm |
| `client.seed_reader(name, size, reader, options)` | Share a stream instead of a path, e.g. an Android content URI |
| `client.download(&code, dest)` / `download_with` | Download. Returns a handle with `events()`, `cancel()` and `finished()` |
| `client.saved()` / `resume()` / `forget()` / `remove()` | Pick up shares and unfinished downloads after a restart |
| `client.read_range()` / `export_file()` | Read or save files kept in encrypted storage |
| `client.network_status()` / `check_peers(&code)` | Find out why something won't connect |

`Event` covers peers connecting and leaving, pieces arriving, swarm status and more, and can
be turned into JSON (serde) for a UI in another process.

## Using your app's own iroh endpoint

If your app already runs iroh, give chorrent the same endpoint, so transfers use the same
identity, relay server and connections. Add `chorrent::ALPN` to the endpoint's ALPNs and pass
matching incoming connections to `client.handle_connection(conn)`. In your accept loop,
finish each handshake in its own task: awaiting it inline lets one slow peer hold up every
other connection.

```rust
let client = Arc::new(chorrent::Client::builder()
    .endpoint(endpoint.clone())
    .data_dir(app_data.join("chorrent"))
    .encrypted_storage(key_from_keychain)   // received files stay encrypted on disk
    .build().await?);
```

In this mode chorrent never waits for the relay: your app manages the connection. Check
`client.network_status()` before sending, so you can warn the user if it isn't connected.

The full example is in the
[repository README](https://github.com/mrsarthi/Chorrent#inside-an-app-that-already-uses-iroh).

## Features

- `mainline`: also find devices through the BitTorrent Mainline DHT
  (`ClientBuilder::mainline_dht`). Only relay addresses are published, never IP addresses.

## Requirements

Rust 1.88 or newer, a tokio runtime, and iroh 1.0.3 or newer (if you bring your own endpoint).

## License

Apache-2.0
