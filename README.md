# Chorrent (vers. 0.5.1)

Chorrent sends files and folders straight from one computer to another. There's no upload
to a website or cloud in between, and no size limit. You share a file, Chorrent gives you a
**share code**, you send that code to someone however you like (chat, email, text), and
they use it to download the file directly from you.

If several people have the same file, a downloader pulls different parts from all of them
at once, and everyone who downloads helps pass the file on. So it gets faster as more people
have it, and keeps working after the original sender goes offline.

Chorrent is two things:
- a **command-line program** (`chorrent`), which this guide is mostly about, and
- a **Rust library** that other apps can build on (see [For developers](#for-developers)).

---

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Commands](#commands)
  - [seed: share a file or folder](#seed-share-a-file-or-folder)
  - [get: download](#get-download)
  - [doctor: check your connection](#doctor-check-your-connection)
  - [list, resume, forget: saved transfers](#list-resume-forget-saved-transfers)
  - [Options that work with every command](#options-that-work-with-every-command)
- [Common situations](#common-situations)
- [Good to know](#good-to-know)
- [For developers](#for-developers)

---

## Install

**If you have Rust installed**, build and install it from this folder:
```bash
cargo install --path crates/chorrent-cli
```
After that, `chorrent` works from any folder in any terminal.

**If someone gave you `chorrent.exe`**, put it in a folder and open a terminal there:
- In **Command Prompt**, type `chorrent ...`
- In **PowerShell**, type `.\chorrent ...`
- In **Git Bash**, type `./chorrent ...`

Check it works:
```bash
chorrent --version
```
Everyone sharing with each other should use the same version.

---

## Quick start

**To send a file:**
```bash
chorrent seed "C:\Videos\holiday.mp4"
```
Chorrent reads the file (this takes a moment for big files) and prints a long code
starting with `chr2`. Send that code to the other person. **Keep the window open**: the
file is only available while it's running. Press **Ctrl+C** to stop sharing.

**To receive it:**
```bash
chorrent get chr2xxxxxxxx...
```
The file downloads into the folder you're in. When it says `done, verified`, it's
complete and checked.

That's all you need for the basics. The rest of this guide covers the other options.

---

## Commands

### seed: share a file or folder

```bash
chorrent seed <file or folder>
```
Use this when you want to **give** something to someone. It prints a share code and keeps
running until you press Ctrl+C. While it runs, it shows how much has been uploaded.

You can share a whole folder too. The other person gets the same folder with everything
inside it, in the same layout.

**Options:**

| Option | What it does | When to use it |
|---|---|---|
| `--private` | Only people who have the code can find or download the file | Anything personal. Without it, anyone who learns the file's ID could find it |
| `--join <code>` | Share the same file alongside someone who's already sharing it | When you also have the file and want to help send it, so downloads are faster |

**Examples:**
```bash
chorrent seed "D:\Photos\Wedding"                 # share a whole folder
chorrent seed report.pdf --private                # only people with the code can get it
chorrent seed movie.mp4 --join chr2xxxx...        # help someone else share movie.mp4
```

**Things to know:**
- For `--join`, you need the **exact same file**, including the **same file name**.
  If it's different in any way, Chorrent tells you the code is for different content.
- If you see a **WARNING about the relay server**, people on other networks may not be able
  to reach you yet. Chorrent keeps trying. When it connects, it prints an **updated code**.
  Send that one instead. See [It won't connect](#it-wont-connect).
- When someone joins with `--join`, their code includes everyone sharing the file, so any
  sharer's code works for downloading.

---

### get: download

```bash
chorrent get <code> [folder]
```
Use this when someone sent you a share code. It downloads into the folder you name, or the
current folder if you don't name one. A progress bar shows speed and time left.

**While it runs you'll see lines like:**
- `Peer 3a7f9c21b0 connected (6856 of 6856 pieces)`: you've connected to someone who has
  the file. With several sharers you'll see several of these.
- `Peer 3a7f9c21b0: direct connection`: the fastest kind of connection.
- `Peer 3a7f9c21b0: via relay (slower...)`: your connection goes through a helper server
  because a direct one wasn't possible. It works, just slower.
- `Swarm: linked to 2 peer(s)`: you're linked with others sharing the file, so you'll
  find new sharers as they appear.
- `done, verified`: finished, and every part of the file was checked.

**Options:**

| Option | What it does | When to use it |
|---|---|---|
| `--seed` | Keeps sharing the file after it's downloaded, until Ctrl+C | When you want to help others get it too |
| `--no-reseed` | Doesn't share anything while downloading | On a slow or metered connection where you don't want any upload |

**Examples:**
```bash
chorrent get chr2xxxx...                       # download into the current folder
chorrent get chr2xxxx... D:\Downloads          # download into D:\Downloads
chorrent get chr2aaaa... chr2bbbb... D:\Downloads  # several sharers' codes for the same file
chorrent get chr2xxxx... --seed                # download, then keep sharing it
```

**Things to know:**
- **Stopping and continuing:** press Ctrl+C any time. Run the same `get` command again later
  (or `chorrent resume`) and it continues from where it stopped. Nothing is downloaded twice.
- **Already have it?** If the file is already complete in that folder, `get` notices and
  finishes almost straight away.
- **While downloading, you also share.** Parts you've already received are passed on to
  other downloaders, unless you use `--no-reseed`.
- Every part is checked as it arrives. A damaged or tampered part is thrown away and fetched
  again, so the finished file is always exactly what the sender shared.

---

### doctor: check your connection

```bash
chorrent doctor              # check this computer
chorrent doctor <code>       # also check the people in a share code
```
Use this **when something doesn't connect**, or before an important transfer.

The first part checks your own computer:
- `[ok] relay server: connected`: good, people on any network can reach you.
- `[FAIL] relay server`: people on other networks probably can't reach you. See
  [It won't connect](#it-wont-connect).

With a code, it then tries to reach everyone listed in it:
- `[ok] peer ...: reachable (direct, 12 ms), serving this share`: all good.
- `[ok] peer ...: reachable (via relay (slower), 300 ms)`: works, but slower.
- `[FAIL] peer ...: unreachable`: that person can't be reached right now. They may have
  closed Chorrent, or they have the relay problem above. Ask them to run `chorrent doctor`.

`doctor` prints your IP addresses. That's fine to share with someone helping you, but
remove them before posting the output anywhere public.

---

### list, resume, forget: saved transfers

Chorrent remembers what you were sharing and any downloads that didn't finish, so you can
pick them up after closing the window or restarting the computer.

```bash
chorrent list                 # show what's saved
chorrent resume               # start all of them again
chorrent forget <share-id>    # remove one from the list
```
- **`list`** shows each saved item with its share ID, the long string of letters and
  numbers you need for `forget`.
- **`resume`** restarts everything in the list: sharing continues and unfinished downloads
  carry on. Your share codes **keep working after a restart**, so people you already sent a
  code to don't need a new one.
- **`forget`** only removes an item from the list. It **never deletes your files**.

---

### Options that work with every command

Put these before or after the command, e.g. `chorrent --upload-limit 1M seed movie.mp4`.

| Option | What it does | When to use it |
|---|---|---|
| `--upload-limit 1M` | Caps how fast you send, e.g. `500K` or `2M` per second | When sharing slows down your internet for everything else |
| `--download-limit 2M` | Caps how fast you download | Same idea, for downloads |
| `--max-uploads 32` | The most parts sent at the same time (default 32) | Lower it on a weak computer or connection |
| `--data-dir <folder>` | Where Chorrent keeps its saved information | Running two copies of Chorrent on one computer (give each its own folder) |
| `--no-state` | Don't save or remember anything this time | A quick one-off transfer, or testing |
| `--dht` | An extra way for computers to find each other | If a sender's code stops working after their internet connection changed |

By default, saved information lives in `%APPDATA%\chorrent` on Windows,
`~/Library/Application Support/chorrent` on macOS, and `~/.local/share/chorrent` on Linux.

---

## Common situations

### Sending to several people
Run `seed` once and send everyone the same code. Each downloader also helps the others, so
it doesn't take you much longer than sending to one person.

### Two people have the file and want to share it together
The first person runs `chorrent seed file`. The second runs
`chorrent seed file --join <first person's code>` with the same file. Downloaders can use
either code and will get parts from both.

### The download stopped halfway
Run the same `get` command again, or `chorrent resume`. It continues from where it left off.

### It won't connect
1. Run `chorrent doctor` on **both** computers.
2. If one shows `[FAIL] relay server`:
   - Make sure Windows Firewall allows Chorrent: *Windows Security → Firewall & network
     protection → Allow an app through firewall*, and tick `chorrent.exe` for both
     **Private** and **Public**.
   - Turn off any VPN and try again.
   - Try a different network. Some mobile hotspots and office networks block the connection.
3. Run `chorrent doctor <code>` on the downloading computer to see whether the sender can be
   reached at all.
4. Make sure the sender's Chorrent window is still open. Sharing stops when it's closed.

### It's slow
- If you see `via relay`, a direct connection wasn't possible between your two networks.
  It still works, but speed depends on the helper server.
- The sender's **upload** speed is usually the limit, and home connections often upload much
  more slowly than they download. More sharers (`--join`) means more upload speed in total.
- Speed going up and down during a transfer is normal. It follows the networks in between.

### Running two copies on one computer
Give each its own folder: `chorrent --data-dir C:\chorrent-a seed ...` and
`chorrent --data-dir C:\chorrent-b get ...`. If you forget, the second copy notices and
carries on without saving anything.

---

## Good to know

- **Is it private?** Files travel encrypted, so nobody in between can see what you're
  sending. They can see that two computers are connected, when, and roughly how much
  data moved. Use `--private` for anything personal, and only send the code to people
  you trust: **anyone with a private code can download the file.**
- **Is the file safe from tampering?** Yes. Every part is checked against the original, so a
  bad or altered part is rejected, never saved.
- **The share code changes if the file changes.** Editing, renaming or replacing the file
  gives it a new code.
- **Sharing only works while Chorrent is running** on at least one computer that has the
  complete file (or enough downloaders have it between them).

---

## For developers

### As a Rust library

The `chorrent` crate (`crates/chorrent`) does all the work. The command-line program is a thin
layer over it.

Add it to your project:
```bash
cargo add chorrent                      # or: cargo add chorrent --features mainline
```
API docs: <https://docs.rs/chorrent>

```rust
let client = chorrent::Client::builder()
    .data_dir("state")            // optional: remember identity + resume downloads
    .upload_limit(Some(1 << 20))  // optional: 1 MiB/s
    .build()
    .await?;

// Share a file or folder
let seed = client.seed("holiday-photos/").await?;
println!("share this: {}", seed.share_code());

// Download, watching progress
let code: chorrent::ShareCode = share_text.parse()?;
let download = client.download(&code, None).await?;
let mut events = download.events();      // PeerConnected, PieceVerified, ...
let done = download.finished().await?;   // every file verified
println!("saved to {:?}", done.path);   // None with an encrypted store
```

- `chorrent::Event` can be converted to JSON (serde), so a UI in another process can show progress.
- `Client::network_status` and `Client::check_peers` give you what `chorrent doctor` shows.
- Turn on the `mainline` cargo feature to get `ClientBuilder::mainline_dht` (what `--dht` uses).
- [`crates/chorrent/examples/share.rs`](crates/chorrent/examples/share.rs) is a complete small program.

### Inside an app that already uses iroh

An app with its own iroh endpoint (EchoIt, for example) can run Chorrent on it, so transfers
use the app's identity, relay server and connections:

```rust
let endpoint = iroh::Endpoint::builder(presets::N0)
    .alpns(vec![MY_ALPN.to_vec(), chorrent::ALPN.to_vec()])
    // ...your relay and key settings...
    .bind().await?;
let client = Arc::new(chorrent::Client::builder()
    .endpoint(endpoint.clone())          // gossip is off by default here
    .data_dir(app_data.join("chorrent"))
    .encrypted_storage(key_from_keychain) // received files stay encrypted on disk
    .build().await?);

// In your accept loop: finish each handshake in its own task (awaiting it
// inline lets one stalled peer block every connection), then route by ALPN.
while let Some(incoming) = endpoint.accept().await {
    let client = client.clone();
    tokio::spawn(async move {
        let Ok(conn) = incoming.await else { return };
        if client.alpns().iter().any(|a| a.as_slice() == conn.alpn()) {
            client.handle_connection(conn).await;
        } else {
            // ...your own protocol...
        }
    });
}

// Send: only the chat partner may download, even if the code leaks.
let seed = client.seed_with(path, SeedOptions::default().private(true).allow_peers([friend])).await?;
// Receive: lands in the encrypted store; read or export when the user asks.
let done = client.download(&code, None).await?.finished().await?;
let bytes = client.read_range(&done.id, 0, 0, 4096).await?;
client.export_file(&done.id, 0, save_path).await?;   // plaintext only when asked
```

`seed_reader` shares a stream (e.g. an Android content URI) instead of a file path. Chorrent
works with iroh 1.0.3 and newer, and builds for Android (`aarch64-linux-android`).

### How it works, briefly

- Files are split into 64 KiB pieces, each checked with BLAKE3 (`bao-tree`) as it arrives.
- Connections use iroh (QUIC, encrypted). Iroh tries to connect computers directly and falls
  back to a relay server when it can't.
- Sharers of the same file find each other through iroh-gossip. Downloads pull the rarest
  pieces first, from every sharer at once.

### Development

```bash
cargo test --workspace     # all tests (the network ones need internet: they use iroh relays)
cargo run --release -p chorrent --example bench -- 500    # local speed test, size in MiB
BENCH_ENCRYPTED=1 cargo run --release -p chorrent --example bench -- 500   # same, into an encrypted store
cargo test -p chorrent --test latency -- --ignored --nocapture   # speed over a relay-only path
```
