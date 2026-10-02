use anyhow::{bail, Context, Result};
use chorrent::{Client, DownloadHandle, Error, Event, SavedTransfer, SeedHandle, SeedOptions, ShareCode, Transfer};
use clap::{Args, Parser, Subcommand};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;

#[derive(Parser)]
#[command(name = "chorrent", version, about = "Peer-to-peer file and folder sharing")]
struct Cli {
    #[command(flatten)]
    opts: GlobalOpts,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct GlobalOpts {
    /// Where to keep identity, seeds and download progress
    /// [default: the platform's app data folder]
    #[arg(long, global = true, env = "CHORRENT_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Don't save or load any state (fresh identity, no resume)
    #[arg(long, global = true)]
    no_state: bool,
    /// Upload speed limit, e.g. 500K or 2M (bytes per second)
    #[arg(long, global = true, value_parser = parse_rate)]
    upload_limit: Option<u64>,
    /// Download speed limit, e.g. 500K or 2M (bytes per second)
    #[arg(long, global = true, value_parser = parse_rate)]
    download_limit: Option<u64>,
    /// Most pieces uploaded at the same time
    #[arg(long, global = true, default_value_t = 32)]
    max_uploads: usize,
    /// Also find peers' addresses via the BitTorrent Mainline DHT (publishes
    /// this node's relay address there, never its IP)
    #[arg(long, global = true)]
    dht: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Share a file or folder
    Seed {
        path: PathBuf,
        /// Only people with the share code can find or download it
        #[arg(long)]
        private: bool,
        /// Join the swarm of another seeder of the same content, using their share code
        #[arg(long, value_name = "SHARE_CODE")]
        join: Option<String>,
    },
    /// Download a share
    Get {
        share: String,
        /// Folder to download into [default: current folder]
        dest_dir: Option<PathBuf>,
        /// Keep seeding after the download finishes, until Ctrl+C
        #[arg(long)]
        seed: bool,
        /// Don't serve pieces to other peers while downloading
        #[arg(long)]
        no_reseed: bool,
    },
    /// List saved seeds and unfinished downloads
    List,
    /// Resume every saved seed and unfinished download
    Resume,
    /// Remove a share from saved state (files are not touched)
    Forget { share_id: String },
}

fn parse_rate(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (digits, mult) = match s.char_indices().last() {
        Some((i, 'k' | 'K')) => (&s[..i], 1024),
        Some((i, 'm' | 'M')) => (&s[..i], 1024 * 1024),
        Some((i, 'g' | 'G')) => (&s[..i], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    digits
        .trim()
        .parse::<u64>()
        .map(|n| n * mult)
        .map_err(|_| format!("{s:?} isn't a rate like 500K or 2M"))
}

fn default_data_dir() -> Option<PathBuf> {
    let base = if cfg!(windows) {
        std::env::var_os("APPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        std::env::var_os("HOME").map(|h| PathBuf::from(h).join("Library/Application Support"))
    } else {
        std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
    };
    base.map(|b| b.join("chorrent"))
}

/// `needs_state`: the command is about saved state (list, resume, forget), so
/// running without it would be meaningless.
async fn make_client(opts: &GlobalOpts, reseed: bool, needs_state: bool) -> Result<Client> {
    let builder = Client::builder()
        .reseed(reseed)
        .max_concurrent_uploads(opts.max_uploads)
        .upload_limit(opts.upload_limit)
        .download_limit(opts.download_limit)
        .mainline_dht(opts.dht);
    let data_dir = if opts.no_state { None } else { opts.data_dir.clone().or_else(default_data_dir) };
    let Some(dir) = data_dir else {
        if needs_state {
            bail!("this command needs saved state; drop --no-state");
        }
        return Ok(builder.build().await?);
    };
    match builder.clone().data_dir(&dir).build().await {
        Ok(client) => Ok(client),
        Err(Error::DataDirInUse(_)) if needs_state => {
            bail!("another chorrent is using {}; stop it first, or pass a different --data-dir", dir.display())
        }
        Err(Error::DataDirInUse(_)) => {
            eprintln!(
                "Note: another chorrent is using {}; this one runs without saved state.",
                dir.display()
            );
            Ok(builder.build().await?)
        }
        Err(e) => Err(e).context("failed to start"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Seed { path, private, join } => run_seed(&cli.opts, path, private, join).await,
        Command::Get { share, dest_dir, seed, no_reseed } => {
            run_get(&cli.opts, share, dest_dir, seed, !no_reseed).await
        }
        Command::List => run_list(&cli.opts).await,
        Command::Resume => run_resume(&cli.opts).await,
        Command::Forget { share_id } => {
            let client = make_client(&cli.opts, true, true).await?;
            client.forget(&share_id.parse()?)?;
            println!("Forgot {share_id}");
            client.shutdown().await;
            Ok(())
        }
    }
}

/// Print a line above a progress bar. When output isn't a terminal the bar
/// is hidden and would swallow the line, so print it plainly instead.
fn say(pb: &ProgressBar, line: String) {
    if pb.is_hidden() {
        println!("{line}");
    } else {
        pb.println(line);
    }
}

fn say_multi(multi: &MultiProgress, line: String) {
    if multi.is_hidden() {
        println!("{line}");
    } else {
        let _ = multi.println(line);
    }
}

/// Endpoint ids are long; the first few characters are enough to tell peers apart.
fn short(peer: &str) -> &str {
    &peer[..peer.len().min(10)]
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} {prefix} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({binary_bytes_per_sec}, ETA {eta}) {msg}",
    )
    .unwrap()
    .progress_chars("#>-")
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.green} {prefix} {msg}").unwrap()
}

fn print_seed_info(seed: &SeedHandle) {
    let code = seed.share_code();
    println!("Sharing: {}", seed.path().display());
    println!("Size:    {}", HumanBytes(code.total_size()));
    if code.is_private() {
        println!("Private: only people with this code can find it");
    }
    println!("Share id: {}", code.id());
    println!("\nShare this code:\n{code}\n");
}

async fn run_seed(opts: &GlobalOpts, path: PathBuf, private: bool, join: Option<String>) -> Result<()> {
    let mut options = SeedOptions::default().private(private);
    if let Some(code) = join {
        options = options.join(code.parse().context("the --join share code isn't valid")?);
    }
    let client = make_client(opts, true, false).await?;
    println!("Hashing {}...", path.display());
    let seed = client
        .seed_with(&path, options)
        .await
        .with_context(|| format!("failed to share {}", path.display()))?;
    print_seed_info(&seed);
    println!("Seeding. Press Ctrl+C to stop.");

    let multi = MultiProgress::new();
    let watcher = tokio::spawn(watch_seed(seed.events(), multi.add(ProgressBar::new_spinner()), String::new()));
    tokio::signal::ctrl_c().await?;
    watcher.abort();
    seed.stop();
    client.shutdown().await;
    Ok(())
}

async fn watch_seed(mut events: tokio::sync::broadcast::Receiver<Event>, spinner: ProgressBar, prefix: String) {
    spinner.set_style(spinner_style());
    spinner.set_prefix(prefix);
    spinner.enable_steady_tick(Duration::from_millis(200));
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let (mut total, mut this_second) = (0u64, 0u64);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                spinner.set_message(format!("uploaded {} ({}/s)", HumanBytes(total), HumanBytes(this_second)));
                this_second = 0;
            }
            event = events.recv() => match event {
                Ok(Event::Uploaded { bytes }) => { total += bytes; this_second += bytes; }
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            },
        }
    }
}

/// Show a download's progress on `pb` until it ends; returns its result.
async fn drive_download(download: DownloadHandle, pb: ProgressBar) -> chorrent::Result<chorrent::Finished> {
    pb.set_style(bar_style());
    pb.set_length(download.share_code().total_size());
    pb.set_message("finding peers...");
    pb.enable_steady_tick(Duration::from_millis(200));

    let mut events = download.events();
    let progress = tokio::spawn({
        let pb = pb.clone();
        async move {
            let mut peers = 0usize;
            loop {
                match events.recv().await {
                    Ok(Event::ManifestReceived { files, .. }) => {
                        pb.set_message(format!("{files} file(s)"));
                    }
                    Ok(Event::Resumed { pieces, bytes }) => {
                        pb.inc(bytes);
                        say(&pb, format!("Resuming: {pieces} pieces ({}) already on disk", HumanBytes(bytes)));
                    }
                    Ok(Event::PeerConnected { peer, pieces, total }) => {
                        peers += 1;
                        pb.set_message(format!("{peers} peer(s)"));
                        say(&pb, format!("Peer {} connected ({pieces} of {total} pieces)", short(&peer)));
                    }
                    Ok(Event::PeerPath { peer, direct, rtt_ms }) => {
                        let how = if direct { "direct connection" } else { "via relay (slower; hole punching didn't work yet)" };
                        say(&pb, format!("Peer {}: {how}, {rtt_ms} ms round trip", short(&peer)));
                    }
                    Ok(Event::PeerDisconnected { peer }) => {
                        peers = peers.saturating_sub(1);
                        pb.set_message(format!("{peers} peer(s)"));
                        say(&pb, format!("Peer {} left", short(&peer)));
                    }
                    Ok(Event::PieceVerified { bytes, .. }) => pb.inc(bytes),
                    Ok(Event::PieceFailed { index, reason }) => {
                        say(&pb, format!("Piece {index} failed, retrying: {reason}"));
                    }
                    Ok(_) | Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => break,
                }
            }
        }
    });
    let result = download.finished().await;
    progress.abort();
    match &result {
        Ok(_) => pb.finish_with_message("done, verified"),
        Err(_) => pb.abandon(),
    }
    result
}

async fn run_get(opts: &GlobalOpts, share: String, dest_dir: Option<PathBuf>, keep_seeding: bool, reseed: bool) -> Result<()> {
    let code: ShareCode = share.parse().context("the share code you provided isn't valid")?;
    let client = make_client(opts, reseed, false).await?;
    println!("Downloading {} ({})", code.name(), HumanBytes(code.total_size()));
    let download = client.download(&code, dest_dir).await.context("failed to start the download")?;

    let pb = ProgressBar::new(code.total_size());
    let driver = tokio::spawn(drive_download(download, pb.clone()));
    let abort = driver.abort_handle();
    let finished = tokio::select! {
        result = driver => result?,
        _ = tokio::signal::ctrl_c() => {
            // Dropping the handle (by aborting its driver) saves progress and stops.
            abort.abort();
            tokio::time::sleep(Duration::from_millis(300)).await;
            client.shutdown().await;
            println!("\nStopped. Run `chorrent resume` (or the same `get`) to continue.");
            return Ok(());
        }
    };

    let finished = match finished {
        Ok(f) => f,
        Err(e) => {
            client.shutdown().await;
            return Err(e).context("download failed");
        }
    };
    println!("Saved to {}", finished.path.display());

    if keep_seeding && let Some(seed) = finished.seed {
        println!("\nSeeding it too. Share this code:\n{}\n", seed.share_code());
        println!("Press Ctrl+C to stop.");
        let watcher = tokio::spawn(watch_seed(seed.events(), ProgressBar::new_spinner(), String::new()));
        tokio::signal::ctrl_c().await?;
        watcher.abort();
    }
    client.shutdown().await;
    Ok(())
}

async fn run_list(opts: &GlobalOpts) -> Result<()> {
    let client = make_client(opts, true, true).await?;
    let saved = client.saved()?;
    if saved.is_empty() {
        println!("Nothing saved.");
    }
    for item in &saved {
        match item {
            SavedTransfer::Seed { id, path, private } => {
                let private = if *private { " (private)" } else { "" };
                println!("seed      {id}  {}{private}", path.display());
            }
            SavedTransfer::Download { code, dest_dir } => {
                println!("download  {}  {} -> {}", code.id(), code.name(), dest_dir.display());
            }
            _ => {}
        }
    }
    client.shutdown().await;
    Ok(())
}

async fn run_resume(opts: &GlobalOpts) -> Result<()> {
    let client = make_client(opts, true, true).await?;
    let saved = client.saved()?;
    if saved.is_empty() {
        bail!("nothing to resume");
    }
    let multi = MultiProgress::new();
    let mut seeds = Vec::new();
    let mut tasks = tokio::task::JoinSet::new();
    for item in &saved {
        match client.resume(item).await {
            Ok(Transfer::Seed(seed)) => {
                say_multi(&multi, format!("Seeding {}\n  code: {}", seed.path().display(), seed.share_code()));
                let name = seed.share_code().name().to_string();
                tasks.spawn(watch_seed(seed.events(), multi.add(ProgressBar::new_spinner()), name));
                seeds.push(seed);
            }
            Ok(Transfer::Download(download)) => {
                let pb = multi.add(ProgressBar::new(0));
                pb.set_prefix(download.share_code().name().to_string());
                tasks.spawn(async move {
                    match drive_download(download, pb.clone()).await {
                        // Keep it seeding for as long as `resume` runs.
                        Ok(finished) => {
                            if let Some(seed) = finished.seed {
                                watch_seed(seed.events(), pb, String::new()).await;
                            }
                        }
                        Err(e) => say(&pb, format!("Download failed: {e}")),
                    }
                });
            }
            Err(e) => say_multi(&multi, format!("Couldn't resume {item:?}: {e}")),
        }
    }
    println!("Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    tasks.abort_all();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(seeds);
    client.shutdown().await;
    Ok(())
}
