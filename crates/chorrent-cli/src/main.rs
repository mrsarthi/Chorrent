use anyhow::{Context, Result};
use chorrent::{Client, Event, SeedOptions, ShareCode};
use clap::{Parser, Subcommand};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;

#[derive(Parser)]
#[command(name = "chorrent", about = "A peer-to-peer file-sharing engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Share a file with the network
    Seed {
        file: PathBuf,
        /// Join an existing swarm using another seeder's share code for the same file
        #[arg(long)]
        join: Option<String>,
    },
    /// Download a file from the network
    Get {
        share: String,
        /// Optional: override the original filename
        output: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Seed { file, join } => run_seed(file, join).await,
        Command::Get { share, output } => run_get(share, output).await,
    }
}

/// Endpoint ids are long; the first few characters are enough to tell peers apart.
fn short(peer: &str) -> &str {
    &peer[..peer.len().min(10)]
}

async fn run_seed(file: PathBuf, join: Option<String>) -> Result<()> {
    let mut options = SeedOptions::default();
    if let Some(code) = join {
        let code: ShareCode = code.parse().context("the --join share code isn't valid")?;
        options = options.join(code);
    }

    let client = Client::new().await.context("failed to start listening for connections")?;
    println!("Hashing {}...", file.display());
    let seed = client
        .seed_with(&file, options)
        .await
        .with_context(|| format!("failed to seed {}", file.display()))?;

    let share = seed.share_code();
    println!("Root hash: {}", share.root_hash());
    println!(
        "Total size: {} ({} pieces)",
        HumanBytes(share.total_size()),
        share.total_size().div_ceil(chorrent::PIECE_SIZE)
    );
    println!("Share this: {share}");
    println!("Seeding. Press Ctrl+C to stop.");

    let spinner = ProgressBar::new_spinner();
    spinner.enable_steady_tick(Duration::from_millis(200));
    spinner.set_style(ProgressStyle::with_template("{spinner:.green} Uploaded: {msg}").unwrap());

    let mut events = seed.events();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let (mut total, mut this_second) = (0u64, 0u64);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        tokio::select! {
            _ = &mut ctrl_c => break,
            _ = ticker.tick() => {
                spinner.set_message(format!("{} total, {}/s", HumanBytes(total), HumanBytes(this_second)));
                this_second = 0;
            }
            event = events.recv() => match event {
                Ok(Event::Uploaded { bytes }) => { total += bytes; this_second += bytes; }
                Ok(Event::PeerDiscovered { peer }) => spinner.println(format!("Peer {} joined the swarm", short(&peer))),
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => break,
            },
        }
    }

    spinner.finish_and_clear();
    seed.stop();
    client.shutdown().await;
    Ok(())
}

async fn run_get(share: String, output: Option<PathBuf>) -> Result<()> {
    let share: ShareCode = share.parse().context("the share code you provided isn't valid")?;

    let client = Client::new().await.context("failed to start the download node")?;
    let download = client
        .download(&share, output)
        .await
        .context("failed to start the download")?;

    println!("Discovering peers...");
    let pb = ProgressBar::new(share.total_size());
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({binary_bytes_per_sec}, ETA {eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    let mut events = download.events();
    let progress = tokio::spawn({
        let pb = pb.clone();
        async move {
            loop {
                match events.recv().await {
                    Ok(Event::PeerConnected { peer, pieces, total }) => {
                        pb.println(format!("{} connected ({pieces} of {total} pieces)", short(&peer)));
                    }
                    Ok(Event::PieceVerified { bytes, .. }) => pb.inc(bytes),
                    Ok(Event::PieceFailed { index, reason }) => {
                        pb.println(format!("Piece {index} failed, retrying: {reason}"));
                    }
                    Ok(_) | Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => break,
                }
            }
        }
    });

    let result = tokio::select! {
        result = download.finished() => result,
        _ = tokio::signal::ctrl_c() => Err(chorrent::Error::Cancelled),
    };
    progress.abort();

    match result {
        Ok(path) => {
            pb.finish_with_message("done");
            println!("Download complete and verified: {}", path.display());
        }
        Err(e) => {
            pb.abandon();
            client.shutdown().await;
            return Err(e).context("download failed");
        }
    }
    client.shutdown().await;
    Ok(())
}
