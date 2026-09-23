use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::handler::{ChorrentProtocol, NullProtocol};
use chorrent::stage3::protocol::{self, PieceRequest};
use chorrent::stage4::scheduler::{self, SwarmState};
use chorrent::stage5::discovery;
use clap::{Parser, Subcommand};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use iroh::endpoint::Connection;
use iroh_tickets::endpoint::EndpointTicket;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex as TokioMutex;

#[derive(Serialize, Deserialize)]
struct ShareInfo {
    ticket: String,
    root_hash: String,
    total_size: u64,
    file_name: String,
}

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
        /// Join an existing swarm using another peer's ticket
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

async fn run_seed(file: PathBuf, join: Option<String>) -> Result<()> {
    let hashed = chunker::hash_file(&file)
        .with_context(|| format!("failed to hash {}", file.display()))?;
    let total_size = std::fs::metadata(&file)
        .with_context(|| format!("failed to read metadata for {}", file.display()))?
        .len();
    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;
    let held: HashSet<usize> = (0..total_pieces).collect();

    let file_name = file
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "download".to_string());

    println!("Root hash: {}", hashed.root_hash.to_hex());
    println!("Total size: {}", total_size);
    println!("Total pieces: {}", total_pieces);
    println!("Holding {} of {} pieces", held.len(), total_pieces);

    let bytes_served = Arc::new(AtomicU64::new(0));

    let handler = ChorrentProtocol {
        path: file.clone(),
        root_hash: hashed.root_hash,
        outboard: hashed.outboard,
        held: held.clone(),
        total_pieces,
        bytes_served: Arc::clone(&bytes_served),
    };

    let node = ChorrentNode::bind(handler)
        .await
        .context("failed to start listening for connections")?;
    let ticket = EndpointTicket::new(node.addr());
    println!("Ticket: {}", ticket);

    let topic_id = discovery::topic_for(&hashed.root_hash);
    let bootstrap = match join {
        Some(ticket_str) => {
            let boot_ticket: EndpointTicket = ticket_str
                .parse()
                .context("the --join ticket you provided isn't valid")?;
            vec![boot_ticket.endpoint_addr().id]
        }
        None => vec![],
    };

    let (sender, receiver) = node
        .gossip()
        .subscribe(topic_id, bootstrap)
        .await
        .context("failed to join the gossip network for this file")?
        .split();

    let known = Arc::new(StdMutex::new(HashSet::new()));
    tokio::spawn(discovery::listen_for_peers(receiver, known));

    let announcement = discovery::Announcement { ticket: ticket.to_string() };
    tokio::spawn(discovery::announce_periodically(sender, announcement));

    let share_info = ShareInfo {
        ticket: ticket.to_string(),
        root_hash: hashed.root_hash.to_hex().to_string(),
        total_size,
        file_name,
    };
    let bytes = postcard::to_allocvec(&share_info).context("failed to build the share code")?;
    let share_code = STANDARD.encode(bytes);
    println!("Share this: {}", share_code);
    println!("Seeding. Press Ctrl+C to stop.");

    let spinner = ProgressBar::new_spinner();
    spinner.enable_steady_tick(std::time::Duration::from_millis(200));
    spinner.set_style(ProgressStyle::with_template("{spinner:.green} Uploaded: {msg}").unwrap());

    tokio::spawn({
        let bytes_served = Arc::clone(&bytes_served);
        let spinner = spinner.clone();
        async move {
            let mut last = 0u64;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let current = bytes_served.load(Ordering::Relaxed);
                let rate = current.saturating_sub(last);
                last = current;
                spinner.set_message(format!("{} total, {}/s", HumanBytes(current), HumanBytes(rate)));
            }
        }
    });

    tokio::signal::ctrl_c().await.context("failed while waiting for Ctrl+C")?;
    Ok(())
}

async fn run_get(share: String, output_override: Option<PathBuf>) -> Result<()> {
    let bytes = STANDARD
        .decode(&share)
        .context("the share code you provided isn't valid")?;
    let share_info: ShareInfo = postcard::from_bytes(&bytes)
        .context("the share code you provided is corrupted")?;

    let root_hash = blake3::Hash::from_hex(&share_info.root_hash)
        .context("the root hash inside the share code isn't valid")?;
    let bootstrap_ticket: EndpointTicket = share_info
        .ticket
        .parse()
        .context("the ticket inside the share code isn't valid")?;
    let total_size = share_info.total_size;
    let output = output_override.unwrap_or_else(|| PathBuf::from(&share_info.file_name));

    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;

    let node = ChorrentNode::bind(NullProtocol)
        .await
        .context("failed to start the download node")?;
    let topic_id = discovery::topic_for(&root_hash);
    let bootstrap = vec![bootstrap_ticket.endpoint_addr().id];
    let (_sender, receiver) = node
        .gossip()
        .subscribe(topic_id, bootstrap)
        .await
        .context("failed to join the gossip network for this file")?
        .split();

    let discovered: Arc<StdMutex<HashSet<String>>> = Arc::new(StdMutex::new(HashSet::new()));
    tokio::spawn(discovery::listen_for_peers(receiver, Arc::clone(&discovered)));

    println!("Discovering peers...");
    let discovered_peers = loop {
        for elapsed in 1..=30 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let found = discovered.lock().unwrap().clone();
            if !found.is_empty() {
                println!("Found a peer after {} second(s)", elapsed);
                break;
            }
            if elapsed == 30 {
                anyhow::bail!(
                    "No peers were discovered after 30 seconds — double-check the share code, \
                    confirm the seeder is still running, and try again"
                );
            }
        }
        break discovered.lock().unwrap().clone();
    };
    println!("Discovered {} peer(s), connecting...", discovered_peers.len());

    let mut connections: HashMap<String, Connection> = HashMap::new();
    let mut peer_bitfields: HashMap<String, Vec<bool>> = HashMap::new();

    for (i, peer_ticket_str) in discovered_peers.into_iter().enumerate() {
        let label = format!("peer{}", i);
        let peer_ticket: EndpointTicket = peer_ticket_str
            .parse()
            .context("failed to parse a discovered peer's ticket")?;

        let Ok(conn) = node.connect(peer_ticket.endpoint_addr().clone()).await else {
            continue;
        };
        let Ok((mut send, mut recv)) = conn.open_bi().await else {
            continue;
        };
        if protocol::request_bitfield(&mut send).await.is_err() {
            continue;
        }
        let Ok(bitfield) = protocol::receive_bitfield(&mut recv, total_pieces).await else {
            continue;
        };

        println!(
            "{} connected ({} of {} pieces)",
            label,
            bitfield.iter().filter(|&&b| b).count(),
            total_pieces
        );
        connections.insert(label.clone(), conn);
        peer_bitfields.insert(label, bitfield);
    }

    anyhow::ensure!(!connections.is_empty(), "Could not connect to any discovered peer");

    println!("Downloading to {}", output.display());
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({binary_bytes_per_sec}, ETA {eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    let connections = Arc::new(connections);
    let state = Arc::new(TokioMutex::new(SwarmState {
        total_pieces,
        have: vec![false; total_pieces],
        peer_bitfields,
    }));

    const WORKERS_PER_PEER: usize = 8;
    let mut handles = Vec::new();
    for _ in 0..(connections.len() * WORKERS_PER_PEER) {
        let state = Arc::clone(&state);
        let connections = Arc::clone(&connections);
        let output = output.clone();
        let pb = pb.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let claimed = {
                    let mut state = state.lock().await;
                    if state.have.iter().all(|&b| b) {
                        return;
                    }
                    let playhead = state.have.iter().take_while(|&&b| b).count();
                    match scheduler::next_piece_to_request(&state, playhead, 3) {
                        Some((piece, peer_id)) => {
                            state.have[piece] = true;
                            Some((piece, peer_id))
                        }
                        None => None,
                    }
                };

                let (piece, peer_id) = match claimed {
                    Some(p) => p,
                    None => {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        continue;
                    }
                };

                let start = piece as u64 * chunk_size;
                let end = std::cmp::min(start + chunk_size, total_size);
                let conn = &connections[&peer_id];

                let result: Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> = async {
                    let (mut send, mut recv) = conn.open_bi().await?;
                    protocol::send_piece_request(&mut send, &PieceRequest { start, end }).await?;
                    Ok(protocol::receive_piece_response(&mut recv).await?)
                }
                .await;

                match result {
                    Ok(Some(encoded)) => {
                        match chunker::receive_range(&output, root_hash, total_size, start, end, &encoded) {
                            Ok(()) => pb.inc(end - start),
                            Err(e) => pb.println(format!("Failed to save piece {}: {}", piece, e)),
                        }
                    }
                    Ok(None) => {
                        let mut state = state.lock().await;
                        state.have[piece] = false;
                        if let Some(bf) = state.peer_bitfields.get_mut(&peer_id) {
                            bf[piece] = false;
                        }
                    }
                    Err(e) => {
                        let mut state = state.lock().await;
                        state.have[piece] = false;
                        pb.println(format!("Error fetching piece {} from {}: {:?}", piece, peer_id, e));
                        drop(state);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.await.context("a download worker task failed unexpectedly")?;
    }
    for conn in connections.values() {
        conn.close(0u32.into(), b"done");
    }

    let verify = chunker::hash_file(&output).context("failed to verify the downloaded file")?;
    anyhow::ensure!(
        verify.root_hash == root_hash,
        "Downloaded file does not match the expected root hash — it may be corrupted"
    );

    pb.finish_with_message("Download complete and verified");
    Ok(())
}