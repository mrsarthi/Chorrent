use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::handler::NullProtocol;
use chorrent::stage3::protocol::{self, PieceRequest};
use chorrent::stage4::scheduler::{self, SwarmState};
use chorrent::stage5::discovery;
use iroh::endpoint::Connection;
use iroh_tickets::endpoint::EndpointTicket;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    // usage: swarm_leech <root_hash> <total_size> <output> <bootstrap_ticket>
    let root_hash = blake3::Hash::from_hex(&args[1])?;
    let total_size: u64 = args[2].parse()?;
    let output = PathBuf::from(&args[3]);
    let bootstrap_ticket: EndpointTicket = args[4].parse()?;

    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;

    let node = ChorrentNode::bind(NullProtocol).await?;
    let topic_id = discovery::topic_for(&root_hash);
    let bootstrap = vec![bootstrap_ticket.endpoint_addr().id];
    let (_sender, receiver) = node.gossip().subscribe(topic_id, bootstrap).await?.split();

    let discovered: Arc<StdMutex<HashMap<String, Vec<bool>>>> = Arc::new(StdMutex::new(HashMap::new()));
    tokio::spawn(discovery::listen_for_peers(receiver, Arc::clone(&discovered)));

    println!("Discovering peers for 10 seconds...");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    let discovered_peers = discovered.lock().unwrap().clone();
    if discovered_peers.is_empty() {
        return Err("No peers discovered".into());
    }
    println!("Discovered {} peer(s), connecting...", discovered_peers.len());

    let mut connections: HashMap<String, Connection> = HashMap::new();
    let mut peer_bitfields: HashMap<String, Vec<bool>> = HashMap::new();

    for (i, (ticket_str, bitfield)) in discovered_peers.into_iter().enumerate() {
        let label = format!("peer{}", i);
        let ticket: EndpointTicket = ticket_str.parse()?;
        if let Ok(conn) = node.connect(ticket.endpoint_addr().clone()).await {
            println!("{} connected ({} of {} pieces)", label, bitfield.iter().filter(|&&b| b).count(), total_pieces);
            connections.insert(label.clone(), conn);
            peer_bitfields.insert(label, bitfield);
        }
    }

    if connections.is_empty() {
        return Err("Could not connect to any discovered peer".into());
    }

    let connections = Arc::new(connections);
    let state = Arc::new(Mutex::new(SwarmState { total_pieces, have: vec![false; total_pieces], peer_bitfields }));

    let mut handles = Vec::new();
    for _ in 0..connections.len() {
        let state = Arc::clone(&state);
        let connections = Arc::clone(&connections);
        let output = output.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let claimed = {
                    let mut state = state.lock().await;
                    if state.have.iter().all(|&b| b) { return; }
                    let playhead = state.have.iter().take_while(|&&b| b).count();
                    match scheduler::next_piece_to_request(&state, playhead, 3) {
                        Some((piece, peer_id)) => { state.have[piece] = true; Some((piece, peer_id)) }
                        None => None,
                    }
                };

                let (piece, peer_id) = match claimed {
                    Some(p) => p,
                    None => { tokio::time::sleep(std::time::Duration::from_millis(20)).await; continue; }
                };

                let start = piece as u64 * chunk_size;
                let end = std::cmp::min(start + chunk_size, total_size);
                let conn = &connections[&peer_id];

                let result: Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> = async {
                    let (mut send, mut recv) = conn.open_bi().await?;
                    protocol::send_piece_request(&mut send, &PieceRequest { start, end }).await?;
                    Ok(protocol::receive_piece_response(&mut recv).await?)
                }.await;

                match result {
                    Ok(Some(encoded)) => {
                        chunker::receive_range(&output, root_hash, total_size, start, end, &encoded).unwrap();
                        println!("Got piece {} from {} ({}..{})", piece, peer_id, start, end);
                    }
                    Ok(None) => {
                        let mut state = state.lock().await;
                        state.have[piece] = false;
                        if let Some(bf) = state.peer_bitfields.get_mut(&peer_id) { bf[piece] = false; }
                    }
                    Err(e) => {
                        let mut state = state.lock().await;
                        state.have[piece] = false;
                        eprintln!("Error on piece {} from {}: {:?}", piece, peer_id, e);
                    }
                }
            }
        }));
    }

    for handle in handles { handle.await?; }
    for conn in connections.values() { conn.close(0u32.into(), b"done"); }

    let verify = chunker::hash_file(&output)?;
    assert_eq!(verify.root_hash, root_hash, "final file does not match expected root hash!");
    println!("Full file transferred and verified from {} discovered peers.", connections.len());

    Ok(())
}