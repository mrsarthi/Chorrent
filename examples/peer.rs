use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::handler::ChorrentProtocol;
use iroh_tickets::{endpoint::EndpointTicket, Ticket};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use chorrent::stage5::discovery;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let path = PathBuf::from(&args[1]);
    let pieces_arg = args.get(2).map(String::as_str).unwrap_or("all");

    let hashed = chunker::hash_file(&path)?;
    let total_size = std::fs::metadata(&path)?.len();
    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;

    let held: HashSet<usize> = if pieces_arg == "all" {
        (0..total_pieces).collect()
    } else {
        pieces_arg.split(',').map(|s| s.trim().parse().unwrap()).collect()
    };

    println!("Root hash: {}", hashed.root_hash.to_hex());
    println!("Total size: {}", total_size);
    println!("Total pieces: {}", total_pieces);
    println!("Holding {} of {} pieces", held.len(), total_pieces);

    let handler = ChorrentProtocol {
        path: path.clone(),
        root_hash: hashed.root_hash,
        outboard: hashed.outboard,
        held: held.clone(),
        total_pieces,
    };

    let node = ChorrentNode::bind(handler).await?;
    let ticket = EndpointTicket::new(node.addr());
    println!("Ticket: {}", ticket);

    println!("Ready and listening. Press Ctrl+C to stop.");
    let topic_id = discovery::topic_for(&hashed.root_hash);
    let bootstrap = match args.get(3) {
        Some(ticket_str) => {
            let boot_ticket: EndpointTicket = ticket_str.parse()?;
            vec![boot_ticket.endpoint_addr().id]
        }
        None => vec![],
    };
    let (sender, receiver) = node.gossip().subscribe(topic_id, bootstrap).await?.split();

    let known = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(discovery::listen_for_peers(receiver, known));

    let have: Vec<bool> = (0..total_pieces).map(|i| held.contains(&i)).collect();
    let announcement = discovery::Announcement { ticket: ticket.to_string(), bitfield: have };
    tokio::spawn(discovery::announce_periodically(sender, announcement));
    tokio::signal::ctrl_c().await?;

    Ok(())
}