use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::handler::ChorrentProtocol;
use iroh_tickets::{endpoint::EndpointTicket, Ticket};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

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
        held,
        total_pieces,
    };

    let node = ChorrentNode::bind(handler).await?;
    let ticket = EndpointTicket::new(node.addr());
    println!("Ticket: {}", ticket);

    println!("Ready and listening. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;

    Ok(())
}