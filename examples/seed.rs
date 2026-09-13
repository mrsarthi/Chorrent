use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use iroh_tickets::{endpoint::EndpointTicket, Ticket};
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).map(PathBuf::from).expect("usage: seed <file>");

    let hashed = chunker::hash_file(&path)?;
    let total_size = std::fs::metadata(&path)?.len();

    println!("Root hash: {}", hashed.root_hash.to_hex());
    println!("Total size: {}", total_size);

    let node = ChorrentNode::bind().await?;
    let ticket = EndpointTicket::new(node.addr());
    println!("Ticket: {}", ticket);

    println!("Waiting for a peer to connect...");
    let conn = node.accept().await?;

    loop {
        let (mut send, mut recv) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(_) => break, // peer finished and closed the connection
        };

        let request = recv.read_to_end(16).await?;
        let start = u64::from_le_bytes(request[0..8].try_into()?);
        let end = u64::from_le_bytes(request[8..16].try_into()?);
        println!("Serving bytes {}..{}", start, end);

        let encoded = chunker::serve_range(&path, &hashed.outboard, start, end)?;
        send.write_all(&encoded).await?;
        send.finish()?;
    }

    println!("Peer disconnected. Done.");
    Ok(())
}