use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::protocol;
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

    loop {
        println!("Waiting for a peer to connect...");
        let conn = node.accept().await?;

        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };

            let request = protocol::receive_request(&mut recv).await?;
            println!("Serving bytes {}..{}", request.start, request.end);

            let encoded = chunker::serve_range(&path, &hashed.outboard, request.start, request.end)?;
            protocol::send_response(&mut send, &encoded).await?;
        }

        println!("Peer disconnected. Waiting for the next one...");
    }
}