use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::protocol::{self, IncomingMessage};
use iroh_tickets::{endpoint::EndpointTicket, Ticket};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    // usage: peer <file> <pieces held: "all" or comma list like 0,1,2,5>
    let path = PathBuf::from(&args[1]);
    let pieces_arg = args.get(2).map(String::as_str).unwrap_or("all");

    let hashed = chunker::hash_file(&path)?;
    let total_size = std::fs::metadata(&path)?.len();
    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;

    let held: HashSet<usize> = if pieces_arg == "all" {
        (0..total_pieces).collect()
    } else {
        pieces_arg.split(',').map(|s| s.parse().unwrap()).collect()
    };

    let have: Vec<bool> = (0..total_pieces).map(|i| held.contains(&i)).collect();

    println!("Root hash: {}", hashed.root_hash.to_hex());
    println!("Total size: {}", total_size);
    println!("Total pieces: {}", total_pieces);
    println!("Holding {} of {} pieces", held.len(), total_pieces);

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

            match protocol::receive_message(&mut recv).await? {
                IncomingMessage::BitfieldRequest => {
                    protocol::send_bitfield(&mut send, &have).await?;
                }
                IncomingMessage::PieceRequest(req) => {
                    let piece_index = (req.start / chunk_size) as usize;
                    if held.contains(&piece_index) {
                        println!("Serving bytes {}..{}", req.start, req.end);
                        let encoded = chunker::serve_range(&path, &hashed.outboard, req.start, req.end)?;
                        protocol::send_piece_response(&mut send, Some(&encoded)).await?;
                    } else {
                        println!("Don't have piece {}, declining", piece_index);
                        protocol::send_piece_response(&mut send, None).await?;
                    }
                }
            }
        }

        println!("Peer disconnected. Waiting for the next one...");
    }
}