use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::protocol::{self, PieceRequest};
use iroh_tickets::endpoint::EndpointTicket;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let ticket: EndpointTicket = args[1].parse()?;
    let root_hash = blake3::Hash::from_hex(&args[2])?;
    let total_size: u64 = args[3].parse()?;
    let output = PathBuf::from(&args[4]);

    let node = ChorrentNode::bind().await?;
    let conn = node.connect(ticket.endpoint_addr().clone()).await?;

    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let mut start = 0u64;
    while start < total_size {
        let end = std::cmp::min(start + chunk_size, total_size);

        let (mut send, mut recv) = conn.open_bi().await?;
        protocol::send_piece_request(&mut send, &PieceRequest { start, end }).await?;

        let response = protocol::receive_piece_response(&mut recv).await?;
        let encoded = response.expect("seeder should have every piece in this simple test");
        chunker::receive_range(&output, root_hash, total_size, start, end, &encoded)?;
        println!("Verified and saved bytes {}..{}", start, end);

        start = end;
    }

    conn.close(0u32.into(), b"done");

    let verify = chunker::hash_file(&output)?;
    assert_eq!(verify.root_hash, root_hash, "final file does not match expected root hash!");
    println!("Full file transferred and verified successfully.");

    Ok(())
}