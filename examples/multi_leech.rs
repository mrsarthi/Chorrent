use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::protocol::{self, PieceRequest};
use chorrent::stage4::scheduler::{self, SwarmState};
use iroh::endpoint::Connection;
use iroh_tickets::endpoint::EndpointTicket;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    // usage: multi_leech <root_hash> <total_size> <output> <ticket1> [<ticket2> ...]
    let root_hash = blake3::Hash::from_hex(&args[1])?;
    let total_size: u64 = args[2].parse()?;
    let output = PathBuf::from(&args[3]);
    let ticket_strs = &args[4..];

    let chunk_size = chunker::BLOCK_SIZE.bytes() as u64;
    let total_pieces = total_size.div_ceil(chunk_size) as usize;

    let node = ChorrentNode::bind().await?;

    let mut connections: HashMap<String, Connection> = HashMap::new();
    let mut peer_bitfields: HashMap<String, Vec<bool>> = HashMap::new();

    for (i, ticket_str) in ticket_strs.iter().enumerate() {
        let label = format!("peer{}", i);
        let ticket: EndpointTicket = ticket_str.parse()?;
        let conn = node.connect(ticket.endpoint_addr().clone()).await?;

        let (mut send, mut recv) = conn.open_bi().await?;
        protocol::request_bitfield(&mut send).await?;
        let bitfield = protocol::receive_bitfield(&mut recv, total_pieces).await?;

        println!("{} has {} of {} pieces", label, bitfield.iter().filter(|&&b| b).count(), total_pieces);

        connections.insert(label.clone(), conn);
        peer_bitfields.insert(label, bitfield);
    }

    let mut state = SwarmState { total_pieces, have: vec![false; total_pieces], peer_bitfields };

    loop {
        let playhead = state.have.iter().take_while(|&&b| b).count();
        if playhead == total_pieces {
            break; // everything downloaded
        }

        let window = 3;
        let Some((piece, peer_id)) = scheduler::next_piece_to_request(&state, playhead, window) else {
            return Err("Stuck: no connected peer has any remaining piece".into());
        };

        let start = piece as u64 * chunk_size;
        let end = std::cmp::min(start + chunk_size, total_size);

        let conn = &connections[&peer_id];
        let (mut send, mut recv) = conn.open_bi().await?;
        protocol::send_piece_request(&mut send, &PieceRequest { start, end }).await?;

        match protocol::receive_piece_response(&mut recv).await? {
            Some(encoded) => {
                chunker::receive_range(&output, root_hash, total_size, start, end, &encoded)?;
                state.have[piece] = true;
                println!("Got piece {} from {} ({}..{})", piece, peer_id, start, end);
            }
            None => {
                // Bitfield was stale — this peer doesn't actually have it.
                if let Some(bf) = state.peer_bitfields.get_mut(&peer_id) {
                    bf[piece] = false;
                }
                println!("Peer didn't have piece {}, will try someone else", piece);
            }
        }
    }

    for conn in connections.values() {
        conn.close(0u32.into(), b"done");
    }

    let verify = chunker::hash_file(&output)?;
    assert_eq!(verify.root_hash, root_hash, "final file does not match expected root hash!");
    println!("Full file transferred and verified from {} peers.", connections.len());

    Ok(())
}