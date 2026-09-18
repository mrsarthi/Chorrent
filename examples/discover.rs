use chorrent::stage1::chunker;
use chorrent::stage2::node::ChorrentNode;
use chorrent::stage3::handler::NullProtocol;
use chorrent::stage5::discovery;
use iroh_tickets::endpoint::EndpointTicket;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    // usage: discover <file> [bootstrap_ticket]
    let path = PathBuf::from(&args[1]);
    let hashed = chunker::hash_file(&path)?;

    let node = ChorrentNode::bind(NullProtocol).await?;
    let topic_id = discovery::topic_for(&hashed.root_hash);

    let bootstrap = match args.get(2) {
        Some(ticket_str) => {
            let ticket: EndpointTicket = ticket_str.parse()?;
            vec![ticket.endpoint_addr().id]
        }
        None => vec![],
    };

    let (sender, receiver) = node.gossip().subscribe(topic_id, bootstrap).await?.split();

    let known = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(discovery::listen_for_peers(receiver, Arc::clone(&known)));

    let my_ticket = iroh_tickets::endpoint::EndpointTicket::new(node.addr());
    let announcement = discovery::Announcement {
        ticket: my_ticket.to_string(),
        bitfield: vec![true],
    };
    tokio::spawn(discovery::announce_periodically(sender, announcement));

    println!("My ticket (use this as the bootstrap for the next peer): {}", my_ticket);
    println!("Listening for other peers in this room...");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let known = known.lock().unwrap();
        println!("Currently know about {} peer(s)", known.len());
    }
}   