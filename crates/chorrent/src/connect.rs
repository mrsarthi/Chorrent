use chorrent::stage2::node::ChorrentNode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_b = ChorrentNode::bind().await?;
    let node_a = ChorrentNode::bind().await?;
    let addr_b = node_b.addr();

    let handle = tokio::spawn(async move {
        let conn = node_b.accept().await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let bytes = recv.read_to_end(1000).await.unwrap();
        println!("Node B received: {}", String::from_utf8_lossy(&bytes));

        send.write_all(b"Hello Node A, handshake confirmed!").await.unwrap();
        send.finish().unwrap();
    });

    let conn = node_a.connect(addr_b).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    send.write_all(b"Hello Node B, this is Chorrent over QUIC!").await?;
    send.finish()?;

    let reply = recv.read_to_end(1000).await?;
    println!("Node A received reply: {}", String::from_utf8_lossy(&reply));

    handle.await.unwrap();
    Ok(())
}