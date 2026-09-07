#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const ALPN: &[u8] = b"chorrent/0.1";

    let endpoint_b = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;

    let endpoint_a = iroh::Endpoint::bind(iroh::endpoint::presets::N0)
        .await?;
    let addr_b = endpoint_b.addr();

    let handle = tokio::spawn(async move {
        let incoming = endpoint_b.accept().await.unwrap();
        let conn = incoming.await.unwrap();

        let (mut send, mut recv) = conn.accept_bi().await.unwrap();

        let bytes = recv.read_to_end(1000).await.unwrap();
        println!("Node B received: {}", String::from_utf8_lossy(&bytes));

        send.write_all(b"Hello Node A, handshake confirmed!").await.unwrap();
        send.finish().unwrap();
    });

    let conn = endpoint_a.connect(addr_b, ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    send.write_all(b"Hello Node B, this is Chorrent over QUIC!").await?;
    send.finish()?;

    let reply = recv.read_to_end(1000).await?;
    println!("Node A received reply: {}", String::from_utf8_lossy(&reply));

    handle.await.unwrap();
    Ok(())
}

