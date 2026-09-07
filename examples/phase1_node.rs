#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0).bind().await?;
    let node_id = endpoint.id();

    println!("The node id is: {}", node_id);

    Ok(())
}