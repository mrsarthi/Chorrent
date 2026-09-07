#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let content = tokio::fs::read_to_string("test_chunk.txt").await?;
    println!("File content: {}", content);

    Ok(())
}
