//! The smallest useful chorrent program.
//!
//!   cargo run -p chorrent --example share -- seed <file-or-folder>
//!   cargo run -p chorrent --example share -- get <share-code>

use chorrent::{Client, Event};

#[tokio::main]
async fn main() -> chorrent::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let client = Client::new().await?;

    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["seed", path] => {
            let seed = client.seed(path).await?;
            println!("Share code: {}", seed.share_code());
            tokio::signal::ctrl_c().await.ok();
        }
        ["get", code] => {
            let download = client.download(&code.parse()?, None).await?;
            let mut events = download.events();
            tokio::spawn(async move {
                while let Ok(event) = events.recv().await {
                    if let Event::PieceVerified { index, .. } = event {
                        println!("got piece {index}");
                    }
                }
            });
            let done = download.finished().await?;
            println!("Saved to {}", done.path.display());
        }
        _ => eprintln!("usage: share seed <path> | share get <share-code>"),
    }
    client.shutdown().await;
    Ok(())
}
