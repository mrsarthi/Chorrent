//! Throughput check: seed a file and download it between two nodes in one
//! process (over the real network stack, usually a direct local path).
//!
//!   cargo run --release -p chorrent --example bench -- 500    # size in MiB

use chorrent::{Client, Event};
use std::io::Write;
use std::time::Instant;

#[tokio::main]
async fn main() -> chorrent::Result<()> {
    let mib: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let dir = std::env::temp_dir().join(format!("chorrent-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let src = dir.join("payload.bin");
    {
        let mut f = std::io::BufWriter::new(std::fs::File::create(&src).unwrap());
        let block: Vec<u8> = (0..1024 * 1024).map(|i: u32| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        for i in 0..mib {
            let mut b = block.clone();
            b[0] = i as u8; // keep blocks distinct
            f.write_all(&b).unwrap();
        }
    }

    let seeder = Client::new().await?;
    let t = Instant::now();
    let seed = seeder.seed(&src).await?;
    let hash_secs = t.elapsed().as_secs_f64();
    println!("hash:     {mib} MiB in {hash_secs:.2}s ({:.0} MiB/s)", mib as f64 / hash_secs);

    let leecher = Client::new().await?;
    let out = dir.join("out");
    let t = Instant::now();
    let download = leecher.download(seed.share_code(), Some(out.clone())).await?;
    let mut events = download.events();
    let trace = std::env::var_os("BENCH_TRACE").is_some();
    let first_piece = tokio::spawn(async move {
        let mut first = None;
        let (mut window, mut tick) = (0u64, Instant::now());
        loop {
            match events.recv().await {
                Ok(Event::PieceVerified { bytes, .. }) => {
                    first.get_or_insert_with(Instant::now);
                    window += bytes;
                    if trace && tick.elapsed().as_secs_f64() >= 2.0 {
                        println!("  {:.0} MiB/s", window as f64 / 1048576.0 / tick.elapsed().as_secs_f64());
                        (window, tick) = (0, Instant::now());
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => return first,
            }
        }
    });
    let done = download.finished().await?;
    let total = t.elapsed().as_secs_f64();
    let started = first_piece.await.ok().flatten().map(|i| i.duration_since(t).as_secs_f64()).unwrap_or(0.0);
    println!("connect:  {started:.2}s until the first piece arrived");
    println!("transfer: {mib} MiB in {:.2}s ({:.0} MiB/s, includes final verification)", total - started, mib as f64 / (total - started));
    assert_eq!(std::fs::metadata(&done.path).unwrap().len(), (mib * 1024 * 1024) as u64);

    drop(done);
    seed.stop();
    leecher.shutdown().await;
    seeder.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
