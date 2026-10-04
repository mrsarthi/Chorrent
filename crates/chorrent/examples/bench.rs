//! Throughput check: seed a file and download it between two nodes in one
//! process (over the real network stack, usually a direct local path).
//!
//!   cargo run --release -p chorrent --example bench -- 500    # size in MiB
//!
//! Set BENCH_ENCRYPTED=1 to download into an encrypted store (as EchoIt does),
//! and BENCH_TRACE=1 to print the speed every two seconds.

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

    let encrypted = std::env::var_os("BENCH_ENCRYPTED").is_some();
    let leecher = if encrypted {
        Client::builder().data_dir(dir.join("state")).encrypted_storage([9; 32]).build().await?
    } else {
        Client::new().await?
    };
    let out = dir.join("out");
    let t = Instant::now();
    let download = leecher.download(&seed.share_code(), (!encrypted).then(|| out.clone())).await?;
    let mut events = download.events();
    let trace = std::env::var_os("BENCH_TRACE").is_some();
    // A finished download keeps seeding, so its event stream never ends:
    // record the first piece's time and stop watching once it's done.
    let first = std::sync::Arc::new(std::sync::Mutex::new(None::<Instant>));
    let watcher = tokio::spawn({
        let first = std::sync::Arc::clone(&first);
        async move {
            let (mut window, mut tick) = (0u64, Instant::now());
            loop {
                match events.recv().await {
                    Ok(Event::PieceVerified { bytes, .. }) => {
                        first.lock().unwrap().get_or_insert_with(Instant::now);
                        window += bytes;
                        if trace && tick.elapsed().as_secs_f64() >= 2.0 {
                            println!("  {:.0} MiB/s", window as f64 / 1048576.0 / tick.elapsed().as_secs_f64());
                            (window, tick) = (0, Instant::now());
                        }
                    }
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(_) => return,
                }
            }
        }
    });
    let done = download.finished().await?;
    let total = t.elapsed().as_secs_f64();
    watcher.abort();
    let started = first.lock().unwrap().map(|i| i.duration_since(t).as_secs_f64()).unwrap_or(0.0);
    println!("connect:  {started:.2}s until the first piece arrived");
    println!("transfer: {mib} MiB in {:.2}s ({:.0} MiB/s, includes final verification)", total - started, mib as f64 / (total - started));
    if let Some(path) = &done.path {
        assert_eq!(std::fs::metadata(path).unwrap().len(), (mib * 1024 * 1024) as u64);
    }

    drop(done);
    seed.stop();
    leecher.shutdown().await;
    seeder.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
