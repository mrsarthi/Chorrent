use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    let path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("movie.mp4"));

    println!("Hashing {}...", path.display());
    let result = chorrent::stage1::chunker::hash_file(&path)?;

    println!("Root hash: {}", result.root_hash.to_hex());

    let outboard_path = path.with_extension("obao");
    fs::write(&outboard_path, &result.outboard.data)
        .with_context(|| format!("failed to write {}", outboard_path.display()))?;
    println!("Outboard written to {}", outboard_path.display());

    Ok(())
}