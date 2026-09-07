use std::fs::File;
use std::io::prelude::*;

fn main() -> std::io::Result<()> {
    let mut file = File::open("testing.txt")?;
    let mut contents = String::new();
    
    file.read_to_string(&mut contents)?;
    assert_eq!(contents, "Hello World!");

    Ok(())
}