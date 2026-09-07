// use std::fs::File;
use std::io::prelude::*;

fn main() -> std::io::Result<()> {
    let mut file = std::fs::File::open("goal.txt")?;
    let mut contents = String::new();
    
    file.read_to_string(&mut contents)?;
    println!("Contents are: {}", contents);

    Ok(())
}