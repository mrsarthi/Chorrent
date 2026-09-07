#[tokio::main]
async fn main(){
    let handle = tokio::spawn(async {
        println!("Herrro");
    });

    println!("Herrro from main");

    handle.await.unwrap();
}