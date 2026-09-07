#[tokio::main]
async fn main(){
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);

    tokio::spawn(async move {
        tx.send("hello over the channel").await.unwrap();
    });

    let received = rx.recv().await.unwrap();

    println!("{}", received)
}