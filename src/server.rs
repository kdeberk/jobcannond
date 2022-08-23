use crate::jobs::JobStore;
use crate::session::Session;

// run_tcp_server contains the loop that waits for incoming connections and spawns session handlers for each.
pub async fn run_tcp_server(listener: tokio::net::TcpListener) -> Result<(), String> {
 let tube_store = JobStore::new();

 loop {
  let (stream, _) = listener.accept().await.unwrap();
  let tube_store = tube_store.clone();

  tokio::spawn(async {
   match Session::new(stream, tube_store).run().await {
    Ok(_) => println!("Connected terminates successfully"),
    Err(err) => println!("Err: {}", err),
   }
  });
 }
}
