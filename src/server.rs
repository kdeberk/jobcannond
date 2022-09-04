use crate::jobs::TubeStore;
use crate::session::Session;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc};
use tokio::sync::Mutex;

// run_tcp_server contains the loop that waits for incoming connections and spawns session handlers for each.
pub async fn run_tcp_server(listener: tokio::net::TcpListener) -> Result<(), String> {
 let job_id_counter = Arc::new(AtomicU32::new(0));
 let mut tube_store = TubeStore::new();
 let default_tube = tube_store.get("default".into());

 let tube_store = Arc::new(Mutex::new(tube_store));
 
 loop {
  let (stream, _) = listener.accept().await.unwrap();
  let tube_store = tube_store.clone();
  let job_id_counter = job_id_counter.clone();
  let default_tube = default_tube.clone();
  
  tokio::spawn(async {
   match Session::new(stream, job_id_counter, tube_store, default_tube).run().await {
    Ok(_) => println!("Connected terminates successfully"),
    Err(err) => println!("Err: {}", err),
   }
  });
 }
}
