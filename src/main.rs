#![allow(dead_code)]
#![allow(unused)]

mod jobs;
mod ordered_set;
mod protocol;
mod session;
mod tubes;

use crate::tubes::TubeStore;
use async_std;
use session::Session;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:11300")?;
    let tube_store = TubeStore::new();

    loop {
        let (socket, _) = listener.accept()?;
        let tube_store = tube_store.clone();

        async_std::task::spawn(async {
            match Session::new(socket, tube_store).run().await {
                Ok(_) => println!("Connected terminates successfully"),
                Err(err) => println!("Err: {}", err),
            }
        });
    }
}
