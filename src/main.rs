#![allow(dead_code)]
#![allow(unused)]

mod protocol;
mod session;
mod jobs;
mod tubes;
mod ordered_set;

use async_std;
use session::Session;
use crate::tubes::TubeStore;

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
                Err(err) => println!("Err: {}", err)
            }
        });
    }
}
