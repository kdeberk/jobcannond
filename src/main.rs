#![allow(dead_code)]
#![allow(unused)]

mod protocol;
mod session;
mod jobs;
mod tubes;
mod ordered_set;

use session::Session;
use crate::tubes::TubeStore;

// TODO: Switch to tokio.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:11300")?;
    let tube_store = TubeStore::new();

    loop {
        let (socket, _) = listener.accept()?;
        let tube_store = tube_store.clone();

        std::thread::spawn(|| {
            Session::new(socket, tube_store).run();
        });
    }
}
