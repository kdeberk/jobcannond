use jobcannond::protocol::{Command, Response};
use jobcannond::server::run_tcp_server;
use regex::Regex;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time;

enum Action {
 Send(Command),
 Receive(Response),
 Wait(time::Duration),
 Sync(SyncPoint), // Not sure what object, some semaphore perhaps?
}

#[tokio::test]
async fn test_put() {
 run_scenario(vec![vec![Action::Send(Command::Put { pri: 1, delay: 0, ttr: 10, data: "Hello, World!".into() }), Action::Receive(Response::Inserted { id: 1 })]]).await
}

async fn run_scenario(scenario: Vec<Vec<Action>>) {
 let addr = "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap();

 tokio::spawn(async move {
  let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
  run_tcp_server(listener).await.unwrap();
 });

 // for actions in scenario.into_iter() {
 //  let address = address.clone();
 //  thread::spawn(move || {
 //   let mut stream = TcpStream::connect(format!("{}:{}", address.ip(), address.port())).unwrap();

 //   for action in actions.into_iter() {
 //    match action {
 //     Action::Send(str) => {
 //      stream.write(str.as_bytes()).unwrap();
 //     }
 //     Action::Receive(_) => stream.read(),
 //     Action::Wait(dur) => thread::sleep(dur),
 //     Action::Sync(point) => point.sync(),
 //    }
 //   }
 //  })
 //  .join()
 //  .unwrap();
 // }
}

struct SyncPoint {
 mtx: Mutex<u64>,
 cv: Condvar,
}

impl SyncPoint {
 fn sync(&self) {
  let mut count = self.mtx.lock().unwrap();
  *count -= 1;
  if 0 == *count {
   self.cv.notify_all();
   return;
  }

  loop {
   count = self.cv.wait(count).unwrap();

   if 0 == *count {
    return;
   }
  }
 }
}
