use futures::future::join_all;
use jobcannond::protocol::{Command, Error as ProtocolError, Protocol, Response};
use jobcannond::server::run_tcp_server;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

enum Action {
 Send(Command),
 Receive(Response),
 // Wait(time::Duration),
 // Sync(SyncPoint), // Not sure what object, some semaphore perhaps?
}

#[tokio::test]
async fn test_put_single_job() {
 let hello_then_inserted = vec![vec![Action::Send(Command::Put { pri: 1, delay: 0, ttr: 10, data: "Hello, World!".into() }), Action::Receive(Response::Inserted { id: 0 })]];

 run_scenario(hello_then_inserted).await
}

#[tokio::test]
async fn test_put_multiple_jobs() {
 let hello_then_inserted = vec![vec![
  Action::Send(Command::Put { pri: 1, delay: 0, ttr: 10, data: "Test 1".into() }),
  Action::Receive(Response::Inserted { id: 0 }),
  Action::Send(Command::Put { pri: 1, delay: 0, ttr: 10, data: "Test 2".into() }),
  Action::Receive(Response::Inserted { id: 1 }),
 ]];

 run_scenario(hello_then_inserted).await
}

#[tokio::test]
async fn test_put_then_reserve() {
 let hello_then_inserted = vec![vec![
  Action::Send(Command::Put { pri: 1, delay: 0, ttr: 10, data: "Hello, World!".into() }),
  Action::Receive(Response::Inserted { id: 0 }),
  Action::Send(Command::Reserve {}),
  Action::Receive(Response::Reserved { id: 0, data: "Hello, World!".into() }),
 ]];

 run_scenario(hello_then_inserted).await
}

#[tokio::test]
async fn test_puts_then_reserve_by_priority() {
 let hello_then_inserted = vec![vec![
  Action::Send(Command::Put { pri: 3, delay: 0, ttr: 10, data: "Test 1".into() }),
  Action::Receive(Response::Inserted { id: 0 }),
  Action::Send(Command::Put { pri: 1, delay: 0, ttr: 10, data: "Test 2".into() }),
  Action::Receive(Response::Inserted { id: 1 }),
  Action::Send(Command::Put { pri: 2, delay: 0, ttr: 10, data: "Test 3".into() }),
  Action::Receive(Response::Inserted { id: 2 }),
  Action::Send(Command::Reserve {}),
  Action::Receive(Response::Reserved { id: 1, data: "Test 2".into() }),
  Action::Send(Command::Reserve {}),
  Action::Receive(Response::Reserved { id: 2, data: "Test 3".into() }),
  Action::Send(Command::Reserve {}),
  Action::Receive(Response::Reserved { id: 0, data: "Test 1".into() }),
 ]];

 run_scenario(hello_then_inserted).await
}

async fn run_scenario(scenario: Vec<Vec<Action>>) {
 let (addr_tx, mut addr_rx) = mpsc::channel(1);

 tokio::spawn(async move {
  let addr = "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap();
  let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

  addr_tx.send(listener.local_addr().unwrap()).await.unwrap();

  run_tcp_server(listener).await.unwrap();
 });

 let address = addr_rx.recv().await.unwrap();

 let workers = scenario.into_iter().map(|trace| {
  let address = address.clone();

  tokio::spawn(async move {
   let address = address.clone();
   let stream = TcpStream::connect(format!("{}:{}", address.ip(), address.port())).await?;
   let mut protocol = Protocol::new(stream);

   for action in trace.into_iter() {
    match action {
     Action::Send(command) => {
      protocol.write_command(command).await?;
     }
     Action::Receive(expected) => {
      let resp = protocol.read_response().await?;
      assert_eq!(resp, expected);
     }
    }
   }

   Result::<(), ProtocolError>::Ok(())
  })
 });

 for res in join_all(workers).await {
  _ = res.unwrap();
 }
}

// struct SyncPoint {
//  mtx: Mutex<u64>,
//  cv: Condvar,
// }

// impl SyncPoint {
//  fn sync(&self) {
//   let mut count = self.mtx.lock().unwrap();
//   *count -= 1;
//   if 0 == *count {
//    self.cv.notify_all();
//    return;
//   }

//   loop {
//    count = self.cv.wait(count).unwrap();

//    if 0 == *count {
//     return;
//    }
//   }
//  }
// }
