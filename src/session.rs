#![allow(dead_code)]
#![allow(unused)]

use crate::jobs::{Job, Tube, TubeStore};
use crate::protocol::{Command, Error as ProtocolError, Protocol, Response};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

#[derive(thiserror::Error, Debug)]
pub enum SessionError {
 #[error("{:?}", .0)]
 Stream(#[from] ProtocolError),
}

// Session wraps a client connection. It handles ownership of reserved jobs and
// releases them back to the JobStore if expired.
pub struct Session<RW> {
 protocol: Protocol<RW>,

 job_id_counter: Arc<AtomicU32>,
 tube: Arc<Mutex<Tube>>,
 tube_store: Arc<Mutex<TubeStore>>,

 by_time: BinaryHeap<ReservedJob>, // Jobs ordered by reservation expiration time.
 reserved: HashMap<u32, Job>,      // Jobs reserved by this connection.
}

// TODO:
// - Session keeps track of expiring reserved jobs.

// MIN_TTR is the lowest job TTR value we accept.
const MIN_TTR: u32 = 1;

impl<RW> Session<RW>
where RW: AsyncRead + AsyncWrite
{
 pub fn new(stream: RW, job_id_counter: Arc<AtomicU32>, tube_store: Arc<Mutex<TubeStore>>, tube: Arc<Mutex<Tube>>) -> Self {
  Session {
   // protocol wraps the client connection. It handles reading commands and writing responses.
   protocol: Protocol::new(stream),

   job_id_counter,
   tube,
   tube_store,

   by_time: BinaryHeap::new(),
   reserved: HashMap::new(),
  }
 }

 // run handles the connection until it's closed. It contains a single main
 // loop that reads commands and returns responses until the connection is closed.
 pub async fn run(&mut self) -> Result<(), SessionError> {
  loop {
   // TODO: also await some timer. If timer fires, release them back to store, and inform client of lost job.
   let cmd = self.read_command().await?;
   let response = self.handle(cmd).await?;
   self.write_response(response).await?;
  }

  for (_, job) in self.reserved.into_iter() {
   let mut tube = self.tube.lock().await;

   tube.push(job);
  }

  Ok(())
 }

 async fn read_command(&mut self) -> Result<Command, SessionError> {
  Ok(self.protocol.read_command().await?)
 }

 // handle acts on the read command; it could update the store, reserve a job, etc. Returns a response object.
 async fn handle(&mut self, command: Command) -> Result<Response, SessionError> {
  match command {
   Command::Put { pri, delay, ttr, data } => self.handle_put(pri, delay, ttr, data).await,
   Command::Use { tube } => self.handle_use(tube).await,
   Command::Reserve => self.handle_reserve().await,
   Command::ReserveWithTimeout { seconds: timeout } => Ok(Response::NotFound), // TODO
   Command::Delete { id } => self.handle_delete(id),
   Command::Release { id, pri, delay } => self.handle_release(id, pri, delay).await,
   Command::Bury { id, pri } => Ok(Response::NotFound), // TODO
   Command::Touch { id } => Ok(Response::NotFound),     // TODO
   _ => todo!(),
  }
 }

 // handle_put places the job within the current session tube.
 async fn handle_put(&mut self, pri: u32, delay: u32, mut ttr: u32, data: Vec<u8>) -> Result<Response, SessionError> {
  if ttr == 0 {
   ttr = MIN_TTR;
  }

  let job_id = self.job_id_counter.fetch_add(1, AtomicOrdering::AcqRel);
  let job = Job { id: None, pri, ttr, data: Arc::new(data) };
  let mut tube = self.tube.lock().await;

  if 0 < delay {
   let until = std::time::Instant::now() + std::time::Duration::new(delay as u64, 0);
   tube.push_delayed(job, until)
  } else {
   tube.push(job)
  };

  Ok(Response::Inserted { id: job_id })
 }

 // handle_use sets the current tube used by this session. New pushed jobs will be pushed to this session.
 async fn handle_use(&mut self, tube_name: String) -> Result<Response, SessionError> {
  let mut tube_store = self.tube_store.lock().await;
  self.tube = tube_store.get(tube_name);

  Ok(Response::Inserted { id: 1 })
 }

 // handle_reserve blocks until a job has become available on one of the watched tubes.
 async fn handle_reserve(&mut self) -> Result<Response, SessionError> {
  let mut tube = self.tube.lock().await;

  match tube.pop() {
   Some(job) => {
    let (id, ttr, data) = (job.id.unwrap(), job.ttr, job.data.clone());
    self.by_time.push(ReservedJob { id, until: std::time::Instant::now() + std::time::Duration::new(ttr as u64, 0) });
    self.reserved.insert(id, job);
    Ok(Response::Reserved { id, data })
   }
   None => Ok(Response::NotFound), // TODO: wait until one job is available
  }
 }

 fn handle_delete(&mut self, id: u32) -> Result<Response, SessionError> {
  match self.reserved.remove(&id) {
   Some(_) => Ok(Response::Deleted {}),
   None => Ok(Response::NotFound),
  }
 }

 async fn handle_release(&mut self, id: u32, pri: u32, delay: u32) -> Result<Response, SessionError> {
  match self.reserved.remove(&id) {
   Some(job) => {
    let mut tube = self.tube.lock().await;

    if 0 < delay {
     let until = std::time::Instant::now() + std::time::Duration::new(delay as u64, 0);
     tube.push_delayed(job, until);
    } else {
     tube.push(job);
    }
    Ok(Response::Released)
   }
   None => Ok(Response::NotFound),
  }
 }

 // write_response sends the response to the client.
 async fn write_response(&mut self, resp: Response) -> Result<(), SessionError> {
  self.protocol.write_response(resp).await?;
  Ok(())
 }
}

#[derive(PartialEq, Eq)]
// A ReservedJob wraps the id and reserved-until timestamp. It's used by the
// Session to order the jobs by their expiration times.
struct ReservedJob {
 pub until: std::time::Instant,
 pub id: u32,
}

impl Ord for ReservedJob {
 fn cmp(&self, other: &Self) -> Ordering {
  if self.until == other.until {
   self.id.cmp(&other.id)
  } else {
   self.until.cmp(&other.until)
  }
 }
}

impl PartialOrd for ReservedJob {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
  self.until.partial_cmp(&other.until)
 }
}
