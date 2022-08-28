#![allow(dead_code)]
#![allow(unused)]

use crate::jobs::{Job, JobStore, TubeID};
use crate::protocol::{Command, Error as ProtocolError, Protocol, Response};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(thiserror::Error, Debug)]
pub enum SessionError {
 #[error("{:?}", .0)]
 Stream(#[from] ProtocolError),
}

// Session wraps a client connection. It handles ownership of reserved jobs and
// releases them back to the JobStore if expired. At the moment, a session handles
// a single connection, but in the future, a session could have multiple connections.
pub struct Session<RW> {
 protocol: Protocol<RW>,

 tube_id: TubeID,
 tube_name: String,
 watched: HashSet<TubeID>,

 store: JobStore,

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
 pub fn new(stream: RW, store: JobStore) -> Self {
  let mut watched = HashSet::new();
  watched.insert(JobStore::DEFAULT_TUBE_ID);

  Session {
   // protocol wraps the client connection. It handles reading commands and writing responses.
   protocol: Protocol::new(stream),

   tube_id: JobStore::DEFAULT_TUBE_ID,
   tube_name: JobStore::DEFAULT_TUBE_NAME.into(),
   watched,

   store,

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
   self.store.push_ready(job);
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
   Command::Watch { tube } => {
    let tube_id = self.store.tube_id_for_name(&tube).await;
    self.watched.insert(tube_id);
    Ok(Response::Watching { count: self.watched.len() as u32 })
   }
   Command::Ignore { tube } => {
    let tube_id = self.store.tube_id_for_name(&tube).await;
    self.watched.remove(&tube_id);
    Ok(Response::Watching { count: self.watched.len() as u32 })
   }
   _ => todo!(),
  }
 }

 // handle_put places the job within the current session tube.
 async fn handle_put(&mut self, pri: u32, delay: u32, mut ttr: u32, data: Vec<u8>) -> Result<Response, SessionError> {
  if ttr == 0 {
   ttr = MIN_TTR;
  }

  let job = Job { id: None, pri, ttr, data, tube: self.tube_id };

  let job_id = if 0 < delay {
   let until = std::time::Instant::now() + std::time::Duration::new(delay as u64, 0);
   self.store.push_delayed(job, until).await
  } else {
   self.store.push_ready(job).await
  };

  Ok(Response::Inserted { id: job_id })
 }

 // handle_use sets the current tube used by this session. New pushed jobs will be pushed to this session.
 async fn handle_use(&mut self, tube: String) -> Result<Response, SessionError> {
  self.tube_name = tube;
  self.tube_id = self.store.tube_id_for_name(&self.tube_name).await;
  Ok(Response::Inserted { id: 1 })
 }

 // handle_reserve blocks until a job has become available on one of the watched tubes.
 async fn handle_reserve(&mut self) -> Result<Response, SessionError> {
  match self.store.pop_ready(&self.watched).await {
   Some(job) => {
    let (id, ttr, data) = (job.id.unwrap(), job.ttr, job.data);
    self.by_time.push(ReservedJob { id, until: std::time::Instant::now() + std::time::Duration::new(ttr as u64, 0) });
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
    if 0 < delay {
     let until = std::time::Instant::now() + std::time::Duration::new(delay as u64, 0);
     self.store.push_delayed(job, until).await;
    } else {
     self.store.push_ready(job).await;
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
