use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, PartialEq, Eq)]
pub struct Job {
 pub id: Option<u32>,
 pub pri: u32,
 pub ttr: u32,
 pub data: Arc<Vec<u8>>,
}

impl Ord for Job {
 fn cmp(&self, other: &Self) -> Ordering {
  self.pri.cmp(&other.pri).reverse()
 }
}

impl PartialOrd for Job {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
  self.pri.partial_cmp(&other.pri).map(|o| o.reverse())
 }
}

// A Tube is an object that holds unreserved jobs. These jobs can be ready or
// delayed.
#[derive(Default)]
pub struct Tube {
 // ready jobs are ordered by their priority.
 ready: BinaryHeap<ReadyJob>,
 // delayed jobs are ordered by a timestamp.
 delayed: BinaryHeap<DelayedJob>,
}

impl Tube {
 pub fn new() -> Self {
  Tube { ready: BinaryHeap::new(), delayed: BinaryHeap::new() }
 }

 // delayed_maintenance moves the delayed jobs to the
 // ready heap if there `until` time has passed.
 pub fn delayed_maintenance(&mut self) {
  loop {
   if self.delayed.is_empty() {
    break;
   }

   let job = self.delayed.peek().unwrap();
   if job.until < std::time::Instant::now() {
    break;
   }

   let job = self.delayed.pop().unwrap().job;
   self.push(job);
  }
 }

 pub fn push(&mut self, job: Job) {
  self.ready.push(ReadyJob { job });
 }

 pub fn pop(&mut self) -> Option<Job> {
  self.delayed_maintenance();

  match self.ready.pop() {
   None => None,
   Some(ready) => Some(ready.job),
  }
 }

 pub fn push_delayed(&mut self, job: Job, until: std::time::Instant) {
  self.delayed.push(DelayedJob { until, job });
 }

 // TODO: kick, bury
}

#[derive(PartialEq, Eq)]
struct ReadyJob {
 pub job: Job,
}

impl Ord for ReadyJob {
 fn cmp(&self, other: &Self) -> Ordering {
  self.job.cmp(&other.job)
 }
}

impl PartialOrd for ReadyJob {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
  self.job.partial_cmp(&other.job)
 }
}

#[derive(PartialEq, Eq)]
struct DelayedJob {
 pub until: std::time::Instant,
 pub job: Job,
}

impl Ord for DelayedJob {
 fn cmp(&self, other: &Self) -> Ordering {
  if self.until == other.until {
   self.job.cmp(&other.job).reverse()
  } else {
   self.until.cmp(&other.until) // TODO: reverse this to?
  }
 }
}

impl PartialOrd for DelayedJob {
 fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
  self.until.partial_cmp(&other.until)
 }
}

pub struct TubeStore {
 // store contains Arcs, it doesn't own the tube, but merely serves as a lookup.
 store: HashMap<String, Arc<Mutex<Tube>>>,
}

impl TubeStore {
 pub fn new() -> Self {
  Self { store: HashMap::new() }
 }

 pub fn get(&mut self, name: String) -> Arc<Mutex<Tube>> {
  let tube = self.store.entry(name).or_insert(Arc::new(Mutex::new(Tube::new())));

  tube.clone()
 }

 // TODO: delete
}
