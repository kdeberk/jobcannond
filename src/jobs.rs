use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type TubeID = usize;

// TODO: Write :tiny: tests for the comparisons

#[derive(Debug, PartialEq, Eq)]
pub struct Job {
 pub id: Option<u32>,
 pub pri: u32,
 pub ttr: u32,
 pub data: Vec<u8>,
 pub tube: TubeID,
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
 // ready jobs are ordered by their priority
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

 pub fn peek(&self) -> Option<&Job> {
  match self.ready.peek() {
   None => None,
   Some(ready) => Some(&ready.job),
  }
 }

 pub fn pop(&mut self) -> Option<Job> {
  match self.ready.pop() {
   None => None,
   Some(ready) => Some(ready.job),
  }
 }

 pub fn push_delayed(&mut self, job: Job, until: std::time::Instant) {
  self.delayed.push(DelayedJob { until, job });
 }
}

// JobStore owns all tubes, and so indirectly, also owns all unreserved jobs.
#[derive(Clone, Default)]
pub struct JobStore {
 job_counter: Arc<AtomicU32>,
 tube_ids: Arc<Mutex<HashMap<String, usize>>>,
 store: Arc<Mutex<HashMap<TubeID, Tube>>>,
}

impl JobStore {
 pub const DEFAULT_TUBE_NAME: &'static str = "default";
 pub const DEFAULT_TUBE_ID: TubeID = 0;

 pub fn new() -> Self {
  let mut tube_ids = HashMap::<String, usize>::new();
  tube_ids.insert(Self::DEFAULT_TUBE_NAME.into(), Self::DEFAULT_TUBE_ID);

  Self {
   job_counter: Arc::new(AtomicU32::new(0)),
   tube_ids: Arc::new(Mutex::new(tube_ids)),
   store: Arc::new(Mutex::new(HashMap::<TubeID, Tube>::new())),
  }
 }

 pub async fn tube_id_for_name(&mut self, name: &String) -> TubeID {
  let mut tube_ids = self.tube_ids.lock().await;
  match tube_ids.get(name) {
   None => {
    let id = tube_ids.len() + 1 as TubeID;
    tube_ids.insert(name.clone(), id);
    id
   }
   Some(id) => *id,
  }
 }

 pub async fn push_ready(&mut self, mut job: Job) -> u32 {
  let job_id = match job.id {
   None => self.job_counter.fetch_add(1, AcqRel) as u32,
   Some(id) => id,
  };
  job.id = Some(job_id);

  self
   .with_tube(job.tube, |tube: &mut Tube| {
    tube.push(job);
   })
   .await;
  job_id
 }

 pub async fn push_delayed(&mut self, mut job: Job, until: std::time::Instant) -> u32 {
  let job_id = match job.id {
   None => self.job_counter.fetch_add(1, AcqRel) as u32,
   Some(id) => id,
  };
  job.id = Some(job_id);

  self
   .with_tube(job.tube, |tube: &mut Tube| {
    tube.push_delayed(job, until);
   })
   .await;

  job_id
 }

 // pop_ready acquires a lock on *all* tubes and iterates over all named
 // tubes to pop and return the job with the lowest priority. This job will
 // then no longer be owned by the store and will have to be returned (=
 // released).
 pub async fn pop_ready(&mut self, tubes: &HashSet<TubeID>) -> Option<Job> {
  let mut store = self.store.lock().await;

  let best = tubes.iter().fold(None, |best: Option<(&TubeID, u32)>, cur: &TubeID| -> Option<(&TubeID, u32)> {
   match store.get(cur) {
    None => best,
    Some(tube) => match (best, tube.peek()) {
     (None, None) => best,
     (None, Some(job)) => Some((cur, job.pri)),
     (Some((_, _)), None) => best,
     (Some((_, pri)), Some(job)) => {
      if job.pri < pri {
       Some((cur, job.pri))
      } else {
       best
      }
     }
    },
   }
  });

  match best {
   None => None,
   Some((tube_id, _)) => {
    let tube = store.get_mut(tube_id).unwrap();
    Some(tube.pop().unwrap())
   }
  }
 }

 // with_tube calls the function with the specified id. Using a closure is
 // easier than returning a borrow/reference to the indicated tube.
 async fn with_tube<F>(&mut self, id: TubeID, f: F)
 where F: FnOnce(&mut Tube) {
  let mut store = self.store.lock().await;

  let tube = match store.get_mut(&id) {
   None => {
    store.insert(id, Tube::new());
    store.get_mut(&id).unwrap()
   }
   Some(tube) => tube,
  };

  tube.delayed_maintenance();
  f(tube);
 }
}

impl Deref for JobStore {
 type Target = Arc<Mutex<HashMap<TubeID, Tube>>>;

 fn deref(&self) -> &Self::Target {
  &self.store
 }
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
