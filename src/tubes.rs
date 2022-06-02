use crate::jobs::{ReadyJob, DelayedJob, Job};
use crate::ordered_set::OrderedSet;
use std::collections::{HashMap, BinaryHeap};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::AcqRel;
use std::time;
use std::ops::Deref;

pub struct Tube {
    pub id: usize,
    pub name: String,
    pub ready: BinaryHeap<ReadyJob>,
    pub delayed: BinaryHeap<DelayedJob>,
}

// TODO:
// - To prevent cloning of names, convert tube names to ids.
//   Use these ids everywhere so that we don't clone all over the place.

impl Tube {
    pub fn new(id: usize, name: String) -> Self {
        return Tube{
            id, name,
            ready: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
        }
    }

    pub fn delayed_maintenance(&mut self) {
        loop {
            if self.delayed.is_empty() {
                break
            }

            let job = self.delayed.peek().unwrap();
            if job.until < time::Instant::now() {
                break
            }

            let job = self.delayed.pop().unwrap().job;
            self.ready.push(ReadyJob { job });
        }
    }
}

pub enum TubeEntry {
    Tube(Tube),
    Taken,
}

#[derive(Clone)]
pub struct TubeStore {
    job_counter: Arc<AtomicU32>,
    wake_on_return: Arc<Condvar>,
    store: Arc<Mutex<HashMap<String, TubeEntry>>>,
}

impl TubeStore {
    pub fn new() -> Self {
        Self {
            job_counter: Arc::new(AtomicU32::new(0)),
            wake_on_return: Arc::new(Condvar::new()),
            store: Arc::new(Mutex::new(HashMap::<String, TubeEntry>::new())),
        }
    }

    fn next_job_id(&mut self) -> u32 {
        self.job_counter.fetch_add(1, AcqRel) as u32
    }

    fn take_tubes(&mut self, watched: &OrderedSet<String>) -> Vec<Tube> {
        watched
            .iter()
            .filter_map(|name| {
                self.take_tube(name)
                    .and_then(|mut tube| {
                        tube.delayed_maintenance();

                        if tube.ready.is_empty() {
                            self.return_tube(tube);
                            None
                        } else {
                            Some(tube)
                        }
                    })
            }).collect()
    }

    fn take_tube(&mut self, name: &String) -> Option<Tube> {
        loop {
            let mut store = self.store.lock().unwrap();

            match store.remove_entry(name) {
                Some((k, TubeEntry::Tube(tube))) => {
                    store.insert(k, TubeEntry::Taken);
                    return Some(tube);
                },
                Some((k, TubeEntry::Taken)) => {
                    store.insert(k, TubeEntry::Taken);
                },
                None => {
                    return None;
                }
            }

            // Wait for a return_tube call to wake this thread.
            self.wake_on_return.wait(store).unwrap();
        }
    }

    fn return_tube(&mut self, tube: Tube) {
        let mut store = self.store.lock().unwrap();
        let name = tube.name.clone();

        store.insert(name, TubeEntry::Tube(tube));
        self.wake_on_return.notify_all();
    }

    fn with_tube<F>(&mut self, name: &String, f: F)
    where F: FnOnce(&mut Tube) {
        loop {
            let mut store = self.store.lock().unwrap();

            match store.entry(name.clone())
                       .or_insert(TubeEntry::Tube(Tube::new(0, name.clone()))) {
                TubeEntry::Taken => {
                    self.wake_on_return.wait(store).unwrap();
                },
                TubeEntry::Tube(t) => {
                    f(t);
                    return
                },
            }
        }
    }
}

impl Deref for TubeStore {
    type Target = Arc<Mutex<HashMap<String, TubeEntry>>>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

pub struct TubeView {
    // Each view stores its watched tubes in an ordered set. This is a
    // solution to the dining philosophers problem.
    watched: OrderedSet<String>,
    store: TubeStore,
}

impl TubeView {
    pub fn new(store: TubeStore) -> Self {
        Self {
            watched: OrderedSet::new(),
            store,
        }
    }

    pub fn next_job_id(&mut self) -> u32 {
        self.store.next_job_id()
    }

    pub fn watch(&mut self, tube: String) -> usize {
        self.watched.add(tube);
        self.watched.len()
    }

    pub fn ignore(&mut self, tube: &String) -> usize {
        self.watched.remove(&tube);
        self.watched.len()
    }

    pub fn push_ready(&mut self, job: Job) {
        let tube = job.tube.clone();
        self.store.with_tube(&tube, |tube: &mut Tube| {
            tube.ready.push( ReadyJob { job });
        });
    }

    pub fn push_delayed(&mut self, job: Job, until: time::Instant) {
        let tube = job.tube.clone();
        self.store.with_tube(&tube, |tube: &mut Tube| {
            tube.delayed.push( DelayedJob { job, until });
        });
    }

    pub fn push_buried(&mut self, job: Job) {
        let tube = job.tube.clone();
        self.store.with_tube(&tube, |tube: &mut Tube| {
        });
    }

    // TODO: don't return None, block until job was added.
    pub fn pop_ready(&mut self) -> Option<Job> {
        let tubes = self.store.take_tubes(&self.watched);
        if tubes.is_empty() {
            return None
        }

        let (mut tube, _) = tubes
            .into_iter()
            .map(|tube| {
                let pri = tube.ready.peek().unwrap().job.pri;
                (tube, pri)
            })
            .reduce(|a, b| {
                let (min, max) = if a.1 < b.1 { (a, b) } else { (b, a) };
                self.store.return_tube(max.0);
                min
            }).unwrap();

        let ready = tube.ready.pop().unwrap();
        self.store.return_tube(tube);
        Some(ready.job)
    }
}
