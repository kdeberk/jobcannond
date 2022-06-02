use std::cmp::Ordering;
use std::time;

#[derive(PartialEq, Eq)]
pub struct Job {
    pub id: u32,
    pub pri: u32,
    pub ttr: u32,
    pub data: Vec<u8>,
    pub tube: String,
}

#[derive(PartialEq, Eq)]
pub struct ReadyJob {
    pub job: Job,
}

impl Ord for ReadyJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.job.pri.cmp(&other.job.pri)
    }
}

impl PartialOrd for ReadyJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.job.pri.partial_cmp(&other.job.pri)
    }
}

#[derive(PartialEq, Eq)]
pub struct DelayedJob {
    pub until: time::Instant,
    pub job: Job,
}

impl Ord for DelayedJob {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.until == other.until {
            self.job.pri.cmp(&other.job.pri)
        } else {
            self.until.cmp(&other.until)
        }
    }
}

impl PartialOrd for DelayedJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.until.partial_cmp(&other.until)
    }
}

#[derive(PartialEq, Eq)]
pub struct ReservedJob {
    pub until: time::Instant,
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
