use crate::jobs::{Job, ReservedJob};
use crate::ordered_set::OrderedSet;
use crate::protocol::{Command, Protocol, Response, StreamError};
use crate::tubes::{TubeStore, TubeView};
use std::collections::{BinaryHeap, HashMap};
use std::time;

#[derive(thiserror::Error, Debug)]
pub enum SessionError {
    #[error("{:?}", .0)]
    Stream(#[from] StreamError),
}

pub struct Session {
    protocol: Protocol<std::net::TcpStream>,

    tube: String,
    view: TubeView,

    by_time: BinaryHeap<ReservedJob>,
    reserved: HashMap<u32, Job>,
}

// TODO:
// - Session keeps track of expiring reserved jobs.

const MIN_TTR: u32 = 1;

impl Session {
    pub fn new(socket: std::net::TcpStream, store: TubeStore) -> Self {
        Session {
            protocol: Protocol::new(socket),

            tube: "default".into(),
            view: TubeView::new(store),

            by_time: BinaryHeap::new(),
            reserved: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), SessionError> {
        loop {
            let cmd = self.read_command().await?;
            let response = self.handle(cmd).await?;
            self.write_response(response)?;
        }
    }

    async fn read_command(&mut self) -> Result<Command, SessionError> {
        Ok(self.protocol.read_command()?)
    }

    async fn handle(&mut self, command: Command) -> Result<Response, SessionError> {
        match command {
            Command::Put {
                pri,
                delay,
                mut ttr,
                data,
            } => {
                let job_id = self.view.next_job_id();

                if (ttr == 0) {
                    ttr = MIN_TTR;
                }

                let job = Job {
                    id: job_id,
                    pri,
                    ttr,
                    data,
                    tube: self.tube.clone(),
                };

                if 0 < delay {
                    let until = time::Instant::now() + time::Duration::new(delay as u64, 0);
                    self.view.push_delayed(job, until);
                } else {
                    self.view.push_ready(job);
                }
                Ok(Response::Inserted { id: job_id })
            }
            Command::Use { tube } => {
                self.tube = tube;
                Ok(Response::Inserted { id: 1 })
            }
            Command::Reserve {} => match self.view.pop_ready().await {
                Some(job) => {
                    let (id, ttr, data) = (job.id, job.ttr, job.data.clone());
                    self.by_time.push(ReservedJob {
                        id,
                        until: time::Instant::now() + time::Duration::new(ttr as u64, 0),
                    });
                    Ok(Response::Reserved { id, data })
                }
                None => Ok(Response::NotFound),
            },
            Command::ReserveWithTimeout { timeout } => Ok(Response::NotFound), // TODO
            Command::Delete { id } => match self.reserved.remove(&id) {
                Some(_) => Ok(Response::Deleted {}),
                None => Ok(Response::NotFound),
            },
            Command::Release { id, pri, delay } => match self.reserved.remove(&id) {
                Some(job) => {
                    if 0 < delay {
                        let until = time::Instant::now() + time::Duration::new(delay as u64, 0);
                        self.view.push_delayed(job, until);
                    } else {
                        self.view.push_ready(job);
                    }
                    Ok(Response::Released)
                }
                None => Ok(Response::NotFound),
            },
            Command::Bury { id, pri } => Ok(Response::NotFound), // TODO
            Command::Touch { id } => Ok(Response::NotFound),     // TODO
            Command::Watch { tube } => {
                let count = self.view.watch(tube);
                Ok(Response::Watching {
                    count: count as u32,
                })
            }
            Command::Ignore { tube } => {
                let count = self.view.ignore(&tube);
                Ok(Response::Watching {
                    count: count as u32,
                })
            }
            _ => todo!(),
        }
    }

    fn write_response(&mut self, resp: Response) -> Result<(), SessionError> {
        self.protocol.write_response(resp)?;
        Ok(())
    }
}
