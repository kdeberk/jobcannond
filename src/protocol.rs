
// TODO:
// - read job data
// - support missing responses
// - support missing commands

const MAX_TUBE_NAME_SIZE:usize = 200;

#[derive(Debug)]
pub enum Command {
    Bury { id: u32, pri: u32 },
    Delete { id: u32 },
    Ignore { tube: String },
    Kick { count: u32 },
    KickJob { id: u32 },
    ListTubeUsed,
    ListTubes,
    ListTubesWatched,
    PauseTube { tube: String, delay: u32 },
    Peek { id: u32 },
    PeekBuried,
    PeekDelayed,
    PeekReady,
    Put { pri: u32, delay: u32, ttr: u32, data: Vec<u8>},
    Quit,
    Release { id: u32, pri: u32, delay: u32 },
    Reserve,
    ReserveJob { id: u32 },
    ReserveWithTimeout { timeout: u32 },
    Stats,
    StatsJob { id: u32 },
    StatsTube { tube: String },
    Touch { id: u32 },
    Use { tube: String },
    Watch { tube: String },
}

#[derive(Debug)]
pub enum Response {
    Buried { id: u32 },
    Deleted,
    FoundJob { id: u32, data: Vec<u8> },
    Inserted { id: u32 },
    JobTimedOut { id: u32 },
    Kicked { count: u32 },
    KickedOne,
    Paused,
    Released,
    Reserved { id: u32, data: Vec<u8> },
    TimedOut,
    Touched,
    Using { tube: String },
    Watching { count: u32 },
    YamlData { data: Vec<u8> },
    // Following are used for communicate error conditions.
    BadFormat,
    Draining,
    ExpectedCRLF,
    InternalError,
    JobTooBig,
    NotFound,
    NotIgnored,
    OutOfMemory,
    UnknownCommand,
}

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("connection was closed")]
    EOF,
    #[error("error reading from client")]
    Read(#[from] std::io::Error),
    #[error("received invalid input from client: {reason}")]
    InvalidInput { reason: String },
}

pub struct Protocol<RW> {
    helper: ReadHelper<RW>,
    stream: RW,
}

impl <RW> Protocol<RW>
where RW: std::io::Read + std::io::Write {
    pub fn new(stream: RW) -> Self {
        Protocol{helper: ReadHelper::new(), stream}
    }

    pub fn read_command(&mut self) -> Result<Command, StreamError> {
        match self.helper.read_word(&mut self.stream)?.as_str() {
            "put" => {
                let pri = self.helper.read_u32(&mut self.stream)?;
                let delay = self.helper.read_u32(&mut self.stream)?;
                let ttr = self.helper.read_u32(&mut self.stream)?;
                let bytes = self.helper.read_u32(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                let data = self.helper.read_data(bytes as usize)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Put{pri, delay, ttr, data})
            },
            "use" => {
                let tube = self.helper.read_word(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Use{tube})
            },
            "reserve" => {
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Reserve{})
            },
            "reserve-with-timeout" => {
                let timeout = self.helper.read_u32(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::ReserveWithTimeout {timeout})
            },
            "delete" => {
                let id = self.helper.read_u32(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Delete{id})
            }
            "release" => {
                let id = self.helper.read_u32(&mut self.stream)?;
                let pri = self.helper.read_u32(&mut self.stream)?;
                let delay = self.helper.read_u32(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Release{id, pri, delay})
            },
            "bury" => {
                let id = self.helper.read_u32(&mut self.stream)?;
                let pri = self.helper.read_u32(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Bury{id, pri})
            },
            "touch" => {
                let id = self.helper.read_u32(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Touch{id})
            },
            "watch" => {
                let tube = self.helper.read_word(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Watch{tube})
            },
            "ignore" => {
                let tube = self.helper.read_word(&mut self.stream)?;
                _ = self.helper.read_crnl(&mut self.stream)?;
                Ok(Command::Ignore{tube})
            },
            _ => Err(StreamError::InvalidInput{reason: "Unknown command".into()}),
        }
    }

    pub fn write_response(&mut self, r: Response) -> Result<(), StreamError> {
        match r {
            Response::Buried{id} => write!(self.stream, "BURIED {}\r\n", id)?,
            Response::Deleted => write!(self.stream, "DELETED\r\n")?,
            Response::FoundJob{id, data} => {
                write!(self.stream, "FOUND {} {}\r\n", id, data.len())?;
                self.stream.write_all(&data)?;
                write!(self.stream, "\r\n")?;
            }
            Response::Inserted{id} => write!(self.stream, "INSERTED {}\r\n", id)?,
            Response::JobTimedOut{id} => write!(self.stream, "TIMED_OUT {}\r\n", id)?,
            Response::Kicked{count} => write!(self.stream, "KICKED {}\r\n", count)?,
            Response::KickedOne => write!(self.stream, "KICKED\r\n")?,
            Response::Paused => write!(self.stream, "PAUSED\r\n")?,
            Response::Released => write!(self.stream, "RELEASED\r\n")?,
            Response::Reserved{id, data} => {
                write!(self.stream, "RESERVED {} {}\r\n", id, data.len())?;
                self.stream.write_all(&data)?;
                write!(self.stream, "\r\n")?;
            },
            Response::TimedOut => write!(self.stream, "TIMED_OUT\r\n")?,
            Response::Touched => write!(self.stream, "TOUCHED\r\n")?,
            Response::Using{tube} => write!(self.stream, "USING {}\r\n", tube)?,
            Response::Watching{count} => write!(self.stream, "WATCHING {}\r\n", count)?,
            Response::YamlData{data} => {
                write!(self.stream, "OK {}\r\n", data.len())?;
                self.stream.write_all(&data)?;
                write!(self.stream, "\r\n")?;
            },
            // Error conditions
            Response::BadFormat => write!(self.stream, "BAD_FORMAT\r\n")?,
            Response::Draining => write!(self.stream, "DRAINING\r\n")?,
            Response::ExpectedCRLF => write!(self.stream, "EXPECTED_CRLF\r\n")?,
            Response::InternalError => write!(self.stream, "INTERNAL_ERROR\r\n")?,
            Response::JobTooBig => write!(self.stream, "JOB_TOO_BIG\r\n")?,
            Response::NotFound => write!(self.stream, "NOT_FOUND\r\n")?,
            Response::NotIgnored => write!(self.stream, "NOT_IGNORED\r\n")?,
            Response::OutOfMemory => write!(self.stream, "OUT_OF_MEMORY\r\n")?,
            Response::UnknownCommand => write!(self.stream, "UNKNOWN_COMMAND\r\n")?,
        }
        Ok(())
    }
}

const BUF_SIZE:usize = 1024;

struct ReadHelper<R> {
    buf: [u8; BUF_SIZE],
    start: usize,
    end: usize,
    _r: std::marker::PhantomData<R>,
}

impl<R> ReadHelper<R>
where R: std::io::Read {
    pub fn new() -> Self {
        ReadHelper{ buf: [0u8; BUF_SIZE], start: 0, end: 0, _r: std::marker::PhantomData }
    }

    pub fn read_word(&mut self, stream: &mut R) -> Result<String, StreamError> {
        let frag = self.read_until_whitespace(stream)?;

        match String::from_utf8(frag.to_vec()) {
            Ok(res) => Ok(res),
            Err(_) => Err(StreamError::InvalidInput{reason: "not valid utf8".into()}),
        }
    }

    pub fn read_u32(&mut self, stream: &mut R) -> Result<u32, StreamError> {
        let frag = self.read_until_whitespace(stream)?;

        match std::str::from_utf8(frag) {
            Ok(s) => match s.parse::<u32>() {
                Ok(n) => Ok(n),
                Err(_) => Err(StreamError::InvalidInput{reason: format!("not a number {:?}", frag).into()}),
            },
            Err(_) => Err(StreamError::InvalidInput{reason: "not valid utf8".into()}),
        }
    }

    pub fn read_crnl(&mut self, stream: &mut R) -> Result<(), StreamError> {
        if self.available_bytes() < 2 {
            self.read(stream)?;
        }

        if self.buf[self.start] == '\r' as u8 && self.buf[self.start+1] == '\n' as u8 {
            self.start += 2;
            Ok(())
        } else {
            Err(StreamError::InvalidInput {reason: "missing \r\n".into() })
        }
    }

    pub fn read_data(&mut self, _: usize) -> Result<Vec<u8>, StreamError> {
        // TODO:
        Ok(Vec::new())
    }

    fn read_until_whitespace(&mut self, stream: &mut R) -> Result<&[u8], StreamError>
    where R: std::io::Read {
        while ' ' == self.buf[self.start] as char {
            self.start += 1;
        }

        loop {
            for idx in self.start..self.end {
                match self.buf[idx] as char {
                    ' ' | '\r' | '\n' => {
                        let result = &self.buf[self.start..idx];
                        self.start = idx;
                        return Ok(result);
                    },
                    _ => (),
                }
            }

            if 0 == self.available_room() {
                return Err(StreamError::InvalidInput { reason: "read buffer full and no space found".into() })
            }
            self.read(stream)?;
        }
    }

    fn read(&mut self, stream: &mut R) -> Result<(), StreamError> {
        for idx in 0..self.start {
            self.buf[idx] = self.buf[idx + self.start]
        }
        self.end -= self.start;
        self.start = 0;

        let n = stream.read(&mut self.buf[self.end..])?;
        if 0 < n {
            self.end += n;
            Ok(())
        } else {
            Err(StreamError::EOF)
        }
    }

    fn available_bytes(&self) -> usize {
        self.end - self.start
    }

    fn available_room(&self) -> usize {
        BUF_SIZE - self.end
    }
}
