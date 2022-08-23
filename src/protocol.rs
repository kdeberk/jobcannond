#![allow(dead_code)]
#![allow(unused)]

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncBufRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

const MAX_TUBE_NAME_SIZE: usize = 200;

#[derive(Debug)]
pub enum Command {
 Bury {
  id: u32,
  pri: u32,
 },
 Delete {
  id: u32,
 },
 Ignore {
  tube: String,
 },
 Kick {
  count: u32,
 },
 KickJob {
  id: u32,
 },
 ListTubeUsed,
 ListTubes,
 ListTubesWatched,
 PauseTube {
  tube: String,
  delay: u32,
 },
 Peek {
  id: u32,
 },
 PeekBuried,
 PeekDelayed,
 PeekReady,
 Put {
  pri: u32,
  delay: u32,
  ttr: u32,
  data: Vec<u8>,
 },
 Quit,
 Release {
  id: u32,
  pri: u32,
  delay: u32,
 },
 Reserve,
 ReserveJob {
  id: u32,
 },
 ReserveWithTimeout {
  seconds: u32,
 },
 Stats,
 StatsJob {
  id: u32,
 },
 StatsTube {
  tube: String,
 },
 Touch {
  id: u32,
 },
 Use {
  tube: String,
 },
 Watch {
  tube: String,
 },
}

#[derive(Debug)]
pub enum Response {
 Buried {
  id: u32,
 },
 Deleted,
 Found {
  id: u32,
  data: Vec<u8>,
 },
 Inserted {
  id: u32,
 },
 Kicked {
  count: Option<u32>,
 },
 Paused,
 Released,
 Reserved {
  id: u32,
  data: Vec<u8>,
 },
 TimedOut,
 Touched,
 Using {
  tube: String,
 },
 Watching {
  count: u32,
 },
 YamlData {
  data: Vec<u8>,
 },
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
 UnexpectedInput {
  reason: String,
 },
}

macro_rules! async_write {
 ($dst: expr, $fmt: expr) => {{
  use std::io::Write;

  let mut buf: Vec<u8> = vec![];
  write!(buf, $fmt);
  tokio::io::AsyncWriteExt::write_all(&mut $dst, &buf).await?;
 }};
 ($dst: expr, $fmt: expr, $($arg: tt)*) => {{
  use std::io::Write;

  let mut buf: Vec<u8> = vec![];
  write!(buf, $fmt, $( $arg )*);
  tokio::io::AsyncWriteExt::write_all(&mut $dst, &buf).await?;
 }};
}

pub struct Protocol<RW> {
 stream: Stream<RW>,
}

impl<RW> Protocol<RW>
where RW: tokio::io::AsyncRead + tokio::io::AsyncWrite
{
 pub fn new(stream: RW) -> Self {
  Protocol { stream: Stream::new(stream) }
 }

 pub async fn read_command(&mut self) -> Result<Command, StreamError> {
  use Command::*;

  let command = match self.stream.read_word().await?.as_str() {
   "bury" => {
    let id = self.stream.read_u32().await?;
    let pri = self.stream.read_u32().await?;
    Ok(Bury { id, pri })
   }
   "delete" => Ok(Delete { id: self.stream.read_u32().await? }),
   "ignore" => Ok(Ignore { tube: self.stream.read_word().await? }),
   "kick" => Ok(Kick { count: self.stream.read_u32().await? }),
   "kick-job" => Ok(KickJob { id: self.stream.read_u32().await? }),
   "list-tubes-used" => Ok(ListTubeUsed),
   "list-tubes" => Ok(ListTubes),
   "list-tubes-watched" => Ok(ListTubesWatched),
   "pause-tube" => {
    let tube = self.stream.read_word().await?;
    let delay = self.stream.read_u32().await?;
    Ok(PauseTube { tube, delay })
   }
   "peek" => Ok(Peek { id: self.stream.read_u32().await? }),
   "peek-buried" => Ok(PeekBuried),
   "peek-ready" => Ok(PeekReady),
   "put" => {
    let pri = self.stream.read_u32().await?;
    let delay = self.stream.read_u32().await?;
    let ttr = self.stream.read_u32().await?;
    let bytes = self.stream.read_u32().await?;
    _ = self.stream.read_crnl().await?;
    let data = self.stream.read_data(bytes as usize).await?;
    Ok(Put { pri, delay, ttr, data })
   }
   "quit" => Ok(Quit),
   "release" => {
    let id = self.stream.read_u32().await?;
    let pri = self.stream.read_u32().await?;
    let delay = self.stream.read_u32().await?;
    Ok(Release { id, pri, delay })
   }
   "reserve" => Ok(Reserve {}),
   "reserve-job" => Ok(ReserveJob { id: self.stream.read_u32().await? }),
   "reserve-with-timeout" => Ok(ReserveWithTimeout { seconds: self.stream.read_u32().await? }),
   "stats" => Ok(Stats {}),
   "stats-job" => Ok(StatsJob { id: self.stream.read_u32().await? }),
   "stats-tube" => Ok(StatsTube { tube: self.stream.read_word().await? }),
   "touch" => Ok(Touch { id: self.stream.read_u32().await? }),
   "use" => Ok(Use { tube: self.stream.read_word().await? }),
   "watch" => Ok(Watch { tube: self.stream.read_word().await? }),
   _ => Err(StreamError::UnexpectedInput { reason: "Unknown command".into() }),
  };

  if let Ok(_) = command {
   self.stream.read_crnl().await?;
  };
  command
 }

 pub async fn read_response(&mut self) -> Result<Response, StreamError> {
  use Response::*;

  let response = match self.stream.read_word().await?.as_str() {
   "BURIED" => Ok(Buried { id: self.stream.read_u32().await? }),
   "DELETED" => Ok(Deleted),
   "FOUND" => {
    let id = self.stream.read_u32().await?;
    let bytes = self.stream.read_u32().await?;
    let data = self.stream.read_data(bytes as usize).await?;
    _ = self.stream.read_crnl().await?;
    Ok(Found { data, id })
   }
   "INSERTED" => Ok(Inserted { id: self.stream.read_u32().await? }),
   "KICKED" => {
    let arg = self.stream.read_word().await?;
    if arg == "" {
     Ok(Kicked { count: None })
    } else {
     match arg.parse::<u32>() {
      Ok(count) => Ok(Kicked { count: Some(count) }),
      Err(_) => Err(StreamError::UnexpectedInput { reason: format!("not a number {:?}", arg).into() }),
     }
    }
   }
   "PAUSED" => Ok(Paused),
   "RELEASED" => Ok(Released),
   "RESERVED" => {
    let id = self.stream.read_u32().await?;
    let bytes = self.stream.read_u32().await?;
    self.stream.read_crnl().await?;
    let data = self.stream.read_data(bytes as usize).await?;
    Ok(Reserved { id, data })
   }
   "TIMED_OUT" => Ok(TimedOut),
   "TOUCHED" => Ok(Touched),
   "WATCHING" => Ok(Watching { count: self.stream.read_u32().await? }),
   "OK" => {
    let bytes = self.stream.read_u32().await?;
    self.stream.read_crnl().await?;
    let data = self.stream.read_data(bytes as usize).await?;
    Ok(YamlData { data })
   }
   "BAD_FORMAT" => Ok(BadFormat),
   "DRAINING" => Ok(Draining),
   "EXPECTED_CRLF" => Ok(ExpectedCRLF),
   "INTERNAL_ERROR" => Ok(InternalError),
   "JOB_TOO_BIG" => Ok(JobTooBig),
   "NOT_FOUND" => Ok(NotFound),
   "NOT_IGNORED" => Ok(NotIgnored),
   "OUT_OF_MEMORY" => Ok(OutOfMemory),
   "UNKNOWN_COMMAND" => Ok(UnknownCommand),
   _ => Err(StreamError::UnexpectedInput { reason: "Unknown response".into() }),
  };

  if let Ok(_) = response {
   self.stream.read_crnl().await?;
  }
  response
 }

 pub async fn write_command(&mut self, c: Command) -> Result<(), StreamError> {
  use Command::*;

  match c {
   Bury { id, pri } => async_write!(self.stream, "bury {} {}", id, pri),
   Delete { id } => async_write!(self.stream, "delete {}", id),
   Ignore { tube } => async_write!(self.stream, "ignore {}", tube),
   Kick { count } => async_write!(self.stream, "kick {}", count),
   KickJob { id } => async_write!(self.stream, "kick-job {}", id),
   ListTubeUsed => async_write!(self.stream, "list-tube-used"),
   ListTubes => async_write!(self.stream, "list-tubes"),
   ListTubesWatched => async_write!(self.stream, "list-tubes-watched"),
   PauseTube { tube, delay } => async_write!(self.stream, "pause-tube {} {}", tube, delay),
   Peek { id } => async_write!(self.stream, "peek {}", id),
   PeekBuried => async_write!(self.stream, "peek-buried"),
   PeekDelayed => async_write!(self.stream, "peek-delayed"),
   PeekReady => async_write!(self.stream, "peek-ready"),
   Put { pri, delay, ttr, data } => {
    async_write!(self.stream, "put {} {} {}\r\n", pri, delay, ttr);
    self.stream.write_all(&data).await;
   }
   Quit => async_write!(self.stream, "quit"),
   Release { id, pri, delay } => async_write!(self.stream, "release {} {} {}", id, pri, delay),
   Reserve => async_write!(self.stream, "reserve"),
   ReserveJob { id } => async_write!(self.stream, "reserve-job {}", id),
   ReserveWithTimeout { seconds } => async_write!(self.stream, "reserve-with-timeout {}", seconds),
   Stats => async_write!(self.stream, "stats"),
   StatsJob { id } => async_write!(self.stream, "stats-job {}", id),
   StatsTube { tube } => async_write!(self.stream, "stats-tube {}", tube),
   Touch { id } => async_write!(self.stream, "touch {}", id),
   Use { tube } => async_write!(self.stream, "use {}", tube),
   Watch { tube } => async_write!(self.stream, "watch {}", tube),
  };

  async_write!(self.stream, "\r\n");
  Ok(())
 }

 pub async fn write_response(&mut self, r: Response) -> Result<(), StreamError> {
  use Response::*;

  match r {
   Buried { id } => async_write!(self.stream, "BURIED {}", id),
   Deleted => async_write!(self.stream, "DELETED"),
   Found { id, data } => {
    async_write!(self.stream, "FOUND {} {}\r\n", id, data.len());
    self.stream.write_all(&data).await?;
   }
   Inserted { id } => async_write!(self.stream, "INSERTED {}", id),
   Kicked { count: Some(count) } => async_write!(self.stream, "KICKED {}", count),
   Kicked { count: None } => async_write!(self.stream, "KICKED"),
   Paused => async_write!(self.stream, "PAUSED"),
   Released => async_write!(self.stream, "RELEASED"),
   Reserved { id, data } => {
    async_write!(self.stream, "RESERVED {} {}\r\n", id, data.len());
    self.stream.write_all(&data).await?;
   }
   TimedOut => async_write!(self.stream, "TIMED_OUT"),
   Touched => async_write!(self.stream, "TOUCHED"),
   Using { tube } => async_write!(self.stream, "USING {}", tube),
   Watching { count } => async_write!(self.stream, "WATCHING {}", count),
   YamlData { data } => {
    async_write!(self.stream, "OK {}\r\n", data.len());
    self.stream.write_all(&data).await?;
   }
   // Error conditions
   BadFormat => async_write!(self.stream, "BAD_FORMAT"),
   Draining => async_write!(self.stream, "DRAINING"),
   ExpectedCRLF => async_write!(self.stream, "EXPECTED_CRLF"),
   InternalError => async_write!(self.stream, "INTERNAL_ERROR"),
   JobTooBig => async_write!(self.stream, "JOB_TOO_BIG"),
   NotFound => async_write!(self.stream, "NOT_FOUND"),
   NotIgnored => async_write!(self.stream, "NOT_IGNORED"),
   OutOfMemory => async_write!(self.stream, "OUT_OF_MEMORY"),
   UnknownCommand => async_write!(self.stream, "UNKNOWN_COMMAND"),
  };

  async_write!(self.stream, "\r\n");
  Ok(())
 }
}

// Stream wraps the readable/writeable stream and offers primitive read
// operators.
struct Stream<RW> {
 reader: tokio::io::BufReader<tokio::io::ReadHalf<RW>>,
 writer: tokio::io::WriteHalf<RW>,
}

impl<RW> Stream<RW>
where RW: tokio::io::AsyncRead + tokio::io::AsyncWrite
{
 pub fn new(stream: RW) -> Self {
  let (reader, writer) = tokio::io::split(stream);

  Stream { reader: tokio::io::BufReader::new(reader), writer }
 }

 async fn read_word(&mut self) -> Result<String, StreamError> {
  let mut buf = vec![];
  read_word(&mut self.reader, &mut buf).await?;

  match String::from_utf8(buf) {
   Ok(word) => Ok(word.trim().to_string()),
   Err(err) => Err(StreamError::UnexpectedInput { reason: err.to_string() }),
  }
 }

 async fn read_u32(&mut self) -> Result<u32, StreamError> {
  let word = self.read_word().await?;

  match word.parse::<u32>() {
   Ok(n) => Ok(n),
   Err(_) => Err(StreamError::UnexpectedInput { reason: format!("not a number {:?}", &word).into() }),
  }
 }

 async fn read_data(&mut self, count: usize) -> Result<Vec<u8>, StreamError> {
  let mut buf = vec![0u8; count];
  self.reader.read_exact(&mut buf).await?;

  Ok(buf)
 }

 async fn read_crnl(&mut self) -> Result<(), StreamError> {
  let mut buf = [0u8; 2];
  self.reader.read_exact(&mut buf).await?;

  match (buf[0] as char, buf[1] as char) {
   ('\r', '\n') => Ok(()),
   _ => Err(StreamError::UnexpectedInput { reason: format!("expected CRNL, got {:?}", buf) }),
  }
 }

 async fn write<S: AsRef<str>>(&mut self, s: S) -> Result<(), StreamError> {
  self.writer.write_all(s.as_ref().as_bytes()).await?;
  Ok(())
 }
}

// TODO: Check if we can simply ref so that the compiler can replace Stream with self.writer.
impl<RW> tokio::io::AsyncWrite for Stream<RW>
where RW: tokio::io::AsyncRead + tokio::io::AsyncWrite
{
 fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
  Pin::new(&mut self.writer).poll_write(cx, buf)
 }

 fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
  Pin::new(&mut self.writer).poll_flush(cx)
 }

 fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
  Pin::new(&mut self.writer).poll_shutdown(cx)
 }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct WordReader<'a, R: ?Sized> {
 reader: &'a mut R,
 buf: &'a mut Vec<u8>,
 // The number of bytes appended to buf. This can be less than buf.len() if
 // the buffer was not empty when the operation was started.
 read: usize,
 read_non_ws: bool,
}

fn read_word<'a, R>(reader: &'a mut R, buf: &'a mut Vec<u8>) -> WordReader<'a, R>
where R: AsyncBufRead + ?Sized + Unpin {
 WordReader { reader, buf, read: 0, read_non_ws: false }
}

const SPACE: u8 = ' ' as u8;
const CARRIAGE_RETURN: u8 = '\r' as u8;
const LINE_FEED: u8 = '\n' as u8;

impl<R: AsyncBufRead + ?Sized + Unpin> std::future::Future for WordReader<'_, R> {
 type Output = std::io::Result<usize>;

 fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
  let Self { reader, buf, read, read_non_ws } = &mut *self;

  let mut reader = std::pin::Pin::new(reader);
  let mut done = false;

  loop {
   let mut used: usize = 0;
   let available = futures::ready!(reader.as_mut().poll_fill_buf(cx))?;

   for idx in 0..available.len() {
    match available[idx] {
     SPACE | CARRIAGE_RETURN if *read_non_ws => {
      used = idx;
      done = true;
      break;
     }
     SPACE | CARRIAGE_RETURN | LINE_FEED if *read_non_ws => (),
     _ if !*read_non_ws => {
      *read_non_ws = true;
     }
     _ => (),
    }
   }

   reader.as_mut().consume(used);
   *read += used;
   if done || used == 0 {
    return std::task::Poll::Ready(Ok(std::mem::replace(read, 0)));
   }
  }
 }
}
