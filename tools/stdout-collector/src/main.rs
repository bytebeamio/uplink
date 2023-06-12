use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::sink::SinkExt;
use log::error;
use serde::Serialize;
use serde_json::{json, Value};
use structopt::StructOpt;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Lines codec error {0}")]
    Codec(#[from] tokio_util::codec::LinesCodecError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

#[derive(StructOpt, Debug)]
#[structopt(name = "stdout collector", about = "collect stdout lines and push to uplink")]
pub struct CommandLine {
    /// uplink port
    #[structopt(short = "p", help = "uplink port")]
    pub port: u16,
    /// stream name
    #[structopt(short = "s", help = "stream to push lines onto")]
    pub stream: String,
}

#[derive(Debug, Serialize)]
struct LogEntry {
 pub line: String,
 pub level: String,
 #[serde(skip)]
 pub timestamp: u64,
 pub message: String,
}

impl LogEntry {
    fn parse(line: String) -> Self {
        Self {
            line,
            level: "".to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
            message: "".to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Payload {
    pub stream: String,
    #[serde(skip)]
    pub device_id: Option<String>,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

async fn reader(stream_name: &str, mut tx: Framed<TcpStream, LinesCodec>) -> Result<(), Error> {
    let stdin = stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut sequence = 0;

    loop {
        match lines.next_line().await? {
            Some(l) => {
                sequence += 1;
                let log_entry = LogEntry::parse(l);
                let data = Payload {
                    stream: stream_name.to_owned(),
                    device_id: None,
                    sequence,
                    timestamp: log_entry.timestamp,
                    payload: json!(log_entry),
                };
                let payload = serde_json::to_string(&data)?;
                tx.send(payload).await?;
            }
            None => return Ok(()),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let commandline: CommandLine = StructOpt::from_args();
    let addr = format!("localhost:{}", commandline.port);
    let stream: TcpStream = TcpStream::connect(addr).await?;
    let data_tx = Framed::new(stream, LinesCodec::new());

    reader(&commandline.stream, data_tx).await?;

    Ok(())
}
