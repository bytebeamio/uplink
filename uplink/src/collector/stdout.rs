use regex::Regex;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};

use serde::Serialize;
use serde_json::json;

use crate::base::bridge::{BridgeTx, Payload};
use crate::base::{clock, StdoutConfig};

lazy_static::lazy_static! {
    pub static ref COLORS_RE: Regex = Regex::new(r#"\u{1b}\[[0-9;]*m"#).unwrap();
}

#[derive(Debug, Serialize)]
struct LogEntry {
    pub line: String,
    pub tag: String,
    pub level: String,
    pub log_timestamp: String,
    pub message: String,
}

impl LogEntry {
    // NOTE: expects log lines to contain information in the format "{log_timestamp} {level} {tag}: {message}"
    fn parse(line: String) -> Option<Self> {
        // NOTE: remove any tty color escape characters
        let line = COLORS_RE.replace_all(&line, "").trim().to_string();
        let mut tokens = line.split(' ');
        let log_timestamp = tokens.next()?.to_string();
        let level = tokens.next()?.to_string();
        let tag = tokens.next()?.trim_end_matches(':').to_string();
        let message = tokens.fold("".to_string(), |acc, s| acc + " " + s).trim().to_string();

        Some(Self { line, level, tag, log_timestamp, message })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

pub struct Stdout {
    config: StdoutConfig,
    tx: BridgeTx,
}

impl Stdout {
    pub fn new(config: StdoutConfig, tx: BridgeTx) -> Self {
        Self { config, tx }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(&self) -> Result<(), Error> {
        let stdin = stdin();
        let mut lines = BufReader::new(stdin).lines();
        let mut sequence = 0;

        loop {
            match lines.next_line().await? {
                Some(l) => {
                    sequence += 1;
                    if let Some(log_entry) = LogEntry::parse(l) {
                        let payload = Payload {
                            stream: self.config.stream_name.to_owned(),
                            device_id: None,
                            sequence,
                            timestamp: clock() as u64,
                            payload: json!(log_entry),
                        };
                        self.tx.send_payload(payload).await;
                    }
                }
                None => return Ok(()),
            }
        }
    }
}
