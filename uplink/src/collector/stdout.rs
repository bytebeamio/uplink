use regex::{Match, Regex};
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
    pub tag: Option<String>,
    pub level: Option<String>,
    pub log_timestamp: Option<String>,
    pub message: Option<String>,
}

impl LogEntry {
    // NOTE: expects log lines to contain information in the format "{log_timestamp} {level} {tag}: {message}"
    fn parse(line: String, log_template: &Regex) -> Option<Self> {
        let to_string = |x: Match| x.as_str().to_string();
        // NOTE: remove any tty color escape characters
        let line = COLORS_RE.replace_all(&line, "").trim().to_string();
        let captures = log_template.captures(&line)?;
        let log_timestamp = captures.name("log_timestamp").map(to_string);
        let level = captures.name("level").map(to_string);
        let tag = captures.name("tag").map(to_string);
        let message = captures.name("message").map(to_string);

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
    log_template: Regex,
}

impl Stdout {
    pub fn new(config: StdoutConfig, tx: BridgeTx) -> Self {
        let log_template = Regex::new(&config.log_template).unwrap();
        Self { config, tx, log_template }
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
                    if let Some(log_entry) = LogEntry::parse(l, &self.log_template) {
                        let payload = Payload {
                            stream: self.config.stream_name.to_owned(),
                            device_id: None,
                            sequence,
                            timestamp: clock() as u64,
                            payload: json!(dbg!(log_entry)),
                        };
                        self.tx.send_payload(payload).await;
                    }
                }
                None => return Ok(()),
            }
        }
    }
}
