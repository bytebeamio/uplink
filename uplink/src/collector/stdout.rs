use tokio::io::{stdin, AsyncBufReadExt, BufReader};

use serde::Serialize;
use serde_json::json;

use crate::base::{
    bridge::{BridgeTx, Payload},
    clock, StdoutConfig,
};

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
        Self { line, level: "".to_string(), timestamp: clock() as u64, message: "".to_string() }
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
                    let log_entry = LogEntry::parse(l);
                    let payload = Payload {
                        stream: self.config.stream_name.to_owned(),
                        device_id: None,
                        sequence,
                        timestamp: log_entry.timestamp,
                        payload: json!(log_entry),
                    };
                    self.tx.send_payload(payload).await;
                }
                None => return Ok(()),
            }
        }
    }
}
