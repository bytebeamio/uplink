use regex::{Match, Regex};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};

use serde::Serialize;
use serde_json::json;

use crate::base::bridge::{BridgeTx, Payload};
use crate::base::StdoutConfig;

lazy_static::lazy_static! {
    pub static ref COLORS_RE: Regex = Regex::new(r#"\u{1b}\[[0-9;]*m"#).unwrap();
}

#[derive(Debug, Serialize)]
struct LogEntry {
    pub line: String,
    pub tag: Option<String>,
    pub level: Option<String>,
    #[serde(skip)]
    pub timestamp: u64,
    pub message: Option<String>,
}

/// Parse timestamp from log line, use current time as default if unable to ascertain partially
pub fn parse_timestamp(s: &str, template: &Regex) -> Option<u64> {
    let matches = template.captures(s)?;
    let mut date = time::OffsetDateTime::now_utc();
    if let Some(year) = matches.name("year") {
        let year = year.as_str().parse().ok()?;
        date = date.replace_year(year).ok()?;
    }
    if let Some(month) = matches.name("month") {
        let month = month.as_str().parse().ok()?;
        date = date.replace_month(month).ok()?;
    }
    if let Some(day) = matches.name("day") {
        let day = day.as_str().parse().ok()?;
        date = date.replace_day(day).ok()?;
    }
    if let Some(hour) = matches.name("hour") {
        let hour = hour.as_str().parse().ok()?;
        date = date.replace_hour(hour).ok()?;
    }
    if let Some(minute) = matches.name("minute") {
        let minute = minute.as_str().parse().ok()?;
        date = date.replace_minute(minute).ok()?;
    }
    if let Some(second) = matches.name("second") {
        let second = second.as_str().parse().ok()?;
        date = date.replace_second(second).ok()?;
    }
    if let Some(microsecond) = matches.name("microsecond") {
        let microsecond = microsecond.as_str().parse().ok()?;
        date = date.replace_microsecond(microsecond).ok()?;
    }

    Some((date.unix_timestamp_nanos() / 1_000_000) as u64)
}

impl LogEntry {
    // NOTE: expects log lines to contain information in the format "{log_timestamp} {level} {tag}: {message}"
    fn parse(line: String, log_template: &Regex, timestamp_template: &Regex) -> Option<Self> {
        let to_string = |x: Match| x.as_str().to_string();
        let to_timestamp = |t: Match| parse_timestamp(t.as_str(), timestamp_template);
        // NOTE: remove any tty color escape characters
        let line = COLORS_RE.replace_all(&line, "").trim().to_string();
        let captures = log_template.captures(&line)?;
        // Use current time if not able to parse properly
        let timestamp = match captures.name("timestamp").map(to_timestamp).flatten() {
            Some(t) => t,
            _ => (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as u64,
        };
        let level = captures.name("level").map(to_string);
        let tag = captures.name("tag").map(to_string);
        let message = captures.name("message").map(to_string);

        Some(Self { line, level, tag, timestamp, message })
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
    timestamp_template: Regex,
}

impl Stdout {
    pub fn new(config: StdoutConfig, tx: BridgeTx) -> Self {
        let log_template = Regex::new(&config.log_template).unwrap();
        let timestamp_template = Regex::new(&config.timestamp_template).unwrap();
        Self { config, tx, log_template, timestamp_template }
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
                    if let Some(log_entry) =
                        LogEntry::parse(l, &self.log_template, &self.timestamp_template)
                    {
                        let payload = Payload {
                            stream: self.config.stream_name.to_owned(),
                            device_id: None,
                            sequence,
                            timestamp: log_entry.timestamp,
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
