use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::{spawn_logger, LoggerInstance, LoggingConfig};
use crate::{Payload, Stream};

#[derive(Debug, Deserialize)]
pub struct JournalctlConfig {
    pub tags: Vec<String>,
    pub min_level: u8,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Emergency = 0,
    Alert = 1,
    Critical = 2,
    Error = 3,
    Warn = 4,
    Notice = 5,
    Info = 6,
    Debug = 7,
}

impl LogLevel {
    pub fn from_syslog_level(s: &str) -> LogLevel {
        match s {
            "0" => LogLevel::Emergency,
            "1" => LogLevel::Alert,
            "2" => LogLevel::Critical,
            "3" => LogLevel::Error,
            "4" => LogLevel::Warn,
            "5" => LogLevel::Notice,
            "6" => LogLevel::Info,
            "7" => LogLevel::Debug,
            _ => LogLevel::Info,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct LogEntry {
    level: LogLevel,
    log_timestamp: String,
    tag: String,
    message: String,
    line: String,
}

#[derive(Deserialize)]
struct JournaldEntry {
    #[serde(rename = "PRIORITY")]
    priority: String,

    #[serde(rename = "__REALTIME_TIMESTAMP")]
    log_timestamp: String,

    #[serde(rename = "SYSLOG_IDENTIFIER")]
    tag: String,

    #[serde(rename = "MESSAGE")]
    message: String,
}

impl LogEntry {
    pub fn from_string(line: &str) -> Option<Self> {
        let entry: Option<JournaldEntry> = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(e) => {
                log::error!("Failed to deserialize line . Error = {:?}", e);
                None
            }
        };

        match entry {
            Some(entry) => Some(Self {
                level: LogLevel::from_syslog_level(&entry.priority),
                log_timestamp: entry.log_timestamp,
                tag: entry.tag,
                message: entry.message,
                line: "".to_string(),
            }),
            None => None,
        }
    }

    pub fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        let payload = serde_json::to_value(self)?;
        let timestamp = self.log_timestamp.parse::<u64>().unwrap();

        Ok(Payload { stream: "logs".to_string(), sequence, timestamp, payload })
    }
}

/// On a linux system, starts a journalctl instance that reports to the logs stream for a given
/// device+project id, that journalctl instance is killed when this object is dropped
/// On any other system, it's a noop

pub fn new_journalctl(
    log_stream: Stream<Payload>,
    logging_config: &LoggingConfig,
) -> LoggerInstance {
    let kill_switch = Arc::new(Mutex::new(true));

    // silence everything
    let mut journalctl_args = ["-o", "json", "-f", "-p", &logging_config.min_level.to_string()]
        .map(String::from)
        .to_vec();
    // enable logging for requested tags
    for tag in &logging_config.tags {
        let tag_args = ["-t", tag].map(String::from).to_vec();
        journalctl_args.extend(tag_args);
    }

    log::info!("journalctl args: {:?}", journalctl_args);
    let mut journalctl = Command::new("journalctl");
    journalctl.args(journalctl_args).stdout(Stdio::piped());
    spawn_logger(log_stream, kill_switch.clone(), journalctl);

    LoggerInstance { kill_switch }
}
