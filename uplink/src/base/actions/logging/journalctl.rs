use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{Payload, Stream};
use serde::{Deserialize, Serialize};

use super::{LoggerInstance, LoggingConfig};

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
struct LogEntry {
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
    fn from_string(line: &str) -> Option<Self> {
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

    fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
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
    spawn_journalctl(log_stream, kill_switch.clone(), journalctl_args);
    LoggerInstance { kill_switch }
}

// Spawn a thread to read from journalctl and write to
fn spawn_journalctl(
    mut log_stream: Stream<Payload>,
    kill_switch: Arc<Mutex<bool>>,
    journalctl_args: Vec<String>,
) {
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_micros(1_000_000));
        let mut log_index = 1;
        let mut journalctl =
            match Command::new("journalctl").args(journalctl_args).stdout(Stdio::piped()).spawn() {
                Ok(journalctl) => journalctl,
                Err(e) => {
                    log::error!("failed to start journalctl: {}", e);
                    return;
                }
            };
        let stdout = journalctl.stdout.take().unwrap();
        let mut buf_stdout = BufReader::new(stdout);
        loop {
            if *kill_switch.lock().unwrap() == false {
                journalctl.kill().unwrap();
                break;
            }
            let mut next_line = String::new();
            match buf_stdout.read_line(&mut next_line) {
                Ok(0) => {
                    log::info!("journalctl output has ended");
                    break;
                }
                Err(e) => {
                    log::error!("error while reading journalctl output: {}", e);
                    break;
                }
                _ => (),
            };

            let next_line = next_line.trim();
            let entry = match LogEntry::from_string(next_line) {
                Some(entry) => entry,
                _ => {
                    log::warn!("log line in unknown format: {:?}", next_line);
                    continue;
                }
            };
            let payload = entry.to_payload(log_index).unwrap();
            log::debug!("Log entry {:?}", payload);
            log_stream.push(payload).unwrap();
            log_index += 1;
        }
    });
}
