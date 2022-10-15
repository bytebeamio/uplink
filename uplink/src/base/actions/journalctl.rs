use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration};

use serde::{Deserialize, Serialize};
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
    Debug = 7
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
    #[serde(rename="PRIORITY")]
    priority: String,

    #[serde(rename="__REALTIME_TIMESTAMP")]
    log_timestamp: String,

    #[serde(rename="SYSLOG_IDENTIFIER")]
    tag: String,

    #[serde(rename="MESSAGE")]
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
            None => None
        }
    }

    fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        let payload = serde_json::to_value(self)?;
        let timestamp = self.log_timestamp.parse::<u64>().unwrap();

        Ok(Payload {
            stream: "logs".to_string(),
            sequence,
            timestamp,
            payload,
        })
    }
}

/// On an android system, starts a logcat instance that reports to the logs stream for a given
/// device+project id, that logcat instance is killed when this object is dropped
/// On any other system, it's a noop
pub struct JournalctlInstance {
    kill_switch: Arc<Mutex<bool>>,
}

impl JournalctlInstance {
    pub fn new(mut log_stream: Stream<Payload>, logcat_config: &JournalctlConfig) -> Self {
        let kill_switch = Arc::new(Mutex::new(true));

        // silence everything
        let mut logcat_args = vec!["-o", "json", "-f", "-p", &logcat_config.min_level.to_string()].iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // enable logging for requested tags
        for tag in &logcat_config.tags {
            logcat_args.push("-t".to_string());
            logcat_args.push(tag.to_string());
        }

        log::info!("logcat args: {:?}", logcat_args);

        {
            let kill_switch = kill_switch.clone();

            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_micros(1_000_000));
                let mut log_index = 1;
                match Command::new("journalctl")
                    .args(logcat_args.iter().collect::<Vec<_>>())
                    .stdout(Stdio::piped())
                    .spawn() {
                    Ok(mut logcat) => {
                        let stdout = logcat
                            .stdout
                            .take()
                            .unwrap();
                        let mut buf_stdout = BufReader::new(stdout);
                        loop {
                            if *kill_switch.lock().unwrap() == false {
                                logcat.kill().ok();
                                break;
                            } else {
                                let mut next_line = String::new();
                                match buf_stdout.read_line(&mut next_line) {
                                    Ok(bc) => {
                                        if bc == 0 {
                                            break;
                                        }
                                        let next_line = next_line.trim();
                                        if let Some(entry) = LogEntry::from_string(next_line) {
                                            let payload = entry.to_payload(log_index).unwrap();
                                            log::debug!("Log entry {:?}", payload);
                                            log_stream.push(payload).unwrap();
                                            log_index += 1;
                                        } else {
                                            log::warn!("log line in unknown format: {:?}", next_line);
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("error while reading logcat output: {}", e);
                                        break;
                                    }
                                };
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("failed to start logcat: {}", e);
                    }
                };
            });
        }
        Self {
            kill_switch,
        }
    }
}

impl Drop for JournalctlInstance {
    fn drop(&mut self) {
        *self.kill_switch.lock().unwrap() = false;
    }
}
