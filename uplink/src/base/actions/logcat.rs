use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::{Datelike, Local, Timelike};

use serde::{Deserialize, Serialize};
use crate::{Payload, Stream};

#[derive(Debug, Deserialize)]
pub struct LogcatConfig {
    pub tags: Vec<String>,
    pub min_level: LogLevel,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Verbose = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Assert = 5,
    Fatal = 6,
}

impl LogLevel {
    pub fn from_str(s: &str) -> Option<LogLevel> {
        match s {
            "V" => Some(LogLevel::Verbose),
            "D" => Some(LogLevel::Debug),
            "I" => Some(LogLevel::Info),
            "W" => Some(LogLevel::Warn),
            "E" => Some(LogLevel::Error),
            "A" => Some(LogLevel::Assert),
            "F" => Some(LogLevel::Fatal),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            LogLevel::Verbose => "V",
            LogLevel::Debug => "D",
            LogLevel::Info => "I",
            LogLevel::Warn => "W",
            LogLevel::Error => "E",
            LogLevel::Assert => "A",
            LogLevel::Fatal => "F",
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

lazy_static::lazy_static! {
    pub static ref LOGCAT_RE: regex::Regex = regex::Regex::new(r#"^(\S+ \S+) (\w)/([^(]+)\([^)]+\): (.*)$"#).unwrap();
    // 07-24 14:52:18.220
    pub static ref LOGCAT_TIME_RE: regex::Regex = regex::Regex::new(r#"^(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\.(\d+)$"#).unwrap();
}

impl LogEntry {
    fn from_string(line: &str) -> Option<Self> {
        let matches = LOGCAT_RE.captures(line)?;
        let log_timestamp = matches.get(1)?.as_str().to_string();
        let level = LogLevel::from_str(matches.get(2)?.as_str())?;
        let tag = matches.get(3)?.as_str().to_string();
        let message = matches.get(4)?.as_str().to_string();
        Some(Self {
            level,
            log_timestamp,
            tag,
            message,
            line: line.to_string(),
        })
    }

    fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        let payload = serde_json::to_value(self)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

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
pub struct LogcatInstance {
    kill_switch: Arc<Mutex<bool>>,
}

impl LogcatInstance {
    pub fn new(mut log_stream: Stream<Payload>, logcat_config: &LogcatConfig) -> Self {
        let kill_switch = Arc::new(Mutex::new(true));

        // silence everything
        let mut logcat_args = vec!["-v", "time", "*:S"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // enable logging for requested tags
        for tag in &logcat_config.tags {
            logcat_args.push(format!("{}:{}", tag, logcat_config.min_level.to_str()));
        }

        log::info!("logcat args: {:?}", logcat_args);
        {
            let kill_switch = kill_switch.clone();

            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_micros(1_000_000));
                let mut log_index = 1;
                match Command::new("logcat")
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
                                            let mut payload = entry.to_payload(log_index).unwrap();
                                            payload.timestamp = parse_logcat_time(entry.log_timestamp.as_str()).unwrap_or(payload.timestamp);
                                            log_stream.push(payload).unwrap();
                                            log_index += 1;
                                        } else {
                                            log::error!("log line in unknown format: {:?}", next_line);
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

pub fn parse_logcat_time(s: &str) -> Option<u64> {
    let matches = LOGCAT_TIME_RE.captures(s)?;
    let date = Local::now()
        .with_month(matches.get(1)?.as_str().parse::<u32>().ok()?)?
        .with_day(matches.get(2)?.as_str().parse::<u32>().ok()?)?
        .with_hour(matches.get(3)?.as_str().parse::<u32>().ok()?)?
        .with_minute(matches.get(4)?.as_str().parse::<u32>().ok()?)?
        .with_second(matches.get(5)?.as_str().parse::<u32>().ok()?)?
        .with_nanosecond(matches.get(6)?.as_str().parse::<u32>().ok()? * 1_000_000)?;
    Some(date.timestamp_millis() as _)
}

impl Drop for LogcatInstance {
    fn drop(&mut self) {
        *self.kill_switch.lock().unwrap() = false;
    }
}