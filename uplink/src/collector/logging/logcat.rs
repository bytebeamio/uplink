use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{Datelike, Local, Timelike};
use serde::{Deserialize, Serialize};

use super::LoggingConfig;
use crate::Payload;

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

    pub fn from_syslog_level(l: u8) -> Option<LogLevel> {
        let level = match l {
            0 => LogLevel::Verbose,
            1 => LogLevel::Debug,
            2 => LogLevel::Info,
            3 => LogLevel::Warn,
            4 => LogLevel::Error,
            5 => LogLevel::Assert,
            6 => LogLevel::Fatal,
            _ => return None,
        };

        Some(level)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Couldn't parse logline")]
    Parse,
    #[error("Couldn't parse timestamp")]
    Timestamp,
    #[error("Couldn't parse level")]
    Level,
    #[error("Couldn't parse tag")]
    Tag,
    #[error("Couldn't parse message")]
    Msg,
}

#[derive(Debug, Serialize)]
pub struct LogEntry {
    level: LogLevel,
    log_timestamp: String,
    tag: String,
    message: String,
    line: String,
    timestamp: u64,
}

lazy_static::lazy_static! {
    pub static ref LOGCAT_RE: regex::Regex = regex::Regex::new(r#"^(\S+ \S+) (\w)/([^(\s]*).+?:\s*(.*)$"#).unwrap();
    pub static ref LOGCAT_TIME_RE: regex::Regex = regex::Regex::new(r#"^(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\.(\d+)$"#).unwrap();
}

pub fn parse_logcat_time(s: &str) -> Option<u64> {
    let matches = LOGCAT_TIME_RE.captures(s)?;
    let date = Local::now()
        .with_month(matches.get(1)?.as_str().parse::<u32>().ok()?)?
        .with_day(matches.get(2)?.as_str().parse::<u32>().ok()?)?
        .with_hour(matches.get(3)?.as_str().parse::<u32>().ok()?)?
        .with_minute(matches.get(4)?.as_str().parse::<u32>().ok()?)?
        .with_second(matches.get(5)?.as_str().parse::<u32>().ok()?)?
        .with_second(matches.get(6)?.as_str().parse::<u32>().ok()? * 1_000_000)?;
    Some(date.timestamp_millis() as _)
}

impl LogEntry {
    pub fn from_string(line: &str) -> anyhow::Result<Self> {
        let matches = LOGCAT_RE.captures(line).ok_or(Error::Parse)?;
        let log_timestamp = matches.get(1).ok_or(Error::Timestamp)?.as_str().to_string();
        let level =
            LogLevel::from_str(matches.get(2).ok_or(Error::Level)?.as_str()).ok_or(Error::Level)?;
        let tag = matches.get(3).ok_or(Error::Tag)?.as_str().to_string();
        let message = matches.get(4).ok_or(Error::Msg)?.as_str().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;
        let timestamp = parse_logcat_time(line).unwrap_or(timestamp);

        Ok(Self { level, log_timestamp, tag, message, line: line.to_string(), timestamp })
    }

    pub fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        let payload = serde_json::to_value(self)?;

        Ok(Payload {
            stream: "logs".to_string(),
            sequence,
            timestamp: self.timestamp,
            collection_timestamp: self.timestamp,
            payload,
        })
    }
}

pub fn new_logcat(logging_config: &LoggingConfig) -> Command {
    // silence everything
    let mut logcat_args = ["-v", "time", "*:S"].map(String::from).to_vec();
    // enable logging for requested tags
    for tag in &logging_config.tags {
        let min_level = LogLevel::from_syslog_level(logging_config.min_level)
            .expect("Couldn't figure out log level");
        logcat_args.push(format!("{}:{}", tag, min_level.to_str()));
    }

    tracing::info!("logcat args: {:?}", logcat_args);
    let mut logcat = Command::new("logcat");
    logcat.args(logcat_args).stdout(Stdio::piped());

    logcat
}
