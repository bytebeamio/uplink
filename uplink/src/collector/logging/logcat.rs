use serde::{Deserialize, Serialize};

use std::process::{Command, Stdio};

use super::LoggerConfig;
use crate::{base::clock, Payload};

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
            0 => LogLevel::Fatal,
            1 => LogLevel::Assert,
            2 => LogLevel::Error,
            3 => LogLevel::Warn,
            4 => LogLevel::Info,
            5 => LogLevel::Debug,
            6 => LogLevel::Verbose,
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
    log_timestamp: u64,
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
    let date = time::OffsetDateTime::now_utc()
        .replace_month(matches.get(1)?.as_str().parse::<u8>().ok()?.try_into().ok()?)
        .ok()?
        .replace_day(matches.get(2)?.as_str().parse::<u8>().ok()?)
        .ok()?
        .replace_hour(matches.get(3)?.as_str().parse::<u8>().ok()?)
        .ok()?
        .replace_minute(matches.get(4)?.as_str().parse::<u8>().ok()?)
        .ok()?
        .replace_second(matches.get(5)?.as_str().parse::<u8>().ok()?)
        .ok()?
        .replace_microsecond(matches.get(6)?.as_str().parse::<u32>().ok()? * 1_000_000)
        .ok()?;
    Some(date.unix_timestamp() as _)
}

impl LogEntry {
    pub fn from_string(line: &str) -> anyhow::Result<Self> {
        let matches = LOGCAT_RE.captures(line).ok_or(Error::Parse)?;
        let timestamp = clock() as u64;
        let log_timestamp = parse_logcat_time(matches.get(1).ok_or(Error::Timestamp)?.as_str())
            .unwrap_or(timestamp);
        let level =
            LogLevel::from_str(matches.get(2).ok_or(Error::Level)?.as_str()).ok_or(Error::Level)?;
        let tag = matches.get(3).ok_or(Error::Tag)?.as_str().to_string();
        let message = matches.get(4).ok_or(Error::Msg)?.as_str().to_string();

        Ok(Self { level, log_timestamp, tag, message, line: line.to_string(), timestamp })
    }

    pub fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        let payload = serde_json::to_value(self)?;

        Ok(Payload {
            stream: "logs".to_string(),
            device_id: None,
            sequence,
            timestamp: self.timestamp,
            payload,
        })
    }
}

pub fn new_logcat(logging_config: &LoggerConfig) -> Command {
    let now = {
        let timestamp = clock();
        let millis = timestamp % 1000;
        let seconds = timestamp / 1000;
        format!("{seconds}.{millis:03}")
    };
    // silence everything
    let mut logcat_args = ["-v", "time", "-T", now.as_str(), "*:S"].map(String::from).to_vec();
    // enable logging for requested tags
    for tag in &logging_config.tags {
        let min_level = LogLevel::from_syslog_level(logging_config.min_level)
            .expect("Couldn't figure out log level");
        logcat_args.push(format!("{}:{}", tag, min_level.to_str()));
    }

    log::info!("logcat args: {:?}", logcat_args);
    let mut logcat = Command::new("logcat");
    logcat.args(logcat_args).stdout(Stdio::piped());

    logcat
}
