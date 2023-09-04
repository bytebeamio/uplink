use flume::Receiver;
use serde::{Deserialize, Serialize};

use std::io::{BufRead, BufReader};
use std::process::ChildStdout;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::base::{bridge::BridgeTx, clock};
use crate::{Action, ActionResponse, Payload};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error receiving Action {0}")]
    Recv(#[from] flume::RecvError),
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

#[derive(Debug, Clone, Deserialize)]
pub struct LogcatConfig {
    pub tags: Vec<String>,
    pub min_level: u8,
    pub stream_size: Option<usize>,
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
    pub fn from_string(s: &str) -> Option<LogLevel> {
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
    pub static ref LOGCAT_RE: regex::Regex = regex::Regex::new(r"^(\S+ \S+) (\w)/([^(\s]*).+?:\s*(.*)$").unwrap();
    pub static ref LOGCAT_TIME_RE: regex::Regex = regex::Regex::new(r"^(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)\.(\d+)$").unwrap();
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
        let level = LogLevel::from_string(matches.get(2).ok_or(Error::Level)?.as_str())
            .ok_or(Error::Level)?;
        let tag = matches.get(3).ok_or(Error::Tag)?.as_str().to_string();
        let message = matches.get(4).ok_or(Error::Msg)?.as_str().to_string();

        Ok(Self { level, log_timestamp, tag, message, line: line.to_string(), timestamp })
    }

    pub fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        Ok(Payload {
            stream: "logs".to_string(),
            sequence,
            timestamp: self.log_timestamp,
            payload: serde_json::json!({
                "level": self.level,
                "log_timestamp": self.log_timestamp,
                "tag": self.tag,
                "message": self.message,
                "line": self.line
            }),
        })
    }
}

pub struct Logcat {
    config: LogcatConfig,
    kill_switch: Arc<Mutex<bool>>,
    actions_rx: Receiver<Action>,
    bridge: BridgeTx,
}

impl Drop for Logcat {
    // Ensure last running logger process is killed when dropped
    fn drop(&mut self) {
        self.kill_last()
    }
}

impl Logcat {
    pub fn new(config: LogcatConfig, actions_rx: Receiver<Action>, bridge: BridgeTx) -> Self {
        let kill_switch = Arc::new(Mutex::new(true));

        Self { config, kill_switch, actions_rx, bridge }
    }

    /// On an android system, starts a logcat instance that reports to the logs stream for a given device+project id,
    /// that logcat instance is killed when this object is dropped. On any other system, it's a noop.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        self.spawn_logger(self.config.clone()).await;

        loop {
            let action = self.actions_rx.recv()?;
            let mut config = serde_json::from_str::<LogcatConfig>(action.payload.as_str())?;
            config.tags.retain(|tag| !tag.is_empty());

            self.spawn_logger(config).await;

            let response = ActionResponse::success(&action.action_id);
            self.bridge.send_action_response(response).await;
        }
    }

    // Ensure last used kill switch is set to false so that associated child process gets killed
    fn kill_last(&self) {
        *self.kill_switch.lock().unwrap() = false;
    }

    // Spawn a thread to run the logger command
    async fn spawn_logger(&mut self, config: LogcatConfig) {
        let now = {
            let timestamp = clock();
            let millis = timestamp % 1000;
            let seconds = timestamp / 1000;
            format!("{seconds}.{millis:03}")
        };
        // silence everything
        let mut logcat_args = ["-v", "time", "-T", now.as_str(), "*:S"].map(String::from).to_vec();
        // enable logging for requested tags
        for tag in &config.tags {
            let min_level = LogLevel::from_syslog_level(config.min_level)
                .expect("Couldn't figure out log level");
            logcat_args.push(format!("{}:{}", tag, min_level.to_str()));
        }

        log::info!("logcat args: {:?}", logcat_args);
        let mut logcat = Command::new("logcat");
        logcat.args(logcat_args).stdout(Stdio::piped());

        // Ensure any logger child process created earlier gets killed
        self.kill_last();
        // Use a new mutex kill_switch for future child process
        self.kill_switch = Arc::new(Mutex::new(true));
        let kill_switch = self.kill_switch.clone();

        let bridge = self.bridge.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_micros(1_000_000));
            let mut log_index = 1;
            let mut logger = match logcat.spawn() {
                Ok(logger) => logger,
                Err(e) => {
                    log::error!("failed to start logger: {}", e);
                    return;
                }
            };
            let stdout = logger.stdout.take().unwrap();
            let mut buf_stdout = BufReader::new(stdout);
            loop {
                if !(*kill_switch.lock().unwrap()) {
                    logger.kill().unwrap();
                    break;
                }
                let next_line = match read_line_lossy(&mut buf_stdout) {
                    Ok(l) => l,
                    Err(e) => {
                        log::error!("Logcat error: {e}");
                        break;
                    }
                };
                let next_line = next_line.trim();
                let entry = match LogEntry::from_string(next_line) {
                    Ok(entry) => entry,
                    Err(e) => {
                        log::warn!("log line: {} couldn't be parsed due to: {}", next_line, e);
                        continue;
                    }
                };
                let payload = match entry.to_payload(log_index) {
                    Ok(p) => p,
                    Err(e) => {
                        log::error!("Couldn't convert to payload: {:?}", e);
                        continue;
                    }
                };
                log::trace!("Log entry {:?}", payload);
                bridge.send_payload_sync(payload);
                log_index += 1;
            }
        });
    }
}

fn read_line_lossy(reader: &mut BufReader<ChildStdout>) -> Result<String, String> {
    let mut buf = Vec::with_capacity(256);
    match reader.read_until(b'\n', &mut buf) {
        Err(e) => Err(format!("Error when reading from logcat: {e}")),
        Ok(0) => Err("logcat output has ended".to_string()),
        Ok(_) => Ok(String::from_utf8_lossy(buf.as_slice()).to_string()),
    }
}
