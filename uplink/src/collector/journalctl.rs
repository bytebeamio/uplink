use flume::Receiver;
use serde::{Deserialize, Serialize};

use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::{io::BufReader, time::Duration};

use crate::Action;
use crate::{base::bridge::BridgeTx, ActionResponse, Payload};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error receiving Action {0}")]
    Recv(#[from] flume::RecvError),
}

#[derive(Debug, Clone, Deserialize)]
pub struct JournalCtlConfig {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub units: Vec<String>,
    pub min_level: u8,
    pub stream_size: Option<usize>,
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

#[derive(Debug, Serialize)]
pub struct LogEntry {
    level: LogLevel,
    log_timestamp: String,
    tag: String,
    message: String,
    line: String,
}

impl LogEntry {
    pub fn from_string(line: &str) -> anyhow::Result<Self> {
        let entry: JournaldEntry = serde_json::from_str(line)?;

        Ok(Self {
            level: LogLevel::from_syslog_level(&entry.priority),
            log_timestamp: entry.log_timestamp,
            tag: entry.tag,
            message: entry.message,
            line: line.to_owned(),
        })
    }

    pub fn to_payload(&self, sequence: u32) -> anyhow::Result<Payload> {
        let payload = serde_json::to_value(self)?;
        // Convert from microseconds to milliseconds
        let timestamp = self.log_timestamp.parse::<u64>()? / 1000;

        Ok(Payload { stream: "logs".to_string(), sequence, timestamp, payload })
    }
}

pub struct JournalCtl {
    config: JournalCtlConfig,
    kill_switch: Arc<Mutex<bool>>,
    actions_rx: Receiver<Action>,
    bridge: BridgeTx,
}

impl Drop for JournalCtl {
    // Ensure last running logger process is killed when dropped
    fn drop(&mut self) {
        self.kill_last()
    }
}

impl JournalCtl {
    pub fn new(config: JournalCtlConfig, actions_rx: Receiver<Action>, bridge: BridgeTx) -> Self {
        let kill_switch = Arc::new(Mutex::new(true));

        Self { config, kill_switch, actions_rx, bridge }
    }

    /// Starts a journalctl instance on a linux system which reports to the logs stream for a given device+project id,
    /// that logcat instance is killed when this object is dropped. On any other system, it's a noop.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        self.spawn_logger(self.config.clone()).await;

        loop {
            let action = self.actions_rx.recv()?;
            let mut config = serde_json::from_str::<JournalCtlConfig>(action.payload.as_str())?;
            config.tags.retain(|tag| !tag.is_empty());
            config.units.retain(|unit| !unit.is_empty());
            if config.tags.is_empty() && config.units.is_empty() {
                let response = ActionResponse::failure(&action.action_id, "No targets to log for");
                self.bridge.send_action_response(response).await;
                continue;
            }

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
    async fn spawn_logger(&mut self, config: JournalCtlConfig) {
        // silence everything
        let mut journalctl_args =
            ["-o", "json", "-f", "-p", &config.min_level.to_string()].map(String::from).to_vec();
        // enable logging for requested tags
        for tag in &config.tags {
            let tag_args = ["-t", tag].map(String::from).to_vec();
            journalctl_args.extend(tag_args);
        }
        // enable logging for requested units
        for unit in &config.units {
            let unit_args = ["-u", unit].map(String::from).to_vec();
            journalctl_args.extend(unit_args);
        }

        log::info!("journalctl args: {:?}", journalctl_args);
        let mut journalctl = Command::new("journalctl");
        journalctl.args(journalctl_args).stdout(Stdio::piped());

        // Ensure any logger child process created earlier gets killed
        self.kill_last();
        // Use a new mutex kill_switch for future child process
        self.kill_switch = Arc::new(Mutex::new(true));
        let kill_switch = self.kill_switch.clone();

        let bridge = self.bridge.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_micros(1_000_000));
            let mut log_index = 1;
            let mut logger = match journalctl.spawn() {
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
                let mut next_line = String::new();
                match buf_stdout.read_line(&mut next_line) {
                    Ok(0) => {
                        log::info!("logger output has ended");
                        break;
                    }
                    Err(e) => {
                        log::error!("error while reading logger output: {}", e);
                        break;
                    }
                    _ => (),
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
