use flume::Sender;
use serde::Deserialize;

use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::{process::Command, time::Duration};

#[cfg(target_os = "linux")]
mod journalctl;
#[cfg(target_os = "android")]
mod logcat;

use crate::base::{bridge::BridgeTx, ActionRoute};
use crate::{Config, Package, Payload, Stream};
#[cfg(target_os = "linux")]
pub use journalctl::{new_journalctl, LogEntry};
#[cfg(target_os = "android")]
pub use logcat::{new_logcat, LogEntry};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error receiving Action {0}")]
    Recv(#[from] flume::RecvError),
}

pub struct LoggerInstance {
    config: Arc<Config>,
    log_stream: Stream<Payload>,
    kill_switch: Arc<Mutex<bool>>,
    bridge: BridgeTx,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggerConfig {
    pub tags: Vec<String>,
    pub min_level: u8,
    pub stream_size: Option<usize>,
}

impl Drop for LoggerInstance {
    // Ensure last running logger process is killed when dropped
    fn drop(&mut self) {
        self.kill_last()
    }
}

impl LoggerInstance {
    pub fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>, bridge: BridgeTx) -> Self {
        let buf_size = config.logging.as_ref().and_then(|c| c.stream_size).unwrap_or(32);

        let log_stream = Stream::dynamic_with_size(
            "logs",
            &config.project_id,
            &config.device_id,
            buf_size,
            data_tx,
        );
        let kill_switch = Arc::new(Mutex::new(true));

        Self { config, log_stream, kill_switch, bridge }
    }

    /// On an android system, starts a logcat instance and a journalctl instance for a linux system that
    /// reports to the logs stream for a given device+project id, that logcat instance is killed when
    /// this object is dropped. On any other system, it's a noop.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        if let Some(config) = &self.config.logging {
            #[cfg(target_os = "linux")]
            self.spawn_logger(new_journalctl(config));
            #[cfg(target_os = "android")]
            self.spawn_logger(new_logcat(config));
        }

        let log_rx = self
            .bridge
            .register_action_route(ActionRoute { name: "logging_config".to_string(), timeout: 10 })
            .await;

        loop {
            let action = log_rx.recv()?;
            let mut config = serde_json::from_str::<LoggerConfig>(action.payload.as_str())?;
            config.tags.retain(|tag| !tag.is_empty());

            // Ensure any logger child process created earlier gets killed
            self.kill_last();
            // Use a new mutex kill_switch for future child process
            self.kill_switch = Arc::new(Mutex::new(true));

            #[cfg(target_os = "linux")]
            self.spawn_logger(new_journalctl(&config));
            #[cfg(target_os = "android")]
            self.spawn_logger(new_logcat(&config));
        }
    }

    // Ensure last used kill switch is set to false so that associated child process gets killed
    fn kill_last(&self) {
        *self.kill_switch.lock().unwrap() = false;
    }

    // Spawn a thread to run the logger command
    fn spawn_logger(&mut self, mut logger: Command) {
        let kill_switch = self.kill_switch.clone();
        let mut log_stream = self.log_stream.clone();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_micros(1_000_000));
            let mut log_index = 1;
            let mut logger = match logger.spawn() {
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
                log_stream.push(payload).unwrap();
                log_index += 1;
            }
        });
    }
}
