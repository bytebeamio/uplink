use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::{process::Command, time::Duration};

#[cfg(target_os = "linux")]
mod journalctl;
#[cfg(target_os = "android")]
mod logcat;

use crate::base::bridge::BridgeTx;
#[cfg(target_os = "linux")]
use crate::base::JournalctlConfig;
use crate::Config;
#[cfg(target_os = "linux")]
pub use journalctl::{new_journalctl, LogEntry, LOG_ACTION_NAME};
#[cfg(target_os = "android")]
pub use logcat::{new_logcat, LogEntry, LOG_ACTION_NAME};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error receiving Action {0}")]
    Recv(#[from] flume::RecvError),
}

#[derive(Debug, serde::Deserialize)]
pub struct LoggingConfig {
    pub tags: Vec<String>,
    pub min_level: u8,
}

pub struct LoggerInstance {
    config: Arc<Config>,
    bridge_tx: BridgeTx,
    kill_switch: Arc<Mutex<bool>>,
}

impl Drop for LoggerInstance {
    // Ensure last running logger process is killed when dropped
    fn drop(&mut self) {
        self.kill_last()
    }
}

impl LoggerInstance {
    pub fn new(config: Arc<Config>, bridge_tx: BridgeTx) -> Self {
        let kill_switch = Arc::new(Mutex::new(true));

        Self { config, bridge_tx, kill_switch }
    }

    /// On an android system, starts a logcat instance and a journalctl instance for a linux system that
    /// reports to the logs stream for a given device+project id, that logcat instance is killed when
    /// this object is dropped. On any other system, it's a noop.
    pub fn start(mut self) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        if let Some(JournalctlConfig { priority, tags, stream_size: _ }) = &self.config.journalctl {
            let config = LoggingConfig { tags: tags.clone(), min_level: *priority };
            self.spawn_logger(new_journalctl(&config));
        }

        #[cfg(target_os = "android")]
        if self.config.run_logcat {
            let config = LoggingConfig { tags: vec!["*".to_string()], min_level: 1 };
            self.spawn_logger(new_logcat(&config));
        }

        let log_rx = self.bridge_tx.register_action_route_sync(LOG_ACTION_NAME);

        loop {
            let action = log_rx.recv()?;
            let mut config = serde_json::from_str::<LoggingConfig>(action.payload.as_str())?;
            config.tags.retain(|tag| !tag.is_empty());
            log::info!("restarting journalctl with following config: {:?}", config);

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
        let bridge_tx = self.bridge_tx.clone();

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
                log::debug!("Log entry {:?}", payload);
                bridge_tx.send_payload_sync(payload);
                log_index += 1;
            }
        });
    }
}
