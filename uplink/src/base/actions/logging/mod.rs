use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::{process::Command, time::Duration};

#[cfg(target_os = "linux")]
mod journalctl;
#[cfg(target_os = "android")]
mod logcat;

#[cfg(target_os = "linux")]
pub use journalctl::{new_journalctl, LogEntry};
#[cfg(target_os = "android")]
pub use logcat::{new_logcat, LogEntry};

use crate::{Payload, Stream};

#[derive(Debug, serde::Deserialize)]
pub struct LoggingConfig {
    pub tags: Vec<String>,
    pub min_level: u8,
}

pub struct LoggerInstance {
    kill_switch: Arc<Mutex<bool>>,
}

impl Drop for LoggerInstance {
    fn drop(&mut self) {
        *self.kill_switch.lock().unwrap() = false;
    }
}

// Spawn a thread to run the logger command
fn spawn_logger(
    mut log_stream: Stream<Payload>,
    kill_switch: Arc<Mutex<bool>>,
    mut logger: Command,
) {
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
                Some(entry) => entry,
                _ => {
                    log::warn!("log line in unknown format: {:?}", next_line);
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
            log_stream.push(payload).unwrap();
            log_index += 1;
        }
    });
}
