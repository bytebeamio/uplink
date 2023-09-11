use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::base::clock;

pub mod device_shadow;
pub mod downloader;
pub mod installer;
#[cfg(target_os = "linux")]
pub mod journalctl;
#[cfg(target_os = "android")]
pub mod logcat;
pub mod process;
pub mod script_runner;
pub mod simulator;
pub mod systemstats;
pub mod tcpjson;
pub mod tunshell;

#[derive(Debug, Serialize, Default, Clone)]
pub struct ActionsLogEntry {
    timestamp: u64,
    sequence: u32,
    action_id: String,
    stage: String,
    message: String,
}

/// Writer that allows apps to stage and commit log entries to a shared log
#[derive(Debug, Clone)]
pub struct ActionsLogWriter {
    current_entry: ActionsLogEntry,
    log: Arc<Mutex<Vec<ActionsLogEntry>>>,
}

impl ActionsLogWriter {
    /// New action, reset sequence, update stage and remove message from staged log entry
    fn accept_action(&mut self, action_id: impl Into<String>, stage: impl Into<String>) {
        self.current_entry.sequence = 0;
        self.current_entry.action_id = action_id.into();
        self.current_entry.stage = stage.into();
        self.current_entry.message.clear();
    }

    /// Update stage and remove message from staged log entry
    fn update_stage(&mut self, stage: impl Into<String>) {
        self.current_entry.stage = stage.into();
        self.current_entry.message.clear();
    }

    /// Update message contents of staged log entry
    fn update_message(&mut self, msg: impl Into<String>) {
        self.current_entry.message = msg.into()
    }

    /// Write staged log entry to the log
    fn commit_entry(&mut self) {
        self.current_entry.sequence += 1;
        self.current_entry.timestamp = clock() as u64;

        let entry = self.current_entry.clone();
        let mut log = self.log.lock().unwrap();
        log.push(entry);
    }
}

/// Reader to access entries from shared log
#[derive(Debug)]
pub struct ActionsLogReader {
    log: Arc<Mutex<Vec<ActionsLogEntry>>>,
}

impl ActionsLogReader {
    /// Flush all entries from log
    pub fn flush(&mut self) -> Vec<ActionsLogEntry> {
        let mut log = self.log.lock().unwrap();
        log.drain(..).collect()
    }
}

pub struct ActionsLog;

impl ActionsLog {
    pub fn new() -> (ActionsLogWriter, ActionsLogReader) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let writer = ActionsLogWriter { current_entry: Default::default(), log: log.clone() };
        let reader = ActionsLogReader { log };

        (writer, reader)
    }
}
