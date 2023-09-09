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
pub struct ActionsLog {
    timestamp: u64,
    sequence: u32,
    action_id: String,
    stage: String,
    message: String,
}

impl ActionsLog {
    fn accept_action(&mut self, action_id: impl Into<String>, stage: impl Into<String>) {
        self.sequence = 0;
        self.action_id = action_id.into();
        self.stage = stage.into();
        self.message.clear();
    }

    fn update_stage(&mut self, stage: impl Into<String>) {
        self.stage = stage.into();
        self.message.clear();
    }

    fn update_message(&mut self, msg: impl Into<String>) {
        self.message = msg.into()
    }

    fn capture(&mut self) -> Self {
        self.sequence += 1;
        self.timestamp = clock() as u64;

        self.clone()
    }
}
