use serde::Serialize;

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

#[derive(Debug, Serialize)]
pub struct ActionsLog {
    timestamp: u64,
    sequence: u32,
    action_id: String,
    stage: String,
    message: String,
}
