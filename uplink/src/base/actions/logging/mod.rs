use std::sync::{Arc, Mutex};

#[cfg(target_os = "linux")]
mod journalctl;
#[cfg(target_os = "android")]
mod logcat;

#[cfg(target_os = "linux")]
pub use journalctl::new_journalctl;
#[cfg(target_os = "android")]
pub use logcat::new_logcat;

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
