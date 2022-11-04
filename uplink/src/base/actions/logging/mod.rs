use std::sync::{Arc, Mutex};

mod journalctl;
mod logcat;

pub use journalctl::new_journalctl;
pub use logcat::new_logcat;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
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
