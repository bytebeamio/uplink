use serde::Serialize;
use std::time::Instant;

use super::Error as DownloaderError;
use crate::base::clock;

#[derive(Debug, Serialize, Clone)]
pub struct DownloaderMetrics {
    pub timestamp: u128,
    pub sequence: u32,
    pub current_action: String,
    pub downloads: u64,
    #[serde(skip_serializing)]
    pub download_start_time: Instant,
    #[serde(skip_serializing)]
    pub last_checkpoint_time: Instant,
    #[serde(skip_serializing)]
    pub bytes_downloaded: usize,
    pub min_download_speed: f64,
    pub max_download_speed: f64,
    pub avg_download_speed: f64,
    pub errors: Vec<String>,
}

impl DownloaderMetrics {
    pub fn new() -> Self {
        Self {
            timestamp: clock(),
            sequence: 1,
            downloads: 0,
            current_action: "".to_owned(),
            download_start_time: Instant::now(),
            last_checkpoint_time: Instant::now(),
            bytes_downloaded: 0,
            min_download_speed: 0.0,
            max_download_speed: 0.0,
            avg_download_speed: 0.0,
            errors: vec![],
        }
    }

    pub fn new_download(&mut self, action_id: String) {
        self.current_action = action_id;
        self.downloads += 1;
    }

    pub fn add_bytes(&mut self, bytes: usize) {
        self.bytes_downloaded += bytes;
        self.timestamp = clock();

        let time_delta = self.last_checkpoint_time.elapsed().as_secs_f64();
        let download_speed = bytes as f64 / time_delta;

        self.max_download_speed = self.max_download_speed.max(download_speed);
        self.min_download_speed = self.min_download_speed.min(download_speed);

        let elapsed_time = self.download_start_time.elapsed().as_secs_f64();
        self.avg_download_speed = self.bytes_downloaded as f64 / elapsed_time;
    }

    pub fn add_error(&mut self, error: &DownloaderError) {
        self.errors.push(error.to_string());
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.bytes_downloaded = 0;
        self.download_start_time = Instant::now();
        self.max_download_speed = 0.0;
        self.min_download_speed = 0.0;
        self.avg_download_speed = 0.0;
    }
}
