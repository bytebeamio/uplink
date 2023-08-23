use serde::Serialize;
use std::time::Instant;

use super::Error as DownloaderError;
use crate::base::clock;

#[derive(Debug, Serialize, Clone)]
pub struct DownloaderMetrics {
    pub timestamp: u128,
    pub sequence: u32,
    /// Action that triggered current download
    pub current_action: String,
    /// Total count of downloads since uplink start
    pub total_downloads: u64,
    #[serde(skip_serializing)]
    pub download_start_time: Instant,
    #[serde(skip_serializing)]
    pub last_checkpoint_time: Instant,
    #[serde(skip_serializing)]
    pub bytes_downloaded: usize,
    /// Minimum rate of download observed (bytes/second)
    pub min_download_speed: Option<f64>,
    /// Maximum rate of download observed (bytes/second)
    pub max_download_speed: Option<f64>,
    /// Average rate of download observed (bytes/second)
    pub avg_download_speed: f64,
    /// Errors observed since last metrics push
    pub errors: Vec<String>,
}

impl Default for DownloaderMetrics {
    fn default() -> Self {
        Self {
            timestamp: 0,
            sequence: 0,
            current_action: "".to_string(),
            total_downloads: 0,
            download_start_time: Instant::now(),
            last_checkpoint_time: Instant::now(),
            bytes_downloaded: 0,
            min_download_speed: None,
            max_download_speed: None,
            avg_download_speed: 0.0,
            errors: vec![],
        }
    }
}

impl DownloaderMetrics {
    pub fn new_download(&mut self, action_id: String) {
        self.current_action = action_id;
        self.total_downloads += 1;
    }

    pub fn add_bytes(&mut self, bytes: usize) {
        self.bytes_downloaded += bytes;
        self.timestamp = clock();

        let time_delta = self.last_checkpoint_time.elapsed().as_secs_f64();
        let download_speed = bytes as f64 / time_delta;

        match &mut self.max_download_speed {
            Some(max_speed) => *max_speed = max_speed.max(download_speed),
            _ => self.max_download_speed = Some(download_speed),
        }
        match &mut self.min_download_speed {
            Some(min_speed) => *min_speed = min_speed.min(download_speed),
            _ => self.max_download_speed = Some(download_speed),
        }

        let elapsed_time = self.download_start_time.elapsed().as_secs_f64();
        self.avg_download_speed = self.bytes_downloaded as f64 / elapsed_time;
    }

    pub fn add_error(&mut self, error: &DownloaderError) {
        self.errors.push(error.to_string());
    }

    pub fn remove_download(&mut self) {
        self.current_action.clear();
    }

    pub fn capture(&mut self) -> Self {
        self.timestamp = clock();
        let metrics = self.clone();

        self.sequence += 1;
        self.bytes_downloaded = 0;
        self.last_checkpoint_time = Instant::now();
        self.max_download_speed = None;
        self.min_download_speed = None;
        self.avg_download_speed = 0.0;

        metrics
    }
}
