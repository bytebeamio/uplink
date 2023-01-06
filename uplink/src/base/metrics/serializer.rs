use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Default, Serialize, Clone)]
pub struct SerializerMetrics {
    sequence: u32,
    timestamp: u64,
    total_sent_size: usize,
    total_disk_size: usize,
    lost_segments: usize,
    errors: String,
    error_count: usize,
}

pub struct SerializerMetricsHandler {
    pub topic: String,
    metrics: SerializerMetrics,
}

impl SerializerMetricsHandler {
    pub fn new(topic: String) -> Self {
        let metrics =
            SerializerMetrics { errors: String::with_capacity(1024), ..Default::default() };
        Self { topic, metrics }
    }

    pub fn add_total_sent_size(&mut self, size: usize) {
        self.metrics.total_sent_size = self.metrics.total_sent_size.saturating_add(size);
    }

    pub fn add_total_disk_size(&mut self, size: usize) {
        self.metrics.total_disk_size = self.metrics.total_disk_size.saturating_add(size);
    }

    pub fn sub_total_disk_size(&mut self, size: usize) {
        self.metrics.total_disk_size = self.metrics.total_disk_size.saturating_sub(size);
    }

    pub fn increment_lost_segments(&mut self) {
        self.metrics.lost_segments += 1;
    }

    // pub fn add_error<S: Into<String>>(&mut self, error: S) {
    //     self.error_count += 1;
    //     if self.errors.len() > 1024 {
    //         return;
    //     }
    //
    //     self.errors.push_str(", ");
    //     self.errors.push_str(&error.into());
    // }

    pub fn add_errors<S: Into<String>>(&mut self, error: S, count: usize) {
        self.metrics.error_count += count;
        if self.metrics.errors.len() > 1024 {
            return;
        }

        self.metrics.errors.push_str(&error.into());
        self.metrics.errors.push_str(" | ");
    }

    // Retrieve metrics to send on network
    pub fn update(&mut self) -> &SerializerMetrics {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        self.metrics.timestamp = timestamp.as_millis() as u64;
        self.metrics.sequence += 1;

        &self.metrics
    }

    pub fn clear(&mut self) {
        self.metrics.errors.clear();
        self.metrics.lost_segments = 0;
    }
}
