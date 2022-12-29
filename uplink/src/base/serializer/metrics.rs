use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::Config;

#[derive(Debug, Default, Serialize, Clone)]
pub struct SerializerMetrics {
    #[serde(skip)]
    pub topic: String,
    sequence: u32,
    timestamp: u64,
    total_sent_size: usize,
    total_disk_size: usize,
    lost_segments: usize,
    errors: String,
    error_count: usize,
}

impl SerializerMetrics {
    pub fn new(config: Arc<Config>) -> Option<Self> {
        let topic = match &config.serializer_metrics.as_ref()?.topic {
            Some(topic) => topic.to_owned(),
            _ => {
                String::from("/tenants/")
                    + &config.project_id
                    + "/devices/"
                    + &config.device_id
                    + "/events/metrics/jsonarray"
            }
        };

        Some(Self { topic, errors: String::with_capacity(1024), ..Default::default() })
    }

    pub fn add_total_sent_size(&mut self, size: usize) {
        self.total_sent_size = self.total_sent_size.saturating_add(size);
    }

    pub fn add_total_disk_size(&mut self, size: usize) {
        self.total_disk_size = self.total_disk_size.saturating_add(size);
    }

    pub fn sub_total_disk_size(&mut self, size: usize) {
        self.total_disk_size = self.total_disk_size.saturating_sub(size);
    }

    pub fn increment_lost_segments(&mut self) {
        self.lost_segments += 1;
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
        self.error_count += count;
        if self.errors.len() > 1024 {
            return;
        }

        self.errors.push_str(&error.into());
        self.errors.push_str(" | ");
    }

    pub fn update(&mut self) -> Self {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        self.timestamp = timestamp.as_millis() as u64;
        self.sequence += 1;

        self.clone()
    }

    pub fn clear(&mut self) {
        self.errors.clear();
        self.lost_segments = 0;
    }
}
