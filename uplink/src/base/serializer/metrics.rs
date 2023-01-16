use serde::Serialize;

use crate::collector::utils::{self, clock};

#[derive(Debug, Serialize, Clone)]
pub struct SerializerMetrics {
    pub timestamp: u128,
    pub sequence: u32,
    mode: String,
    batch_count: usize,
    memory_size: usize,
    disk_files: usize,
    lost_segments: usize,
    sent_size: usize,
}

impl SerializerMetrics {
    pub fn new(mode: &str) -> Self {
        SerializerMetrics {
            timestamp: clock(),
            sequence: 1,
            mode: mode.to_owned(),
            batch_count: 0,
            memory_size: 0,
            disk_files: 0,
            lost_segments: 0,
            sent_size: 0,
        }
    }

    pub fn set_mode(&mut self, name: &str) {
        self.mode = name.to_owned();
    }

    pub fn batch_count(&self) -> usize {
        self.batch_count
    }

    pub fn add_batch(&mut self) {
        self.batch_count += 1;
        if self.batch_count == 1 {
            self.timestamp = utils::clock();
        }
    }

    pub fn set_memory_size(&mut self, size: usize) {
        self.memory_size = size;
    }

    pub fn set_disk_files(&mut self, count: usize) {
        self.disk_files = count;
    }

    pub fn increment_lost_segments(&mut self) {
        self.lost_segments += 1;
    }

    pub fn add_sent_size(&mut self, size: usize) {
        self.sent_size += size;
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.batch_count = 0;
        self.memory_size = 0;
        self.disk_files = 0;
        self.lost_segments = 0;
        self.sent_size = 0;
    }
}
