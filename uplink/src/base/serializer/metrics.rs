use serde::Serialize;

use crate::collector::utils::{self, clock};

/// Metrics information relating to the operation of the `Serializer`, all values are reset on metrics flush
#[derive(Debug, Serialize, Clone)]
pub struct SerializerMetrics {
    timestamp: u128,
    sequence: u32,
    /// One of **Catchup**, **Normal**, **Slow** or **Crash**
    pub mode: String,
    /// Number of batches serialized
    pub batches: usize,
    /// Size of the write memory buffer within `Storage`
    pub write_memory: usize,
    /// Size of the read memory buffer within `Storage`
    pub read_memory: usize,
    /// Number of files that have been written to disk
    pub disk_files: usize,
    /// Nuber of persistence files that had to deleted before being consumed
    pub lost_segments: usize,
    /// Number of errors faced during serializer operation
    pub errors: usize,
    /// Size in bytes, of serialized data sent onto network
    pub sent_size: usize,
}

impl SerializerMetrics {
    pub fn new(mode: &str) -> Self {
        SerializerMetrics {
            timestamp: clock(),
            sequence: 1,
            mode: mode.to_owned(),
            batches: 0,
            write_memory: 0,
            read_memory: 0,
            disk_files: 0,
            lost_segments: 0,
            errors: 0,
            sent_size: 0,
        }
    }

    pub fn set_mode(&mut self, name: &str) {
        self.mode = name.to_owned();
    }

    pub fn batches(&self) -> usize {
        self.batches
    }

    pub fn add_batch(&mut self) {
        self.batches += 1;
        if self.batches == 1 {
            self.timestamp = utils::clock();
        }
    }

    pub fn set_write_memory(&mut self, size: usize) {
        self.write_memory = size;
    }

    pub fn set_read_memory(&mut self, size: usize) {
        self.read_memory = size;
    }

    pub fn set_disk_files(&mut self, count: usize) {
        self.disk_files = count;
    }

    pub fn increment_errors(&mut self) {
        self.errors += 1;
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
        self.batches = 0;
        self.write_memory = 0;
        self.read_memory = 0;
        self.disk_files = 0;
        self.lost_segments = 0;
        self.sent_size = 0;
        self.errors = 0;
    }
}
