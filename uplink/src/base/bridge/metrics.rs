use serde::Serialize;
use std::time::Instant;

use crate::collector::utils;

#[derive(Debug, Serialize, Clone)]
pub struct StreamMetrics {
    timestamp: u128,
    sequence: u32,
    stream: String,
    point_count: usize,
    #[serde(skip_serializing)]
    batch_start_time: Instant,
    #[serde(skip_serializing)]
    total_latency: u64,
    batch_average_latency: u64,
    batch_min_latency: u64,
    batch_max_latency: u64,
    batch_count: u64,
}

impl StreamMetrics {
    pub fn new(name: &str) -> Self {
        StreamMetrics {
            stream: name.to_owned(),
            timestamp: 0,
            sequence: 0,
            point_count: 0,
            batch_start_time: Instant::now(),
            total_latency: 0,
            batch_average_latency: 0,
            batch_min_latency: 0,
            batch_max_latency: 0,
            batch_count: 0,
        }
    }

    pub fn add_point(&mut self) {
        self.point_count += 1;
        if self.point_count == 1 {
            self.timestamp = utils::clock();
        }
    }

    pub fn add_batch(&mut self) {
        self.batch_count += 1;

        let latency = self.batch_start_time.elapsed().as_millis() as u64;
        self.batch_max_latency = self.batch_max_latency.max(latency);
        self.batch_min_latency = self.batch_min_latency.min(latency);
        self.total_latency += latency;
        self.batch_average_latency = self.total_latency / self.batch_count;
    }

    pub fn reset(&mut self) {
        *self = StreamMetrics::new(&self.stream)
    }
}
