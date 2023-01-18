use serde::Serialize;
use std::time::Instant;

use crate::collector::utils;

#[derive(Debug, Serialize, Clone)]
pub struct StreamMetrics {
    timestamp: u128,
    sequence: u32,
    stream: String,
    point_count: usize,
    batch_count: u64,
    max_batch_count: usize,
    #[serde(skip_serializing)]
    batch_start_time: Instant,
    #[serde(skip_serializing)]
    total_latency: u64,
    min_batch_latency: u64,
    max_batch_latency: u64,
    average_batch_latency: u64,
}

impl StreamMetrics {
    pub fn new(name: &str, max_batch_count: usize) -> Self {
        StreamMetrics {
            stream: name.to_owned(),
            timestamp: 0,
            sequence: 0,
            point_count: 0,
            batch_count: 0,
            max_batch_count,
            batch_start_time: Instant::now(),
            total_latency: 0,
            average_batch_latency: 0,
            min_batch_latency: 0,
            max_batch_latency: 0,
        }
    }

    pub fn stream(&self) -> &String {
        &self.stream
    }

    pub fn point_count(&self) -> usize {
        self.point_count
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
        self.max_batch_latency = self.max_batch_latency.max(latency);
        self.min_batch_latency = self.min_batch_latency.min(latency);
        self.total_latency += latency;
        self.average_batch_latency = self.total_latency / self.batch_count;
    }

    pub fn reset(&mut self) {
        *self = StreamMetrics::new(&self.stream, self.max_batch_count)
    }
}
