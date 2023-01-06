use serde::Serialize;
use std::collections::hash_map::ValuesMut;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Default, Serialize, Clone)]
pub struct StreamMetrics {
    timestamp: u64,
    sequence: u32,
    stream: String,
    point_count: usize,
    batch_count: u64,
    average_latency: u64,
    min_latency: u64,
    max_latency: u64,
}

pub struct StreamMetricsHandler {
    pub topic: String,
    blacklist: Vec<String>,
    map: HashMap<String, StreamMetrics>,
}

impl StreamMetricsHandler {
    pub fn new(topic: String, blacklist: Vec<String>) -> Self {
        Self { topic, blacklist, map: Default::default() }
    }

    /// Updates the metrics for a stream as deemed necessary with the count of points in batch
    /// and the difference between first and last elements timestamp as latency being inputs.
    pub fn update(&mut self, stream: String, point_count: usize, batch_latency: u64) {
        if self.blacklist.contains(&stream) {
            return;
        }

        // Init stream metrics max/min values with opposite extreme values to ensure first latency value is accepted
        let metrics = self.map.entry(stream.clone()).or_insert(StreamMetrics {
            stream,
            min_latency: u64::MAX,
            max_latency: u64::MIN,
            ..Default::default()
        });

        metrics.max_latency = metrics.max_latency.max(batch_latency);
        metrics.min_latency = metrics.min_latency.min(batch_latency);
        // NOTE: Average latency is calculated in a slightly lossy fashion,
        let total_latency = (metrics.average_latency * metrics.batch_count) + batch_latency;

        metrics.batch_count += 1;
        metrics.point_count += point_count;
        metrics.average_latency = total_latency / metrics.batch_count;
    }

    pub fn streams(&mut self) -> Streams {
        Streams { values: self.map.values_mut() }
    }

    pub fn clear(&mut self) {
        self.map.clear();
    }
}

pub struct Streams<'a> {
    values: ValuesMut<'a, String, StreamMetrics>,
}

impl<'a> Iterator for Streams<'a> {
    type Item = &'a mut StreamMetrics;

    fn next(&mut self) -> Option<Self::Item> {
        let metrics = self.values.next()?;
        metrics.sequence += 1;
        metrics.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

        Some(metrics)
    }
}
