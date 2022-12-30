use std::collections::hash_map::ValuesMut;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::Arc};

use serde::Serialize;

use crate::Config;

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

        let metrics =
            SerializerMetrics { errors: String::with_capacity(1024), ..Default::default() };

        Some(Self { topic, metrics })
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
    map: HashMap<String, StreamMetrics>,
}

impl StreamMetricsHandler {
    pub fn new(config: Arc<Config>) -> Option<Self> {
        let topic = match &config.stream_metrics.as_ref()?.topic {
            Some(topic) => topic.to_owned(),
            _ => {
                String::from("/tenants/")
                    + &config.project_id
                    + "/devices/"
                    + &config.device_id
                    + "/events/stream_metrics/jsonarray"
            }
        };

        Some(Self { topic, map: Default::default() })
    }

    pub fn update(&mut self, stream: String, point_count: usize, batch_latency: u64) {
        // Init stream metrics max/min values with opposite extreme values to ensure first latency value is accepted
        let metrics = self.map.entry(stream.clone()).or_insert(StreamMetrics {
            stream,
            min_latency: u64::MAX,
            max_latency: u64::MIN,
            ..Default::default()
        });

        metrics.max_latency = metrics.max_latency.max(batch_latency);
        metrics.min_latency = metrics.min_latency.min(batch_latency);
        let total_latency = (metrics.average_latency * metrics.batch_count) + batch_latency;

        metrics.batch_count += 1;
        metrics.point_count += point_count;
        metrics.average_latency = total_latency / metrics.batch_count;
    }

    pub fn streams(&mut self) -> Streams {
        Streams { values: self.map.values_mut() }
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
