use std::collections::hash_map::ValuesMut;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::Arc};

use serde::Serialize;

use crate::base::MetricsConfig;
use crate::Config;

#[derive(Debug, Default, Serialize, Clone)]
pub struct SerializerMetrics {
    sequence: u32,
    timestamp: u64,
    total_sent_size: usize,
    total_disk_size: usize,
    lost_segments: usize,
}

pub struct SerializerMetricsHandler {
    pub topic: String,
    metrics: SerializerMetrics,
}

impl SerializerMetricsHandler {
    pub fn new(config: Arc<Config>) -> Option<Self> {
        let topic = match &config.serializer_metrics {
            MetricsConfig { enabled: false, .. } => return None,
            MetricsConfig { topic: Some(topic), .. } => topic.to_owned(),
            _ => {
                String::from("/tenants/")
                    + &config.project_id
                    + "/devices/"
                    + &config.device_id
                    + "/events/serializer_metrics/jsonarray"
            }
        };

        Some(Self { topic, metrics: Default::default() })
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

    // Retrieve metrics to send on network
    pub fn update(&mut self) -> &SerializerMetrics {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        self.metrics.timestamp = timestamp.as_millis() as u64;
        self.metrics.sequence += 1;

        &self.metrics
    }

    pub fn clear(&mut self) {
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
    anomalies: String,
    anomaly_count: usize,
}

pub struct StreamMetricsHandler {
    pub topic: String,
    // Used to set sequence number for stream_metrics messages
    sequence: u32,
    map: HashMap<String, StreamMetrics>,
}

impl StreamMetricsHandler {
    pub fn new(config: Arc<Config>) -> Option<Self> {
        let topic = match &config.stream_metrics {
            MetricsConfig { enabled: false, .. } => return None,
            MetricsConfig { topic: Some(topic), .. } => topic.to_owned(),
            _ => {
                String::from("/tenants/")
                    + &config.project_id
                    + "/devices/"
                    + &config.device_id
                    + "/events/stream_metrics/jsonarray"
            }
        };

        Some(Self { topic, map: Default::default(), sequence: 0 })
    }

    /// Updates the metrics for a stream as deemed necessary with the count of points in batch,
    /// the difference between first and last elements timestamp as latency, details of anomalies
    /// detected at collection, being the inputs.
    pub fn update(
        &mut self,
        stream: String,
        point_count: usize,
        batch_latency: u64,
        anomalies: Option<(String, usize)>,
    ) {
        // Init stream metrics max/min values with opposite extreme values to ensure first latency value is accepted
        let entry = self.map.entry(stream.clone()).or_insert(StreamMetrics {
            stream,
            min_latency: u64::MAX,
            max_latency: u64::MIN,
            ..Default::default()
        });

        entry.max_latency = entry.max_latency.max(batch_latency);
        entry.min_latency = entry.min_latency.min(batch_latency);
        // NOTE: Average latency is calculated in a slightly lossy fashion,
        let total_latency = (entry.average_latency * entry.batch_count) + batch_latency;

        entry.batch_count += 1;
        entry.point_count += point_count;
        entry.average_latency = total_latency / entry.batch_count;

        if let Some((anomalies, anomaly_count)) = anomalies {
            entry.anomaly_count += anomaly_count;
            let suffix = anomalies + ", ";
            entry.anomalies.push_str(&suffix);
        }
    }

    pub fn streams(&mut self) -> Metrics {
        Metrics { sequence: self.sequence, values: self.map.values_mut() }
    }

    pub fn clear(&mut self) {
        self.sequence += self.map.len() as u32;
        self.map.clear();
    }
}

pub struct Metrics<'a> {
    sequence: u32,
    values: ValuesMut<'a, String, StreamMetrics>,
}

impl<'a> Iterator for Metrics<'a> {
    type Item = &'a mut StreamMetrics;

    fn next(&mut self) -> Option<Self::Item> {
        let metrics = self.values.next()?;
        self.sequence += 1;
        metrics.sequence = self.sequence;
        metrics.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

        Some(metrics)
    }
}
