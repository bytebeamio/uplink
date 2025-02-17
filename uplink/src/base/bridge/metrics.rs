use serde::Serialize;
use std::time::Instant;
use serde_json::json;
use crate::base::bridge::Payload;
use crate::base::clock;

#[derive(Debug, Serialize, Clone)]
pub struct StreamMetrics {
    pub timestamp: u64,
    pub sequence: u32,
    pub stream: String,
    pub points: usize,
    pub batches: u64,
    pub max_batch_points: usize,
    #[serde(skip_serializing)]
    pub batch_start_time: Instant,
    #[serde(skip_serializing)]
    pub total_latency: u64,
    pub min_batch_latency: u64,
    pub max_batch_latency: u64,
    pub average_batch_latency: u64,
}

impl StreamMetrics {
    pub fn new(stream: String, max_batch_points: usize) -> Self {
        StreamMetrics {
            stream,
            timestamp: clock(),
            sequence: 1,
            points: 0,
            batches: 0,
            max_batch_points,
            batch_start_time: Instant::now(),
            total_latency: 0,
            average_batch_latency: 0,
            min_batch_latency: 0,
            max_batch_latency: 0,
        }
    }

    pub fn add_point(&mut self) {
        self.points += 1;
        if self.points == 1 {
            self.timestamp = clock();
        }
    }

    pub fn add_batch(&mut self) {
        self.batches += 1;

        let latency = self.batch_start_time.elapsed().as_millis() as u64;
        self.max_batch_latency = self.max_batch_latency.max(latency);
        self.min_batch_latency = self.min_batch_latency.min(latency);
        self.total_latency += latency;
        self.average_batch_latency = self.total_latency / self.batches;
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.batches = 0;
        self.points = 0;
        self.batches = 0;
        self.batch_start_time = Instant::now();
        self.total_latency = 0;
        self.min_batch_latency = 0;
        self.max_batch_latency = 0;
        self.average_batch_latency = 0;
    }

    pub fn to_payload(&self) -> Payload {
        Payload {
            stream: "uplink_stream_metrics".to_owned(),
            sequence: self.sequence,
            timestamp: self.timestamp,
            payload: json!{{
                "stream": self.stream,
                "points": self.points,
                "batches": self.batches,
                "max_batch_points": self.max_batch_points,
                "min_batch_latency": self.min_batch_latency,
                "max_batch_latency": self.max_batch_latency,
                "average_batch_latency": self.average_batch_latency,
            }},
        }
    }
}
