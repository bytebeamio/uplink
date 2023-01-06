use serde::Serialize;

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

impl StreamMetrics {
    pub fn new(topic: String, blacklist: Vec<String>) -> Self {
        StreamMetrics { ..Default::default() }
    }

    /// Updates the metrics for a stream as deemed necessary with the count of points in batch
    /// and the difference between first and last elements timestamp as latency being inputs.
    pub fn update(&mut self, stream: String, point_count: usize, batch_latency: u64) {
        self.max_latency = self.max_latency.max(batch_latency);
        self.min_latency = self.min_latency.min(batch_latency);
        // NOTE: Average latency is calculated in a slightly lossy fashion,
        let total_latency = (self.average_latency * self.batch_count) + batch_latency;

        self.batch_count += 1;
        self.point_count += point_count;
        self.average_latency = total_latency / self.batch_count;
    }
}
