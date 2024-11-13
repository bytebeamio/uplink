use std::time::Instant;

use serde::Serialize;

use crate::base::clock;

/// Metrics collected for a stream, tracking batch statistics and latency information.
#[derive(Debug, Serialize, Clone)]
pub struct StreamMetrics {
    // Timestamp for the first point in the current sequence.
    pub timestamp: u128,
    // Sequence number, incremented for each new batch.
    pub sequence: u32,
    // Name of the stream being tracked.
    pub stream: String,
    // Total points collected in the current batch.
    pub points: usize,
    // Count of batches processed.
    pub batches: u64,
    // Maximum allowed points per batch.
    pub max_batch_points: usize,
    // Start time for the current batch, used for latency.
    #[serde(skip_serializing)]
    pub batch_start_time: Instant,
    // Sum of latencies across all batches.
    #[serde(skip_serializing)]
    pub total_latency: u64,
    // Minimum latency observed in any batch.
    pub min_batch_latency: u64,
    // Maximum latency observed in any batch.
    pub max_batch_latency: u64,
    // Average latency across all batches.
    pub average_batch_latency: u64,
}

impl StreamMetrics {
    /// Creates a new `StreamMetrics` instance with a specified stream name and max points per batch.
    pub fn new(name: &str, max_batch_points: usize) -> Self {
        StreamMetrics {
            stream: name.to_owned(),
            timestamp: clock(),
            sequence: 1,
            points: 0,
            batches: 0,
            max_batch_points,
            batch_start_time: Instant::now(),
            total_latency: 0,
            average_batch_latency: 0,
            min_batch_latency: u64::MAX, // Initialized to max to track minimum latency.
            max_batch_latency: 0,
        }
    }

    /// Returns the stream name.
    pub fn stream(&self) -> &String {
        &self.stream
    }

    /// Returns the number of points collected in the current batch.
    pub fn points(&self) -> usize {
        self.points
    }

    /// Adds a point to the current batch, updating the timestamp for the first point.
    pub fn add_point(&mut self) {
        self.points += 1;
        // Set the timestamp to the current time if this is the first point.
        if self.points == 1 {
            self.timestamp = clock();
        }
    }

    /// Adds a batch, updating latency metrics (min, max, average).
    pub fn add_batch(&mut self) {
        self.batches += 1;

        // Calculate the current batch latency.
        let latency = self.batch_start_time.elapsed().as_millis() as u64;

        // Update max, min, total, and average latencies.
        self.max_batch_latency = self.max_batch_latency.max(latency);
        self.min_batch_latency = self.min_batch_latency.min(latency);
        self.total_latency += latency;
        self.average_batch_latency = self.total_latency / self.batches;
    }

    /// Prepares the metrics for the next batch or sequence by resetting counters.
    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.points = 0;
        self.batches = 0;
        self.batch_start_time = Instant::now();
        self.total_latency = 0;
        self.min_batch_latency = u64::MAX;
        self.max_batch_latency = 0;
        self.average_batch_latency = 0;
    }
}
