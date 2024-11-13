use std::time::Duration;

use serde::Serialize;
use serde_with::{serde_as, DurationSecondsWithFrac};

use crate::base::clock;

/// `Metrics` stores information about the `Serializer`'s operational metrics,
/// such as data batches processed, errors encountered, and storage usage.
#[derive(Debug, Serialize, Clone)]
pub struct Metrics {
    /// Timestamp of the last metrics update
    timestamp: u128,
    /// Sequence number for metrics snapshots
    sequence: u32,
    /// Operational mode of the serializer (e.g., "Catchup", "Normal", "Slow", or "Crash")
    pub mode: String,
    /// Number of data batches serialized
    pub batches: usize,
    /// Size of the write memory buffer in bytes
    pub write_memory: usize,
    /// Size of the read memory buffer in bytes
    pub read_memory: usize,
    /// Number of files written to disk
    pub disk_files: usize,
    /// Total disk space utilized by persistence files in bytes
    pub disk_utilized: usize,
    /// Number of persistence files that were deleted before being consumed
    pub lost_segments: usize,
    /// Number of errors encountered
    pub errors: usize,
    /// Total bytes of serialized data sent to the network
    pub sent_size: usize,
}

impl Metrics {
    /// Creates a new `Metrics` instance, initializing with the given mode and setting default values.
    pub fn new(mode: &str) -> Self {
        Metrics {
            timestamp: clock(),
            sequence: 1,
            mode: mode.to_owned(),
            batches: 0,
            write_memory: 0,
            read_memory: 0,
            disk_files: 0,
            disk_utilized: 0,
            lost_segments: 0,
            errors: 0,
            sent_size: 0,
        }
    }

    /// Returns count of batches processed
    pub fn get_batch_count(&self) -> usize {
        self.batches
    }

    /// Adds one to the batch count, updating the timestamp if this is the first batch.
    pub fn add_batch(&mut self) {
        self.batches += 1;
        if self.batches == 1 {
            self.timestamp = clock();
        }
    }

    /// Resets the metrics in preparation for the next cycle or snapshot.
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

    // Setter methods for updating specific metrics

    pub fn set_mode(&mut self, mode: &str) {
        mode.clone_into(&mut self.mode);
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

    pub fn set_disk_utilized(&mut self, bytes: usize) {
        self.disk_utilized = bytes;
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
}

#[serde_as]
#[derive(Debug, Serialize, Clone)]
/// `StreamMetrics` captures per-stream data metrics, including sizes and durations for serialization and compression.
pub struct StreamMetrics {
    /// Timestamp of the metrics snapshot
    pub timestamp: u128,
    /// Sequence number for metrics snapshots
    pub sequence: u32,
    /// Stream identifier
    pub stream: String,
    /// Total size of serialized data in bytes
    pub serialized_data_size: usize,
    /// Total size of compressed data in bytes
    pub compressed_data_size: usize,
    /// Number of serialization operations
    #[serde(skip)]
    pub serializations: u32,
    /// Cumulative serialization time
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_serialization_time: Duration,
    /// Average time per serialization operation
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub avg_serialization_time: Duration,
    /// Number of compression operations
    #[serde(skip)]
    pub compressions: u32,
    /// Cumulative compression time
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_compression_time: Duration,
    /// Average time per compression operation
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub avg_compression_time: Duration,
}

impl StreamMetrics {
    /// Creates a new `StreamMetrics` instance for the given stream.
    pub fn new(stream_name: &str) -> Self {
        StreamMetrics {
            stream: stream_name.to_owned(),
            timestamp: clock(),
            sequence: 1,
            serialized_data_size: 0,
            compressed_data_size: 0,
            serializations: 0,
            total_serialization_time: Duration::ZERO,
            avg_serialization_time: Duration::ZERO,
            compressions: 0,
            total_compression_time: Duration::ZERO,
            avg_compression_time: Duration::ZERO,
        }
    }

    /// Adds the size of serialized data and optional compressed data.
    pub fn add_serialized_sizes(&mut self, data_size: usize, compressed_data_size: Option<usize>) {
        self.serialized_data_size += data_size;
        self.compressed_data_size += compressed_data_size.unwrap_or(data_size);
    }

    /// Adds a duration to the total serialization time and increments the serialization count.
    pub fn add_serialization_time(&mut self, serialization_time: Duration) {
        self.serializations += 1;
        self.total_serialization_time += serialization_time;
    }

    /// Adds a duration to the total compression time and increments the compression count.
    pub fn add_compression_time(&mut self, compression_time: Duration) {
        self.compressions += 1;
        self.total_compression_time += compression_time;
    }

    /// Prepares metrics for snapshotting by calculating averages for serialization and compression times.
    /// This should be called before serialization to avoid unnecessary calculations on each update.
    pub fn prepare_snapshot(&mut self) {
        self.avg_serialization_time = self
            .total_serialization_time
            .checked_div(self.serializations)
            .unwrap_or(Duration::ZERO);
        self.avg_compression_time =
            self.total_compression_time.checked_div(self.compressions).unwrap_or(Duration::ZERO);
    }

    /// Resets metrics for the next snapshot cycle.
    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.serialized_data_size = 0;
        self.compressed_data_size = 0;
        self.serializations = 0;
        self.total_serialization_time = Duration::ZERO;
        self.avg_serialization_time = Duration::ZERO;
        self.compressions = 0;
        self.total_compression_time = Duration::ZERO;
        self.avg_compression_time = Duration::ZERO;
    }
}

/// Enum to handle main and per-stream metrics for the serializer
pub enum SerializerMetrics {
    Main(Box<Metrics>),
    Stream(Box<StreamMetrics>),
}
