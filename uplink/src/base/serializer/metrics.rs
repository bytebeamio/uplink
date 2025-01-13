use std::time::Duration;

use serde::Serialize;
use serde_with::{serde_as, DurationSecondsWithFrac};

use crate::base::clock;

/// Metrics information relating to the operation of the `Serializer`, all values are reset on metrics flush
/// Last 5 fields are derived from data given by the `storage::Storage` implementations
#[derive(Debug, Serialize, Clone)]
pub struct Metrics {
    pub timestamp: u128,
    pub sequence: u32,
    /// One of "catchup", "normal", or "slow"
    pub mode: &'static str,
    /// Number of batches serialized
    pub batches: usize,
    /// Size in bytes, of serialized data sent onto network
    pub sent_size: usize,
    /// Number of errors faced during serializer operation
    pub errors: usize,
    /// Size of the write memory buffer within `Storage`
    pub write_memory: usize,
    /// Size of the read memory buffer within `Storage`
    pub read_memory: usize,
    /// Number of files that have been written to disk
    pub disk_files: usize,
    /// Disk size currently occupied by persistence files
    pub disk_utilized: usize,
    /// Nuber of persistence files that had to deleted before being consumed
    pub lost_segments: usize,
}

impl Metrics {
    pub fn new(mode: &'static str) -> Self {
        Metrics {
            timestamp: clock(),
            sequence: 1,
            mode,
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
}

#[serde_as]
#[derive(Debug, Serialize, Clone)]
pub struct StreamMetrics {
    pub timestamp: u128,
    pub sequence: u32,
    pub stream: String,
    pub serialized_data_size: usize,
    pub compressed_data_size: usize,
    #[serde(skip)]
    pub serializations: u32,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_serialization_time: Duration,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub avg_serialization_time: Duration,
    #[serde(skip)]
    pub compressions: u32,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_compression_time: Duration,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub avg_compression_time: Duration,
}

impl StreamMetrics {
    pub fn new(name: &str) -> Self {
        StreamMetrics {
            stream: name.to_owned(),
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

    pub fn add_serialized_sizes(&mut self, data_size: usize, compressed_data_size: Option<usize>) {
        self.serialized_data_size += data_size;
        self.compressed_data_size += compressed_data_size.unwrap_or(data_size);
    }

    pub fn add_serialization_time(&mut self, serialization_time: Duration) {
        self.serializations += 1;
        self.total_serialization_time += serialization_time;
    }

    pub fn add_compression_time(&mut self, compression_time: Duration) {
        self.compressions += 1;
        self.total_compression_time += compression_time;
    }

    // Should be called before serializing metrics to ensure averages are computed.
    // Averages aren't calculated for ever `add_*` call to save on costs.
    pub fn prepare_snapshot(&mut self) {
        self.avg_serialization_time = self
            .total_serialization_time
            .checked_div(self.serializations)
            .unwrap_or(Duration::ZERO);
        self.avg_compression_time =
            self.total_compression_time.checked_div(self.compressions).unwrap_or(Duration::ZERO);
    }

    pub fn prepare_next(&mut self) {
        self.timestamp = clock();
        self.sequence += 1;
        self.serialized_data_size = 0;
        self.compressed_data_size = 0;
    }
}

pub enum SerializerMetrics {
    Main(Box<Metrics>),
    Stream(Box<StreamMetrics>),
}
