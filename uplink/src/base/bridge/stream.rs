use std::{sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{debug, trace};
use serde::Serialize;

use crate::config::StreamConfig;

use super::{Package, Point, StreamMetrics};

/// Represents the status of the stream buffer at various stages.
#[derive(Debug)]
pub enum StreamStatus {
    // Partially filled buffer(count of elements).
    Partial(usize),
    // Buffer has been flushed.
    Flushed,
    // Stream is initializing with a flush period.
    Init(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>), // Error encountered while sending data.
}

/// A threshold to define the maximum size a batch can reach before forced flush.
pub const MAX_BATCH_SIZE: usize = 100;

/// `Stream` buffers incoming data points and flushes them as a batch when the buffer is full.
#[derive(Debug)]
pub struct Stream<T> {
    pub name: Arc<String>,
    pub config: Arc<StreamConfig>,
    // Last seen sequence for anomaly detection.
    last_sequence: u32,
    // Last seen timestamp for anomaly detection.
    last_timestamp: u64,
    // The internal buffer that holds data points.
    buffer: Buffer<T>,
    // Channel to send flushed data to the next stage.
    tx: Sender<Box<dyn Package>>,
    // Collects and reports metrics for the stream.
    pub metrics: StreamMetrics,
}

impl<T> Stream<T>
where
    T: Point,
    Buffer<T>: Package,
{
    /// Creates a new `Stream` with specified configuration and channel.
    pub fn new(
        stream_name: impl Into<String>,
        stream_config: StreamConfig,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let name = Arc::new(stream_name.into());
        let config = Arc::new(stream_config);
        let buffer = Buffer::new(name.clone(), config.clone());
        let metrics = StreamMetrics::new(&name, config.batch_size);

        Stream { name, config, last_sequence: 0, last_timestamp: 0, buffer, tx, metrics }
    }

    /// Dynamically configures a new stream based on `project_id`, `device_id`, and `stream_name`.
    pub fn dynamic(
        stream_name: impl Into<String>,
        project_id: impl Into<String>,
        device_id: impl Into<String>,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let stream_name = stream_name.into();
        let project_id = project_id.into();
        let device_id = device_id.into();

        // Generate a topic from project and device details.
        let topic =
            format!("/tenants/{project_id}/devices/{device_id}/events/{stream_name}/jsonarray");
        let config = StreamConfig { topic, ..Default::default() };

        Stream::new(stream_name, config, tx)
    }

    /// Adds data to the buffer, detecting any sequence/timestamp anomalies. Triggers a flush if full.
    fn add(&mut self, data: T) -> Result<Option<Buffer<T>>, Error> {
        let current_sequence = data.sequence();
        let current_timestamp = data.timestamp();

        // Push data into the buffer.
        self.buffer.buffer.push(data);
        self.metrics.add_point();

        // Check for sequence anomalies.
        if current_sequence <= self.last_sequence {
            debug!("Sequence number anomaly detected!");
            self.buffer.add_sequence_anomaly(self.last_sequence, current_sequence);
        }

        // Check for timestamp anomalies.
        if current_timestamp < self.last_timestamp {
            debug!("Timestamp anomaly detected!");
            self.buffer.add_timestamp_anomaly(self.last_timestamp, current_timestamp);
        }

        // Update for next check.
        self.last_sequence = current_sequence;
        self.last_timestamp = current_timestamp;

        // Flush buffer if batch size is reached.
        if self.buffer.buffer.len() >= self.config.batch_size {
            self.metrics.add_batch();
            return Ok(Some(self.take_buffer()));
        }
        Ok(None)
    }

    /// Retrieves the buffer’s contents, replacing it with a new empty buffer.
    fn take_buffer(&mut self) -> Buffer<T> {
        let name = self.name.clone();
        let config = self.config.clone();
        trace!("Flushing stream buffer for topic: {}", config.topic);

        std::mem::replace(&mut self.buffer, Buffer::new(name, config))
    }

    /// Forces a flush if the buffer is not empty.
    pub async fn flush(&mut self) -> Result<(), Error> {
        if !self.is_empty() {
            let buf = self.take_buffer();
            self.tx.send_async(Box::new(buf)).await?;
        }
        Ok(())
    }

    /// Returns the current number of elements in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.buffer.len()
    }

    /// Checks if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds data to the buffer and flushes if the maximum batch size is exceeded.
    pub async fn fill(&mut self, data: T) -> Result<StreamStatus, Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send_async(Box::new(buf)).await?;
            return Ok(StreamStatus::Flushed);
        }

        // Status when buffer is not full.
        let status = match self.len() {
            1 => StreamStatus::Init(self.config.flush_period),
            len => StreamStatus::Partial(len),
        };

        Ok(status)
    }
}

/// `Buffer` collects data points and detects any anomalies in sequence or timestamp.
#[derive(Debug)]
pub struct Buffer<T> {
    pub stream_name: Arc<String>,
    pub stream_config: Arc<StreamConfig>,
    pub buffer: Vec<T>,
    pub anomalies: String,
    pub anomaly_count: usize,
}

impl<T> Buffer<T> {
    /// Creates a new buffer with specified stream name and configuration.
    pub fn new(stream_name: Arc<String>, stream_config: Arc<StreamConfig>) -> Buffer<T> {
        Buffer {
            buffer: Vec::with_capacity(stream_config.batch_size),
            stream_name,
            stream_config,
            anomalies: String::with_capacity(100),
            anomaly_count: 0,
        }
    }

    /// Logs a sequence anomaly.
    pub fn add_sequence_anomaly(&mut self, last: u32, current: u32) {
        self.anomaly_count += 1;
        if self.anomalies.len() < 100 {
            self.anomalies.push_str(&format!("{}.sequence: {last}, {current}", self.stream_name));
        }
    }

    /// Logs a timestamp anomaly.
    pub fn add_timestamp_anomaly(&mut self, last: u64, current: u64) {
        self.anomaly_count += 1;
        if self.anomalies.len() < 100 {
            self.anomalies.push_str(&format!("timestamp: {last}, {current}"));
        }
    }
}

impl<T> Package for Buffer<T>
where
    T: Point,
    Vec<T>: Serialize,
{
    fn stream_config(&self) -> Arc<StreamConfig> {
        self.stream_config.clone()
    }

    fn stream_name(&self) -> Arc<String> {
        self.stream_name.clone()
    }

    fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.buffer)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        if self.anomalies.is_empty() {
            return None;
        }

        Some((self.anomalies.clone(), self.anomaly_count))
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn latency(&self) -> u64 {
        0
    }
}

impl<T> Clone for Stream<T> {
    fn clone(&self) -> Self {
        Stream {
            name: self.name.clone(),
            config: self.config.clone(),
            last_sequence: 0,
            last_timestamp: 0,
            buffer: Buffer::new(self.buffer.stream_name.clone(), self.buffer.stream_config.clone()),
            metrics: StreamMetrics::new(&self.name, self.config.batch_size),
            tx: self.tx.clone(),
        }
    }
}
