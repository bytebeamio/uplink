use std::{fmt::Debug, mem, sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{debug, trace};
use serde::Serialize;

use crate::base::StreamConfig;

use super::{Package, Point, StreamMetrics};

/// Signals status of stream buffer
#[derive(Debug)]
pub enum StreamStatus {
    Partial(usize),
    Flushed,
    Init(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
}

pub const MAX_BUFFER_SIZE: usize = 100;

#[derive(Debug)]
pub struct Stream<T> {
    pub name: Arc<String>,
    pub config: Arc<StreamConfig>,
    last_sequence: u32,
    last_timestamp: u64,
    buffer: Buffer<T>,
    tx: Sender<Box<dyn Package>>,
    pub metrics: StreamMetrics,
}

impl<T> Stream<T>
where
    T: Point,
    Buffer<T>: Package,
{
    pub fn new(
        stream_name: impl Into<String>,
        stream_config: StreamConfig,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let name = Arc::new(stream_name.into());
        let config = Arc::new(stream_config);
        let buffer = Buffer::new(name.clone(), config.clone());
        let metrics = StreamMetrics::new(&name, config.buf_size);

        Stream { name, config, last_sequence: 0, last_timestamp: 0, buffer, tx, metrics }
    }

    pub fn dynamic(
        stream_name: impl Into<String>,
        project_id: impl Into<String>,
        device_id: impl Into<String>,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let stream_name = stream_name.into();
        let project_id = project_id.into();
        let device_id = device_id.into();

        let topic = String::from("/tenants/")
            + &project_id
            + "/devices/"
            + &device_id
            + "/events/"
            + &stream_name
            + "/jsonarray";
        let config = StreamConfig { topic, ..Default::default() };

        Stream::new(stream_name, config, tx)
    }

    fn add(&mut self, data: T) -> Result<Option<Buffer<T>>, Error> {
        let current_sequence = data.sequence();
        let current_timestamp = data.timestamp();
        let last_sequence = self.last_sequence;
        let last_timestamp = self.last_timestamp;

        // Fill buffer with data
        self.buffer.buffer.push(data);
        self.metrics.add_point();

        // Anomaly detection
        if current_sequence <= self.last_sequence {
            debug!("Sequence number anomaly! [{current_sequence}, {last_sequence}");
            self.buffer.add_sequence_anomaly(self.last_sequence, current_sequence);
        }

        if current_timestamp < self.last_timestamp {
            debug!("Timestamp anomaly!! [{current_timestamp}, {last_timestamp}]",);
            self.buffer.add_timestamp_anomaly(self.last_timestamp, current_timestamp);
        }

        self.last_sequence = current_sequence;
        self.last_timestamp = current_timestamp;

        // if max_buffer_size is breached, flush
        let buf = if self.buffer.buffer.len() >= self.config.buf_size {
            self.metrics.add_batch();
            Some(self.take_buffer())
        } else {
            None
        };

        Ok(buf)
    }

    // Returns buffer content, replacing with empty buffer in-place
    fn take_buffer(&mut self) -> Buffer<T> {
        let name = self.name.clone();
        let config = self.config.clone();
        trace!("Flushing stream name: {}, topic: {}", name, config.topic);

        mem::replace(&mut self.buffer, Buffer::new(name, config))
    }

    /// Triggers flush and async channel send if not empty
    pub async fn flush(&mut self) -> Result<(), Error> {
        if !self.is_empty() {
            let buf = self.take_buffer();
            self.tx.send_async(Box::new(buf)).await?;
        }

        Ok(())
    }

    /// Returns number of elements in Stream buffer
    pub fn len(&self) -> usize {
        self.buffer.buffer.len()
    }

    /// Check if Stream buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fill buffer with data and trigger async channel send on breaching max_buf_size.
    /// Returns [`StreamStatus`].
    pub async fn fill(&mut self, data: T) -> Result<StreamStatus, Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send_async(Box::new(buf)).await?;
            return Ok(StreamStatus::Flushed);
        }

        let status = match self.len() {
            1 => StreamStatus::Init(self.config.flush_period),
            len => StreamStatus::Partial(len),
        };

        Ok(status)
    }

    #[cfg(test)]
    /// Push data into buffer and trigger sync channel send on max_buf_size.
    /// Returns [`StreamStatus`].
    pub fn push(&mut self, data: T) -> Result<StreamStatus, Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send(Box::new(buf))?;
            return Ok(StreamStatus::Flushed);
        }

        let status = match self.len() {
            1 => StreamStatus::Init(self.config.flush_period),
            len => StreamStatus::Partial(len),
        };

        Ok(status)
    }

    // pub fn metrics(&self) -> StreamMetrics {
    //     self.metrics.clone()
    // }
}

/// Buffer is an abstraction of a collection that serializer receives.
/// It also contains meta data to understand the type of data
/// e.g stream to mqtt topic mapping
/// Buffer doesn't put any restriction on type of `T`
#[derive(Debug)]
pub struct Buffer<T> {
    pub stream_name: Arc<String>,
    pub stream_config: Arc<StreamConfig>,
    pub buffer: Vec<T>,
    pub anomalies: String,
    pub anomaly_count: usize,
}

impl<T> Buffer<T> {
    pub fn new(stream_name: Arc<String>, stream_config: Arc<StreamConfig>) -> Buffer<T> {
        Buffer {
            buffer: Vec::with_capacity(stream_config.buf_size),
            stream_name,
            stream_config,
            anomalies: String::with_capacity(100),
            anomaly_count: 0,
        }
    }

    pub fn add_sequence_anomaly(&mut self, last: u32, current: u32) {
        self.anomaly_count += 1;
        if self.anomalies.len() >= 100 {
            return;
        }

        let error = self.stream_name.to_string()
            + ".sequence: "
            + &last.to_string()
            + ", "
            + &current.to_string();
        self.anomalies.push_str(&error)
    }

    pub fn add_timestamp_anomaly(&mut self, last: u64, current: u64) {
        self.anomaly_count += 1;
        if self.anomalies.len() >= 100 {
            return;
        }

        let error = "timestamp: ".to_owned() + &last.to_string() + ", " + &current.to_string();
        self.anomalies.push_str(&error)
    }

    pub fn anomalies(&self) -> Option<(String, usize)> {
        if self.anomalies.is_empty() {
            return None;
        }

        Some((self.anomalies.clone(), self.anomaly_count))
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
        self.anomalies()
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
            metrics: StreamMetrics::new(&self.name, self.config.buf_size),
            tx: self.tx.clone(),
        }
    }
}
