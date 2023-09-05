use std::{fmt::Debug, mem, sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{debug, trace};
use serde::Serialize;

use crate::base::{Compression, StreamConfig, DEFAULT_TIMEOUT};

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
    pub max_buffer_size: usize,
    pub flush_period: Duration,
    topic: Arc<String>,
    last_sequence: u32,
    last_timestamp: u64,
    buffer: Buffer<T>,
    tx: Sender<Box<dyn Package>>,
    pub metrics: StreamMetrics,
    compression: Compression,
}

impl<T> Stream<T>
where
    T: Point + Debug + Send + 'static,
    Buffer<T>: Package,
{
    pub fn new(
        stream: impl Into<String>,
        topic: impl Into<String>,
        max_buffer_size: usize,
        tx: Sender<Box<dyn Package>>,
        compression: Compression,
    ) -> Stream<T> {
        let name = Arc::new(stream.into());
        let topic = Arc::new(topic.into());
        let buffer = Buffer::new(name.clone(), topic.clone(), compression);
        let flush_period = Duration::from_secs(DEFAULT_TIMEOUT);
        let metrics = StreamMetrics::new(&name, max_buffer_size);

        Stream {
            name,
            max_buffer_size,
            flush_period,
            topic,
            last_sequence: 0,
            last_timestamp: 0,
            buffer,
            tx,
            metrics,
            compression,
        }
    }

    pub fn with_config(
        name: &String,
        config: &StreamConfig,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let mut stream = Stream::new(name, &config.topic, config.buf_size, tx, config.compression);
        stream.flush_period = Duration::from_secs(config.flush_period);
        stream
    }

    pub fn dynamic(
        stream: impl Into<String>,
        project_id: impl Into<String>,
        device_id: impl Into<String>,
        max_buffer_size: usize,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let stream = stream.into();
        let project_id = project_id.into();
        let device_id = device_id.into();

        let topic = String::from("/tenants/")
            + &project_id
            + "/devices/"
            + &device_id
            + "/events/"
            + &stream
            + "/jsonarray";

        Stream::new(stream, topic, max_buffer_size, tx, Compression::Disabled)
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
        let buf = if self.buffer.buffer.len() >= self.max_buffer_size {
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
        let topic = self.topic.clone();
        trace!("Flushing stream name: {}, topic: {}", name, topic);

        mem::replace(&mut self.buffer, Buffer::new(name, topic, self.compression))
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
            1 => StreamStatus::Init(self.flush_period),
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
            1 => StreamStatus::Init(self.flush_period),
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
    pub stream: Arc<String>,
    pub topic: Arc<String>,
    pub buffer: Vec<T>,
    pub anomalies: String,
    pub anomaly_count: usize,
    pub compression: Compression,
}

impl<T> Buffer<T> {
    pub fn new(stream: Arc<String>, topic: Arc<String>, compression: Compression) -> Buffer<T> {
        Buffer {
            stream,
            topic,
            buffer: vec![],
            anomalies: String::with_capacity(100),
            anomaly_count: 0,
            compression,
        }
    }

    pub fn add_sequence_anomaly(&mut self, last: u32, current: u32) {
        self.anomaly_count += 1;
        if self.anomalies.len() >= 100 {
            return;
        }

        let error = String::from(self.stream.as_ref())
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
    T: Debug + Send + Point,
    Vec<T>: Serialize,
{
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn stream(&self) -> Arc<String> {
        self.stream.clone()
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

    fn compression(&self) -> Compression {
        self.compression
    }
}

impl<T> Clone for Stream<T> {
    fn clone(&self) -> Self {
        Stream {
            name: self.name.clone(),
            flush_period: self.flush_period,
            max_buffer_size: self.max_buffer_size,
            topic: self.topic.clone(),
            last_sequence: 0,
            last_timestamp: 0,
            buffer: Buffer::new(
                self.buffer.stream.clone(),
                self.buffer.topic.clone(),
                self.compression,
            ),
            metrics: StreamMetrics::new(&self.name, self.max_buffer_size),
            tx: self.tx.clone(),
            compression: self.compression,
        }
    }
}
