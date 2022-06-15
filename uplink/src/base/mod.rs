use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::sync::Arc;

use flume::{SendError, Sender};
use log::{info, warn};
use serde::Deserialize;

pub mod actions;
pub mod mqtt;
pub mod serializer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub topic: String,
    pub buf_size: usize,
    pub flush_period: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Persistence {
    pub path: String,
    pub max_file_size: usize,
    pub max_file_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Authentication {
    ca_certificate: String,
    device_certificate: String,
    device_private_key: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Ota {
    pub enabled: bool,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Stats {
    pub enabled: bool,
    pub process_names: Vec<String>,
    pub update_period: u64,
    pub stream_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    pub project_id: String,
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub authentication: Option<Authentication>,
    pub bridge_port: u16,
    pub max_packet_size: usize,
    pub max_inflight: u16,
    pub actions: Vec<String>,
    pub persistence: Option<Persistence>,
    pub streams: HashMap<String, StreamConfig>,
    pub flush_period: Option<u64>,
    pub ota: Ota,
    pub stats: Stats,
}

pub trait Point: Send + Debug {
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

pub trait Package: Send + Debug {
    fn topic(&self) -> Arc<String>;
    // TODO: Implement a generic Return type that can wrap
    // around custom serialization error types.
    fn serialize(&self) -> serde_json::Result<Vec<u8>>;
    fn anomalies(&self) -> Option<(String, usize)>;
}

/// Signal to modify the behaviour of collector
#[derive(Debug)]
pub enum Control {
    Shutdown,
    StopStream(String),
    StartStream(String),
}

/// Signals status of stream buffer
#[derive(Debug)]
pub enum StreamStatus<'a> {
    Partial,
    Flushed(&'a String),
    Init(&'a String),
}

#[derive(Debug)]
pub struct Stream<T> {
    pub name: Arc<String>,
    topic: Arc<String>,
    last_sequence: u32,
    last_timestamp: u64,
    max_buffer_size: usize,
    buffer: Buffer<T>,
    tx: Sender<Box<dyn Package>>,
}

impl<T> Stream<T>
where
    T: Point + Debug + Send + 'static,
    Buffer<T>: Package,
{
    pub fn new<S: Into<String>>(
        stream: S,
        topic: S,
        max_buffer_size: usize,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let name = Arc::new(stream.into());
        let topic = Arc::new(topic.into());
        let buffer = Buffer::new(name.clone(), topic.clone());

        Stream { name, topic, last_sequence: 0, last_timestamp: 0, max_buffer_size, buffer, tx }
    }

    pub fn dynamic_with_size<S: Into<String>>(
        stream: S,
        project_id: S,
        device_id: S,
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

        Stream::new(stream, topic, max_buffer_size, tx)
    }

    pub fn dynamic<S: Into<String>>(
        stream: S,
        project_id: S,
        device_id: S,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        Stream::dynamic_with_size(stream, project_id, device_id, 100, tx)
    }

    fn add(&mut self, data: T) -> Result<Option<Buffer<T>>, Error> {
        let current_sequence = data.sequence();
        let current_timestamp = data.timestamp();

        // Fill buffer with data
        self.buffer.buffer.push(data);

        // Anomaly detection
        if current_sequence <= self.last_sequence {
            warn!("Sequence number anomaly! [{}, {}]", current_sequence, self.last_sequence);
            self.buffer.add_sequence_anomaly(self.last_sequence, current_sequence);
        }

        if current_timestamp < self.last_timestamp {
            warn!("Timestamp anomaly!! [{}, {}]", current_timestamp, self.last_timestamp);
            self.buffer.add_timestamp_anomaly(self.last_timestamp, current_timestamp);
        }

        self.last_sequence = current_sequence;
        self.last_timestamp = current_timestamp;

        // if max_buffer_size is reached or timedout and not empty
        let buf = if self.buffer.buffer.len() >= self.max_buffer_size {
            Some(self.flush_buffer())
        } else {
            None
        };

        Ok(buf)
    }

    // Returns buffer content, replacing with empty buffer in-place
    fn flush_buffer(&mut self) -> Buffer<T> {
        let name = self.name.clone();
        let topic = self.topic.clone();
        info!("Flushing stream name: {}, topic: {}", name, topic);

        mem::replace(&mut self.buffer, Buffer::new(name, topic))
    }

    /// Triggers flush and async channel send if not empty
    pub async fn flush(&mut self) -> Result<(), Error> {
        if !self.is_empty() {
            let buf = self.flush_buffer();
            self.tx.send_async(Box::new(buf)).await?;
        }

        Ok(())
    }

    /// Method to check if a Stream buffer is empty
    pub fn is_empty(&mut self) -> bool {
        self.buffer.buffer.is_empty()
    }

    /// Fill buffer with data and trigger async channel send on max_buf_size, returning true if flushed.
    pub async fn fill<'a>(&'a mut self, data: T) -> Result<StreamStatus<'a>, Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send_async(Box::new(buf)).await?;
            return Ok(StreamStatus::Flushed(&self.name));
        }

        if self.buffer.buffer.len() == 1 {
            return Ok(StreamStatus::Init(&self.name));
        }

        Ok(StreamStatus::Partial)
    }

    /// Push data into buffer and trigger sync channel send on max_buf_size
    pub fn push(&mut self, data: T) -> Result<(), Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send(Box::new(buf))?;
        }

        Ok(())
    }
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
}

impl<T> Buffer<T> {
    pub fn new(stream: Arc<String>, topic: Arc<String>) -> Buffer<T> {
        Buffer {
            stream,
            topic,
            buffer: vec![],
            anomalies: String::with_capacity(100),
            anomaly_count: 0,
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

impl<T> Clone for Stream<T> {
    fn clone(&self) -> Self {
        Stream {
            name: self.name.clone(),
            topic: self.topic.clone(),
            last_sequence: 0,
            last_timestamp: 0,
            max_buffer_size: self.max_buffer_size,
            buffer: Buffer::new(self.buffer.stream.clone(), self.buffer.topic.clone()),
            tx: self.tx.clone(),
        }
    }
}
