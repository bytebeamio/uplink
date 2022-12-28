use std::{collections::HashMap, fmt::Debug, mem, sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod actions;
pub mod middleware;
pub mod mqtt;
pub mod serializer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
}

pub const DEFAULT_TIMEOUT: u64 = 60;

#[inline]
fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StreamConfig {
    pub topic: Option<String>,
    pub buf_size: usize,
    #[serde(default = "default_timeout")]
    /// Duration(in seconds) that bridge collector waits from
    /// receiving first element, before the stream gets flushed.
    pub flush_period: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Persistence {
    pub path: String,
    pub max_file_size: usize,
    pub max_file_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Authentication {
    pub ca_certificate: String,
    pub device_certificate: String,
    pub device_private_key: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Stats {
    pub enabled: bool,
    pub process_names: Vec<String>,
    pub update_period: u64,
    pub stream_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SimulatorConfig {
    /// number of devices to be simulated
    pub num_devices: u32,
    /// path to directory containing files with gps paths to be used in simulation
    pub gps_paths: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct JournalctlConfig {
    pub tags: Vec<String>,
    pub priority: u8,
    pub stream_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Downloader {
    pub actions: Vec<String>,
    pub path: String,
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
    pub action_status: StreamConfig,
    pub serializer_metrics: Option<StreamConfig>,
    pub bridge_stats: Option<StreamConfig>,
    pub app_stats: Option<StreamConfig>,
    pub downloader: Option<Downloader>,
    pub stats: Stats,
    pub simulator: Option<SimulatorConfig>,
    #[cfg(target_os = "linux")]
    pub journalctl: Option<JournalctlConfig>,
    #[cfg(target_os = "android")]
    pub run_logcat: bool,
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

/// Signals status of stream buffer
#[derive(Debug)]
pub enum StreamStatus<'a> {
    Partial(usize),
    Flushed(&'a String),
    Init(&'a String, Duration),
}

#[derive(Debug)]
pub struct Stream<T> {
    name: Arc<String>,
    topic: Arc<String>,
    last_sequence: u32,
    last_timestamp: u64,
    pub max_buffer_size: usize,
    buffer: Buffer<T>,
    tx: Sender<Box<dyn Package>>,
    pub flush_period: Duration,
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
        let flush_period = Duration::from_secs(DEFAULT_TIMEOUT);

        Stream {
            name,
            topic,
            last_sequence: 0,
            last_timestamp: 0,
            max_buffer_size,
            buffer,
            tx,
            flush_period,
        }
    }

    pub fn with_config(
        name: &String,
        project_id: &String,
        device_id: &String,
        config: &StreamConfig,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let mut stream = if let Some(topic) = &config.topic {
            Stream::new(name, topic, config.buf_size, tx)
        } else {
            Stream::dynamic_with_size(name, project_id, device_id, config.buf_size, tx)
        };
        stream.flush_period = Duration::from_secs(config.flush_period);

        stream
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
            debug!("Sequence number anomaly! [{}, {}]", current_sequence, self.last_sequence);
            self.buffer.add_sequence_anomaly(self.last_sequence, current_sequence);
        }

        if current_timestamp < self.last_timestamp {
            debug!("Timestamp anomaly!! [{}, {}]", current_timestamp, self.last_timestamp);
            self.buffer.add_timestamp_anomaly(self.last_timestamp, current_timestamp);
        }

        self.last_sequence = current_sequence;
        self.last_timestamp = current_timestamp;

        // if max_buffer_size is breached, flush
        let buf = if self.buffer.buffer.len() >= self.max_buffer_size {
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

        mem::replace(&mut self.buffer, Buffer::new(name, topic))
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
    pub async fn fill(&mut self, data: T) -> Result<StreamStatus<'_>, Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send_async(Box::new(buf)).await?;
            return Ok(StreamStatus::Flushed(&self.name));
        }

        let status = match self.len() {
            1 => StreamStatus::Init(&self.name, self.flush_period),
            len => StreamStatus::Partial(len),
        };

        Ok(status)
    }

    /// Push data into buffer and trigger sync channel send on max_buf_size.
    /// Returns [`StreamStatus`].
    pub fn push(&mut self, data: T) -> Result<StreamStatus<'_>, Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send(Box::new(buf))?;
            return Ok(StreamStatus::Flushed(&self.name));
        }

        let status = match self.len() {
            1 => StreamStatus::Init(&self.name, self.flush_period),
            len => StreamStatus::Partial(len),
        };

        Ok(status)
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

impl<T> Package for Buffer<T>
where
    T: Debug + Send,
    Vec<T>: Serialize,
{
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.buffer)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
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
            flush_period: self.flush_period,
        }
    }
}

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is in turn a json
// TODO which cloud will double deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    #[serde(skip_serializing)]
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

impl Point for Payload {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
