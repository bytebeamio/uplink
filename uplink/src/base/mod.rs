use async_channel::{SendError, Sender};
use log::{error, warn};
use serde::Deserialize;

use std::{collections::HashMap, fmt::Debug, mem, sync::Arc};

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

#[derive(Debug, Clone, Deserialize)]
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
    pub persistence: Persistence,
    pub streams: HashMap<String, StreamConfig>,
}

pub trait Point: Send + Debug {
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

pub trait Package: Send + Debug {
    fn topic(&self) -> Arc<String>;
    fn serialize(&self) -> Vec<u8>;
    fn anomalies(&self) -> Option<(String, usize)>;
}

/// Signal to modify the behaviour of collector
#[derive(Debug)]
pub enum Control {
    Shutdown,
    StopStream(String),
    StartStream(String),
}

#[derive(Debug)]
pub struct Stream<T> {
    name: Arc<String>,
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

    pub fn dynamic<S: Into<String>>(
        stream: S,
        project_id: S,
        device_id: S,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let stream = stream.into();
        let project_id = project_id.into();
        let device_id = device_id.into();

        let topic = String::from("/tenants/")
            + &project_id
            + "/devices/"
            + &device_id
            + "/"
            + &stream
            + "/jsonarray";

        let name = Arc::new(stream);
        let topic = Arc::new(topic);
        let buffer = Buffer::new(name.clone(), topic.clone());

        Stream {
            name,
            topic,
            last_sequence: 0,
            last_timestamp: 0,
            max_buffer_size: 100,
            buffer,
            tx,
        }
    }

    pub async fn fill(&mut self, data: T) -> Result<(), Error> {
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

        // Send buffer to serializer
        if self.buffer.buffer.len() >= self.max_buffer_size {
            let name = self.name.clone();
            let topic = self.topic.clone();
            let buffer = mem::replace(&mut self.buffer, Buffer::new(name, topic));
            self.tx.send(Box::new(buffer)).await?;
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
        if self.anomalies.len() == 0 {
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
