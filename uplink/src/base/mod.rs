use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::path::PathBuf;

use async_channel::{SendError, Sender};
use serde::{Deserialize, Serialize};

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
pub struct Config {
    pub device_id: String,
    pub broker: String,
    pub port: u16,
    pub bridge_port: u16,
    pub max_packet_size: usize,
    pub max_inflight: u16,
    pub actions: Vec<String>,
    pub persistence: Persistence,
    pub streams: HashMap<String, StreamConfig>,
    pub key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
}

pub trait Package: Send + Debug {
    fn stream(&self) -> String;
    fn serialize(&self) -> Vec<u8>;
}

/// Signal to modify the behaviour of collector
#[derive(Debug)]
pub enum Control {
    Shutdown,
    StopStream(String),
    StartStream(String),
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Metrics {
    sequence: u64,
    timestamp: u64,
    total_sent_size: usize,
    total_disk_size: usize,
}

pub struct Stream<T> {
    name: String,
    last_sequence: u64,
    last_timestamp: u64,
    max_buffer_size: usize,
    buffer: Buffer<T>,
    tx: Sender<Box<dyn Package>>,
}

impl<T> Stream<T>
where
    T: Debug + Send + 'static,
    Buffer<T>: Package,
{
    pub fn new<S: Into<String>>(stream: S, max_buffer_size: usize, tx: Sender<Box<dyn Package>>) -> Stream<T> {
        let buffer = Buffer::new(stream.into());
        Stream { name: "".to_string(), last_sequence: 0, last_timestamp: 0, max_buffer_size, buffer, tx }
    }

    pub async fn fill(&mut self, data: T) -> Result<(), Error> {
        self.buffer.buffer.push(data);

        if self.buffer.buffer.len() >= self.max_buffer_size {
            let buffer = mem::replace(&mut self.buffer, Buffer::new(self.name.clone()));
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
    pub stream: String,
    pub buffer: Vec<T>,
    pub anomalies: Vec<String>,
}

impl<T> Buffer<T> {
    pub fn new<S: Into<String>>(stream: S) -> Buffer<T> {
        Buffer { stream: stream.into(), buffer: vec![], anomalies: vec![] }
    }
}

impl<T> Clone for Stream<T> {
    fn clone(&self) -> Self {
        Stream {
            name: self.name.clone(),
            last_sequence: 0,
            last_timestamp: 0,
            max_buffer_size: self.max_buffer_size,
            buffer: Buffer::new(self.buffer.stream.clone()),
            tx: self.tx.clone(),
        }
    }
}
