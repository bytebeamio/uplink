use std::{fmt::Debug, mem, sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{debug, trace};

use super::{Payload, StreamMetrics};
use crate::uplink_config::StreamConfig;

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
    Send(#[from] SendError<Box<MessageBuffer>>),
}

pub const MAX_BATCH_SIZE: usize = 100;

#[derive(Debug)]
pub struct Stream {
    pub name: Arc<String>,
    pub config: Arc<StreamConfig>,
    last_sequence: u32,
    last_timestamp: u64,
    buffer: MessageBuffer,
    tx: Sender<Box<MessageBuffer>>,
    pub metrics: StreamMetrics,
}

impl Stream {
    pub fn new(
        stream_name: impl Into<String>,
        stream_config: StreamConfig,
        tx: Sender<Box<MessageBuffer>>,
    ) -> Stream {
        let name = Arc::new(stream_name.into());
        let config = Arc::new(stream_config);
        let buffer = MessageBuffer::new(name.clone(), config.clone());
        let metrics = StreamMetrics::new(&name, config.batch_size);

        Stream { name, config, last_sequence: 0, last_timestamp: 0, buffer, tx, metrics }
    }

    pub fn dynamic(
        stream_name: impl Into<String>,
        project_id: impl Into<String>,
        device_id: impl Into<String>,
        tx: Sender<Box<MessageBuffer>>,
    ) -> Stream {
        let stream_name = stream_name.into();
        let project_id = project_id.into();
        let device_id = device_id.into();

        let topic = format!("/tenants/{project_id}/devices/{device_id}/events/{stream_name}/jsonarray");
        let config = StreamConfig { topic, ..Default::default() };

        Stream::new(stream_name, config, tx)
    }

    fn add(&mut self, data: Payload) -> Result<Option<MessageBuffer>, Error> {
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

        // if max_bATCH_size is breached, flush
        let buf = if self.buffer.buffer.len() >= self.config.batch_size {
            self.metrics.add_batch();
            Some(self.take_buffer())
        } else {
            None
        };

        Ok(buf)
    }

    // Returns buffer content, replacing with empty buffer in-place
    fn take_buffer(&mut self) -> MessageBuffer {
        let name = self.name.clone();
        let config = self.config.clone();
        trace!("Flushing stream name: {name}, topic: {}", config.topic);

        mem::replace(&mut self.buffer, MessageBuffer::new(name, config))
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

    /// Fill buffer with data and trigger async channel send on breaching max_batch_size.
    /// Returns [`StreamStatus`].
    pub async fn fill(&mut self, data: Payload) -> Result<StreamStatus, Error> {
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
}

/// Streams module buffers messages into this type
/// and sends them over to Serializer
/// which does persistence, compression, etc
#[derive(Debug)]
pub struct MessageBuffer {
    pub stream_name: Arc<String>,
    pub stream_config: Arc<StreamConfig>,
    pub buffer: Vec<Payload>,
    pub anomalies: String,
    pub anomaly_count: usize,
}

impl MessageBuffer {
    pub fn new(stream_name: Arc<String>, stream_config: Arc<StreamConfig>) -> MessageBuffer {
        MessageBuffer {
            buffer: Vec::with_capacity(stream_config.batch_size),
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

        let error = format!("{}.sequence: {last}, {current}", self.stream_name);
        self.anomalies.push_str(&error)
    }

    pub fn add_timestamp_anomaly(&mut self, last: u64, current: u64) {
        self.anomaly_count += 1;
        if self.anomalies.len() >= 100 {
            return;
        }

        let error = format!("timestamp: {last}, {current}");
        self.anomalies.push_str(&error)
    }

    pub fn anomalies(&self) -> Option<(String, usize)> {
        if self.anomalies.is_empty() {
            return None;
        }

        Some((self.anomalies.clone(), self.anomaly_count))
    }

    pub fn serialize(&self) -> Vec<u8> {
        // This unwrap is safe because our data meets the requirements of `to_vec`
        serde_json::to_vec(&self.buffer).unwrap()
    }

    // TODO: remove this
    pub fn latency(&self) -> u64 {
        0
    }
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        Stream {
            name: self.name.clone(),
            config: self.config.clone(),
            last_sequence: 0,
            last_timestamp: 0,
            buffer: MessageBuffer::new(self.buffer.stream_name.clone(), self.buffer.stream_config.clone()),
            metrics: StreamMetrics::new(&self.name, self.config.batch_size),
            tx: self.tx.clone(),
        }
    }
}
