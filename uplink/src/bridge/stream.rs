use std::{collections::HashMap, fmt::Debug, mem, sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{debug, error, info, trace};
use serde::Serialize;
use tokio::time::{interval, Interval};

use super::{utils::DelayMap, Config, StreamConfig, StreamMetrics};
use crate::base::{Package, Payload, Point, DEFAULT_TIMEOUT};

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
        }
    }

    pub fn with_config(
        name: &String,
        config: &StreamConfig,
        tx: Sender<Box<dyn Package>>,
    ) -> Stream<T> {
        let mut stream = Stream::new(name, &config.topic, config.buf_size, tx);
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

    pub fn metrics(&self) -> StreamMetrics {
        self.metrics.clone()
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
            buffer: Buffer::new(self.buffer.stream.clone(), self.buffer.topic.clone()),
            metrics: StreamMetrics::new(&self.name, self.max_buffer_size),
            tx: self.tx.clone(),
        }
    }
}

pub struct Streams {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    metrics_tx: Sender<StreamMetrics>,
    map: HashMap<String, Stream<Payload>>,
    pub stream_timeouts: DelayMap<String>,
    pub metrics_timeouts: DelayMap<String>,
    pub metrics_timeout: Interval,
}

impl Streams {
    pub async fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        let mut map = HashMap::new();
        for (name, stream) in &config.streams {
            let stream = Stream::with_config(name, stream, data_tx.clone());
            map.insert(name.to_owned(), stream);
        }

        let metrics_timeout = interval(Duration::from_secs(config.stream_metrics.timeout));
        Self {
            config,
            data_tx,
            metrics_tx,
            map,
            stream_timeouts: DelayMap::new(),
            metrics_timeouts: DelayMap::new(),
            metrics_timeout,
        }
    }

    pub async fn forward(&mut self, data: Payload) {
        let stream_name = data.stream.to_owned();
        let stream = match self.map.get_mut(&stream_name) {
            Some(partition) => partition,
            None => {
                if self.map.keys().len() > 20 {
                    error!("Failed to create {:?} stream. More than max 20 streams", stream_name);
                    return;
                }

                let stream = Stream::dynamic(
                    &stream_name,
                    &self.config.project_id,
                    &self.config.device_id,
                    self.data_tx.clone(),
                );

                self.map.entry(stream_name.to_owned()).or_insert(stream)
            }
        };

        let max_stream_size = stream.max_buffer_size;
        let state = match stream.fill(data).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to send data. Error = {:?}", e.to_string());
                return;
            }
        };

        // Remove timeout from flush_handler for selected stream if stream state is flushed,
        // do nothing if stream state is partial. Insert a new timeout if initial fill.
        // Warn in case stream flushed stream was not in the queue.
        if max_stream_size > 1 {
            match state {
                StreamStatus::Flushed => self.stream_timeouts.remove(&stream_name),
                StreamStatus::Init(flush_period) => {
                    trace!("Initialized stream buffer for {stream_name}",);
                    self.stream_timeouts.insert(&stream_name, flush_period);
                }
                StreamStatus::Partial(_l) => {}
            }
        }
    }

    // Flush stream/partitions that timeout
    pub async fn flush_stream(&mut self, stream: &str) -> Result<(), Error> {
        let stream = self.map.get_mut(stream).unwrap();
        stream.flush().await?;
        Ok(())
    }

    // Enable actual metrics timers when there is data. This method is called every minute by the bridge
    pub fn check_and_flush_metrics(
        &mut self,
    ) -> Result<(), Box<flume::TrySendError<StreamMetrics>>> {
        for (buffer_name, data) in self.map.iter_mut() {
            let metrics = data.metrics.clone();

            // Initialize metrics timeouts when force flush sees data counts
            if metrics.points() > 0 {
                info!(
                    "{:>20}: points = {:<5} batches = {:<5} latency = {}",
                    buffer_name, metrics.points, metrics.batches, metrics.average_batch_latency
                );
                self.metrics_tx.try_send(metrics)?;
                data.metrics.prepare_next();
            }
        }

        Ok(())
    }
}
