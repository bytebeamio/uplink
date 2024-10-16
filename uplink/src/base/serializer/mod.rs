mod metrics;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;
use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use flume::{bounded, Receiver, RecvError, Sender, TrySendError};
use log::{debug, error, info, trace, warn};
use lz4_flex::frame::FrameEncoder;
use rumqttc::*;
use thiserror::Error;
use tokio::{select, time::interval};

use crate::config::{Compression, StreamConfig};
use crate::{Config, Package};
pub use metrics::{Metrics, SerializerMetrics, StreamMetrics};

const METRICS_INTERVAL: Duration = Duration::from_secs(10);

#[derive(thiserror::Error, Debug)]
pub enum MqttError {
    #[error("SendError(..)")]
    Send(Request),
    #[error("TrySendError(..)")]
    TrySend(Request),
}

impl From<ClientError> for MqttError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Request(r) => MqttError::Send(r),
            ClientError::TryRequest(r) => MqttError::TrySend(r),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Collector recv error {0}")]
    Collector(#[from] RecvError),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Storage error {0}")]
    Storage(#[from] storage::Error),
    #[error("Mqtt client error {0}")]
    Client(#[from] MqttError),
    #[error("Storage is disabled/missing")]
    MissingPersistence,
    #[error("LZ4 compression error: {0}")]
    Lz4(#[from] lz4_flex::frame::Error),
    #[error("Empty storage")]
    EmptyStorage,
    #[error("Permission denied while accessing persistence directory {0:?}")]
    Persistence(String),
    #[error("Serializer has shutdown after handling crash")]
    Shutdown,
}

#[derive(Debug, PartialEq)]
enum Status {
    Normal,
    SlowEventloop(Publish, Arc<StreamConfig>),
    EventLoopReady,
    EventLoopCrash(Publish, Arc<StreamConfig>),
    Shutdown,
}

/// Description of an interface that the [`Serializer`] expects to be provided by the MQTT client to publish the serialized data with.
#[async_trait::async_trait]
pub trait MqttClient: Clone {
    /// Accept payload and resolve as an error only when the client has died(thread kill). Useful in Slow/Catchup mode.
    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), MqttError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send;

    /// Accept payload and resolve as an error if data can't be sent over network, immediately. Useful in Normal mode.
    fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), MqttError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send;
}

#[async_trait::async_trait]
impl MqttClient for AsyncClient {
    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), MqttError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send,
    {
        self.publish(topic, qos, retain, payload).await?;
        Ok(())
    }

    fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), MqttError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        self.try_publish(topic, qos, retain, payload)?;
        Ok(())
    }
}

pub struct Storage {
    inner: storage::Storage,
    live_data_first: bool,
    latest_data: Option<Publish>,
}

impl Storage {
    pub fn new(name: impl Into<String>, max_file_size: usize, live_data_first: bool) -> Self {
        Self {
            inner: storage::Storage::new(name, max_file_size),
            live_data_first,
            latest_data: None,
        }
    }

    pub fn set_persistence(
        &mut self,
        backup_path: impl Into<PathBuf>,
        max_file_count: usize,
    ) -> Result<(), storage::Error> {
        self.inner.set_persistence(backup_path.into(), max_file_count)
    }

    // Stores the provided publish packet by serializing it into storage, after setting its pkid to 1.
    // If the write buffer is full, it is flushed/written onto disk based on config.
    pub fn write(&mut self, mut publish: Publish) -> Result<Option<u64>, storage::Error> {
        if self.live_data_first {
            let Some(previous) = self.latest_data.replace(publish) else { return Ok(None) };
            publish = previous;
        } else if self.latest_data.is_some() {
            warn!("Latest data should be unoccupied if not using the live data first scheme");
        }

        publish.pkid = 1;
        if let Err(e) = publish.write(self.inner.writer()) {
            error!("Failed to fill disk buffer. Error = {e}");
            return Ok(None);
        }

        self.inner.flush_on_overflow()
    }

    // Ensures read-buffer is ready to be read from, exchanges buffers if required, returns true if empty.
    pub fn reload_on_eof(&mut self) -> Result<(), storage::Error> {
        self.inner.reload_on_eof()
    }

    // Deserializes publish packets from storage, returns None when it fails, i.e read buffer is empty.
    //
    // ## Panic
    // When any packet other than a publish is deserialized.
    pub fn read(&mut self, max_packet_size: usize) -> Option<Publish> {
        if let Some(publish) = self.latest_data.take() {
            return Some(publish);
        }

        // TODO(RT): This can fail when packet sizes > max_payload_size in config are written to disk.
        // This leads to force switching to normal mode. Increasing max_payload_size to bypass this
        match Packet::read(self.inner.reader(), max_packet_size) {
            Ok(Packet::Publish(publish)) => Some(publish),
            Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
            Err(e) => {
                error!("Failed to read from storage. Forcing into Normal mode. Error = {e}");
                None
            }
        }
    }

    // Ensures all data is written into persistence, when configured.
    pub fn flush(&mut self) -> Result<Option<u64>, storage::Error> {
        // Write live cache to disk when flushing
        if let Some(mut publish) = self.latest_data.take() {
            publish.pkid = 1;
            if let Err(e) = publish.write(self.inner.writer()) {
                error!("Failed to fill disk buffer. Error = {e}");
                return Ok(None);
            }
        }

        self.inner.flush()
    }
}

struct StorageHandler {
    config: Arc<Config>,
    map: BTreeMap<Arc<StreamConfig>, Storage>,
    // Stream being read from
    read_stream: Option<Arc<StreamConfig>>,
}

impl StorageHandler {
    fn new(config: Arc<Config>) -> Result<Self, Error> {
        let mut map = BTreeMap::new();
        let mut streams = config.streams.clone();
        // NOTE: persist action_status if not configured otherwise
        streams.insert("action_status".into(), config.action_status.clone());
        for (stream_name, stream_config) in streams {
            let mut storage = Storage::new(
                &stream_config.topic,
                stream_config.persistence.max_file_size,
                stream_config.live_data_first,
            );
            if stream_config.persistence.max_file_count > 0 {
                let mut path = config.persistence_path.clone();
                path.push(&stream_name);

                std::fs::create_dir_all(&path).map_err(|_| {
                    Error::Persistence(config.persistence_path.to_string_lossy().to_string())
                })?;
                storage.set_persistence(&path, stream_config.persistence.max_file_count)?;

                debug!(
                    "Disk persistance is enabled for stream: {stream_name:?}; path: {}",
                    path.display()
                );
            }
            map.insert(Arc::new(stream_config), storage);
        }

        Ok(Self { config, map, read_stream: None })
    }

    // Selects the right storage for to write into and serializes received data as a Publish packet into it.
    fn store(&mut self, stream: Arc<StreamConfig>, publish: Publish, metrics: &mut Metrics) {
        match self
            .map
            .entry(stream.to_owned())
            .or_insert_with(|| {
                Storage::new(
                    &stream.topic,
                    self.config.default_buf_size,
                    self.config.default_live_data_first,
                )
            })
            .write(publish)
        {
            Ok(Some(deleted)) => {
                debug!("Lost segment = {deleted}");
                metrics.increment_lost_segments();
            }
            Err(e) => {
                error!("Crash loop: write error = {e}");
                metrics.increment_errors();
            }
            _ => {}
        }
    }

    // Extracts a publish packet from storage if any exist, else returns None
    fn next(&mut self, metrics: &mut Metrics) -> Option<(Arc<StreamConfig>, Publish)> {
        let storages = self.map.iter_mut();

        for (stream, storage) in storages {
            match storage.reload_on_eof() {
                // Reading from a non-empty persisted stream
                Ok(_) => {
                    if self.read_stream.is_none() {
                        self.read_stream.replace(stream.to_owned());
                        debug!("Started reading from: {}", stream.topic);
                    } else {
                        trace!("Reading from: {}", stream.topic);
                    }

                    let Some(publish) = storage.read(self.config.mqtt.max_packet_size) else {
                        continue;
                    };
                    metrics.add_batch();

                    return Some((stream.to_owned(), publish));
                }
                // All packets read from storage
                Err(storage::Error::Done) => {
                    if self.read_stream.take_if(|s| s == stream).is_some() {
                        debug!("Done reading from: {}", stream.topic);
                    }
                }
                // Reload again on encountering a corrupted file
                Err(e) => {
                    metrics.increment_errors();
                    metrics.increment_lost_segments();
                    error!("Failed to reload from storage. Error = {e}");
                }
            }
        }

        None
    }

    // Force flushes all in-memory buffers to ensure zero packet loss during uplink restart.
    // TODO: Ensure packets in read-buffer but not on disk are not lost.
    fn flush_all(&mut self) {
        for (stream_config, storage) in self.map.iter_mut() {
            match storage.flush() {
                Ok(_) => trace!("Force flushed stream = {} onto disk", stream_config.topic),
                Err(storage::Error::NoWrites) => {}
                Err(e) => error!(
                    "Error when force flushing storage = {}; error = {e}",
                    stream_config.topic
                ),
            }
        }
    }

    // Update Metrics about all storages being handled
    fn update_metrics(&self, metrics: &mut Metrics) {
        let mut inmemory_write_size = 0;
        let mut inmemory_read_size = 0;
        let mut file_count = 0;
        let mut disk_utilized = 0;

        for storage in self.map.values() {
            inmemory_write_size += storage.inner.inmemory_write_size();
            inmemory_read_size += storage.inner.inmemory_read_size();
            file_count += storage.inner.file_count();
            disk_utilized += storage.inner.disk_utilized();
        }

        metrics.set_write_memory(inmemory_write_size);
        metrics.set_read_memory(inmemory_read_size);
        metrics.set_disk_files(file_count);
        metrics.set_disk_utilized(disk_utilized);
    }
}

/// The uplink Serializer is the component that deals with serializing, compressing and writing data onto disk or Network.
/// In case of network issues, the Serializer enters various states depending on the severeness, managed by [`start()`].
///
/// The Serializer writes data directly to network in **normal mode** with the [`try_publish()`] method on the MQTT client.
/// In case of the network being slow, this fails and we are forced into **slow mode**, where-in new data gets written into
/// [`Storage`] while consequently we await on a [`publish()`]. If the [`publish()`] succeeds, we move into **catchup mode**
/// or if it fails we move to the **crash mode**. In **catchup mode**, we continuously write to [`Storage`] while also
/// pushing data onto the network with a [`publish()`]. If a [`publish()`] succeds, we load the next [`Publish`] packet from
/// [`Storage`], until it is empty. We can transition back into normal mode when [`Storage`] is empty during operation in
/// the catchup mode.
///
/// P.S: We have a transition into **crash mode** when we are in catchup or slow mode and the thread running the MQTT client
/// stalls and dies out. Here we merely write all data received, directly into disk. This is a failure mode that ideally the
/// serializer should never be operated in.
///
/// ```text
///
///                         Send publishes in Storage to                                  No more publishes
///                         Network, write new data to disk                               left in Storage
///                        ┌---------------------┐                                       ┌──────┐
///                        │Serializer::catchup()├───────────────────────────────────────►Normal│
///                        └---------▲---------┬-┘                                       └───┬──┘
///                                  │         │     Network has crashed                     │
///                       Network is │         │    ┌───────────────────────┐                │
///                        available │         ├────►EventloopCrash(publish)│                │
/// ┌-------------------┐    ┌───────┴──────┐  │    └───────────┬───────────┘     ┌----------▼---------┐
/// │Serializer::start()├────►EventloopReady│  │   ┌------------▼-------------┐   │Serializer::normal()│ Forward all data to Network
/// └-------------------┘    └───────▲──────┘  │   │Serializer::crash(publish)├─┐ └----------┬---------┘
///                                  │         │   └-------------------------▲┘ │            │
///                                  │         │    Write all data to Storage└──┘            │
///                                  │         │                                             │
///                        ┌---------┴---------┴-----┐                           ┌───────────▼──────────┐
///                        │Serializer::slow(publish)◄───────────────────────────┤SlowEventloop(publish)│
///                        └-------------------------┘                           └──────────────────────┘
///                         Write to storage,                                     Slow network encountered
///                         but continue trying to publish
///
///```
///
/// NOTE: Shutdown mode and crash mode are only different in how they get triggered,
/// but should be considered as interchangeable in the above diagram.
/// [`start()`]: Serializer::start
/// [`try_publish()`]: AsyncClient::try_publish
/// [`publish()`]: AsyncClient::publish
pub struct Serializer<C: MqttClient> {
    collector_rx: Receiver<Box<dyn Package>>,
    client: C,
    storage_handler: StorageHandler,
    metrics: Metrics,
    metrics_tx: Sender<SerializerMetrics>,
    pending_metrics: VecDeque<SerializerMetrics>,
    stream_metrics: HashMap<String, StreamMetrics>,
    /// Control handles
    ctrl_rx: Receiver<SerializerShutdown>,
    ctrl_tx: Sender<SerializerShutdown>,
}

impl<C: MqttClient> Serializer<C> {
    /// Construct the uplink Serializer with the necessary configuration details, a receiver handle to accept data payloads from,
    /// the handle to an MQTT client(This is constructed as such for testing purposes) and a handle to update serailizer metrics.
    pub fn new(
        config: Arc<Config>,
        collector_rx: Receiver<Box<dyn Package>>,
        client: C,
        metrics_tx: Sender<SerializerMetrics>,
    ) -> Result<Serializer<C>, Error> {
        let storage_handler = StorageHandler::new(config)?;
        let (ctrl_tx, ctrl_rx) = bounded(1);

        Ok(Serializer {
            collector_rx,
            client,
            storage_handler,
            metrics: Metrics::new("catchup"),
            stream_metrics: HashMap::new(),
            metrics_tx,
            pending_metrics: VecDeque::with_capacity(3),
            ctrl_tx,
            ctrl_rx,
        })
    }

    pub fn ctrl_tx(&self) -> CtrlTx {
        CtrlTx { inner: self.ctrl_tx.clone() }
    }

    /// Write all data received, from here-on, to disk only, shutdown serializer
    /// after handling all data payloads.
    fn shutdown(&mut self) -> Result<(), Error> {
        debug!("Forced into shutdown mode, writing all incoming data to persistence.");

        loop {
            // Collect remaining data packets and write to disk
            // NOTE: wait 2s to allow bridge to shutdown and flush leftover data.
            let deadline = Instant::now() + Duration::from_secs(2);
            let Ok(data) = self.collector_rx.recv_deadline(deadline) else {
                self.storage_handler.flush_all();
                return Ok(());
            };

            let stream = data.stream_config();
            let publish = construct_publish(data, &mut self.stream_metrics)?;
            self.storage_handler.store(stream, publish, &mut self.metrics);
        }
    }

    /// Write all data received, from here-on, to disk only.
    async fn crash(
        &mut self,
        publish: Publish,
        stream: Arc<StreamConfig>,
    ) -> Result<Status, Error> {
        // Write failed publish to disk first, metrics don't matter
        self.storage_handler.store(stream, publish, &mut self.metrics);

        loop {
            // Collect next data packet and write to disk
            let data = self.collector_rx.recv_async().await?;
            let stream = data.stream_config();
            let publish = construct_publish(data, &mut self.stream_metrics)?;
            self.storage_handler.store(stream, publish, &mut self.metrics);
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    // TODO: Handle errors. Don't return errors
    async fn slow(&mut self, publish: Publish, stream: Arc<StreamConfig>) -> Result<Status, Error> {
        let mut interval = interval(METRICS_INTERVAL);
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to slow eventloop mode!!");
        self.metrics.set_mode("slow");

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let publish = send_publish(self.client.clone(), publish);
        tokio::pin!(publish);

        let v: Result<Status, Error> = loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let stream = data.stream_config();
                    let publish = construct_publish(data, &mut self.stream_metrics)?;
                    self.storage_handler.store(stream, publish, &mut self.metrics);
                }
                o = &mut publish => match o {
                    Ok(_) => break Ok(Status::EventLoopReady),
                    Err(MqttError::Send(Request::Publish(publish))) => {
                        break Ok(Status::EventLoopCrash(publish, stream));
                    }
                    Err(e) => unreachable!("Unexpected error: {e}"),
                },
                _ = interval.tick() => {
                    check_metrics(&mut self.metrics, &mut self.stream_metrics, &self.storage_handler);
                }
                // Transition into crash mode when uplink is shutting down
                Ok(SerializerShutdown) = self.ctrl_rx.recv_async() => {
                    break Ok(Status::Shutdown)
                }
            }
        };

        save_and_prepare_next_metrics(
            &mut self.pending_metrics,
            &mut self.metrics,
            &mut self.stream_metrics,
            &self.storage_handler,
        );
        let v = v?;
        Ok(v)
    }

    /// Write new collector data to disk while sending existing data on
    /// disk to mqtt eventloop. Collector rx is selected with blocking
    /// `publish` instead of `try_publish` to ensure that transient back
    /// pressure due to a lot of data on disk doesn't switch state to
    /// `Status::SlowEventLoop`
    async fn catchup(&mut self) -> Result<Status, Error> {
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to catchup mode!!");
        let mut interval = interval(METRICS_INTERVAL);
        self.metrics.set_mode("catchup");

        let Some((mut last_publish_stream, publish)) = self.storage_handler.next(&mut self.metrics)
        else {
            return Ok(Status::Normal);
        };
        let mut last_publish_payload_size = publish.payload.len();
        let send = send_publish(self.client.clone(), publish);
        tokio::pin!(send);

        let v: Result<Status, Error> = loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let stream = data.stream_config();
                    let publish = construct_publish(data, &mut self.stream_metrics)?;
                    self.storage_handler.store(stream, publish, &mut self.metrics);
                }
                o = &mut send => {
                    self.metrics.add_sent_size(last_publish_payload_size);
                    // Send failure implies eventloop crash. Switch state to
                    // indefinitely write to disk to not loose data
                    let client = match o {
                        Ok(c) => c,
                        Err(MqttError::Send(Request::Publish(publish))) => {
                            break Ok(Status::EventLoopCrash(publish, last_publish_stream))
                        }
                        Err(e) => unreachable!("Unexpected error: {e}"),
                    };

                    let Some((stream, publish)) = self.storage_handler.next(&mut self.metrics)
                        else {
                            break Ok(Status::Normal);
                        };

                    last_publish_payload_size = publish.payload.len();
                    last_publish_stream = stream;
                    send.set(send_publish(client, publish));
                }
                // On a regular interval, forwards metrics information to network
                _ = interval.tick() => {
                    let _ = check_and_flush_metrics(&mut self.pending_metrics, &mut self.metrics, &self.metrics_tx, &self.storage_handler);
                }
                // Transition into shutdown mode when uplink is shutting down
                Ok(SerializerShutdown) = self.ctrl_rx.recv_async() => {
                    break Ok(Status::Shutdown)
                }
            }
        };

        save_and_prepare_next_metrics(
            &mut self.pending_metrics,
            &mut self.metrics,
            &mut self.stream_metrics,
            &self.storage_handler,
        );

        v
    }

    /// Tries to serialize and directly write all incoming data packets
    /// to network, will transition to 'slow' mode if `try_publish` fails.
    /// Every few seconds pushes serializer metrics. Transitions to
    /// `shutdown` mode if commanded to do so by control signals.
    async fn normal(&mut self) -> Result<Status, Error> {
        let mut interval = interval(METRICS_INTERVAL);
        self.metrics.set_mode("normal");
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to normal mode!!");

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let stream = data.stream_config();
                    let publish = construct_publish(data, &mut self.stream_metrics)?;
                    let payload_size = publish.payload.len();
                    debug!("publishing on {} with size = {payload_size}", publish.topic);
                    match self.client.try_publish(&stream.topic, QoS::AtLeastOnce, false, publish.payload) {
                        Ok(_) => {
                            self.metrics.add_batch();
                            self.metrics.add_sent_size(payload_size);
                            continue;
                        }
                        Err(MqttError::TrySend(Request::Publish(publish))) => return Ok(Status::SlowEventloop(publish, stream)),
                        Err(e) => unreachable!("Unexpected error: {e}"),
                    }
                }
                // On a regular interval, forwards metrics information to network
                _ = interval.tick() => {
                    // Check in storage stats every tick. TODO: Make storage object always
                    // available. It can be inmemory storage
                    if let Err(e) = check_and_flush_metrics(&mut self.pending_metrics, &mut self.metrics, &self.metrics_tx, &self.storage_handler) {
                        debug!("Failed to flush serializer metrics (normal). Error = {e}");
                    }
                }
                // Transition into shutdown mode when uplink is shutting down
                Ok(SerializerShutdown) = self.ctrl_rx.recv_async() => {
                    return Ok(Status::Shutdown)
                }
            }
        }
    }

    /// Starts operation of the uplink serializer, which can transition between the modes mentioned earlier.
    pub async fn start(mut self) -> Result<(), Error> {
        let mut status = Status::EventLoopReady;

        loop {
            let next_status = match status {
                Status::Normal => self.normal().await?,
                Status::SlowEventloop(publish, stream) => self.slow(publish, stream).await?,
                Status::EventLoopReady => self.catchup().await?,
                Status::EventLoopCrash(publish, stream) => self.crash(publish, stream).await?,
                Status::Shutdown => break,
            };

            status = next_status;
        }

        self.shutdown()?;

        info!("Serializer has handled all pending packets, shutting down");

        Ok(())
    }
}

// Used to construct a future that resolves if request is sent to the MQTT handler or errors
async fn send_publish<C: MqttClient>(client: C, publish: Publish) -> Result<C, MqttError> {
    let payload = Bytes::copy_from_slice(&publish.payload[..]);
    let topic = publish.topic;
    debug!("publishing on {topic} with size = {}", payload.len());
    client.publish(topic, QoS::AtLeastOnce, false, payload).await?;
    Ok(client)
}

// Updates payload with compressed content
fn lz4_compress(payload: &mut Vec<u8>) -> Result<(), Error> {
    let mut compressor = FrameEncoder::new(vec![]);
    compressor.write_all(payload)?;
    *payload = compressor.finish()?;

    Ok(())
}

// Constructs a [Publish] packet given a [Package] element. Updates stream metrics as necessary.
fn construct_publish(
    data: Box<dyn Package>,
    stream_metrics: &mut HashMap<String, StreamMetrics>,
) -> Result<Publish, Error> {
    let stream_name = data.stream_name().as_ref().to_owned();
    let stream_config = data.stream_config();
    let point_count = data.len();
    let batch_latency = data.latency();
    trace!("Data received on stream: {stream_name}; message count = {point_count}; batching latency = {batch_latency}");

    let metrics = stream_metrics
        .entry(stream_name.to_owned())
        .or_insert_with(|| StreamMetrics::new(&stream_name));

    let serialization_start = Instant::now();
    let mut payload = data.serialize()?;
    let serialization_time = serialization_start.elapsed();
    metrics.add_serialization_time(serialization_time);

    let data_size = payload.len();
    let mut compressed_data_size = None;

    if let Compression::Lz4 = stream_config.compression {
        let compression_start = Instant::now();
        lz4_compress(&mut payload)?;
        let compression_time = compression_start.elapsed();
        metrics.add_compression_time(compression_time);

        compressed_data_size = Some(payload.len());
    }

    metrics.add_serialized_sizes(data_size, compressed_data_size);

    Ok(Publish::new(&stream_config.topic, QoS::AtLeastOnce, payload))
}

// Updates serializer metrics and logs it, but doesn't push to network
fn check_metrics(
    metrics: &mut Metrics,
    stream_metrics: &mut HashMap<String, StreamMetrics>,
    storage_handler: &StorageHandler,
) {
    use pretty_bytes::converter::convert;

    storage_handler.update_metrics(metrics);
    info!(
        "{:>17}: batches = {:<3} errors = {} lost = {} disk_files = {:<3} disk_utilized = {} write_memory = {} read_memory = {}",
        metrics.mode,
        metrics.batches,
        metrics.errors,
        metrics.lost_segments,
        metrics.disk_files,
        convert(metrics.disk_utilized as f64),
        convert(metrics.write_memory as f64),
        convert(metrics.read_memory as f64),
    );

    for metrics in stream_metrics.values_mut() {
        metrics.prepare_snapshot();
        info!(
            "{:>17}: serialized_data_size = {} compressed_data_size = {} avg_serialization_time = {}us avg_compression_time = {}us",
            metrics.stream,
            convert(metrics.serialized_data_size as f64),
            convert(metrics.compressed_data_size as f64),
            metrics.avg_serialization_time.as_micros(),
            metrics.avg_compression_time.as_micros()
        );
    }
}

// Updates serializer metrics and adds it into a queue to push later
fn save_and_prepare_next_metrics(
    pending: &mut VecDeque<SerializerMetrics>,
    metrics: &mut Metrics,
    stream_metrics: &mut HashMap<String, StreamMetrics>,
    storage_handler: &StorageHandler,
) {
    storage_handler.update_metrics(metrics);
    let m = Box::new(metrics.clone());
    pending.push_back(SerializerMetrics::Main(m));
    metrics.prepare_next();

    for metrics in stream_metrics.values_mut() {
        metrics.prepare_snapshot();
        let m = Box::new(metrics.clone());
        pending.push_back(SerializerMetrics::Stream(m));
        metrics.prepare_next();
    }
}

// Updates serializer metrics and pushes it directly to network
fn check_and_flush_metrics(
    pending: &mut VecDeque<SerializerMetrics>,
    metrics: &mut Metrics,
    metrics_tx: &Sender<SerializerMetrics>,
    storage_handler: &StorageHandler,
) -> Result<(), TrySendError<SerializerMetrics>> {
    use pretty_bytes::converter::convert;

    storage_handler.update_metrics(metrics);
    // Send pending metrics. This signifies state change
    while let Some(metrics) = pending.front() {
        match metrics {
            SerializerMetrics::Main(metrics) => {
                // Always send pending metrics. They represent state changes
                info!(
                    "{:>17}: batches = {:<3} errors = {} lost = {} disk_files = {:<3} disk_utilized = {} write_memory = {} read_memory = {}",
                    metrics.mode,
                    metrics.batches,
                    metrics.errors,
                    metrics.lost_segments,
                    metrics.disk_files,
                    convert(metrics.disk_utilized as f64),
                    convert(metrics.write_memory as f64),
                    convert(metrics.read_memory as f64),
                );
                metrics_tx.try_send(SerializerMetrics::Main(metrics.clone()))?;
                pending.pop_front();
            }
            SerializerMetrics::Stream(metrics) => {
                // Always send pending metrics. They represent state changes
                info!(
                    "{:>17}: serialized_data_size = {} compressed_data_size = {} avg_serialization_time = {}us avg_compression_time = {}us",
                    metrics.stream,
                    convert(metrics.serialized_data_size as f64),
                    convert(metrics.compressed_data_size as f64),
                    metrics.avg_serialization_time.as_micros(),
                    metrics.avg_compression_time.as_micros()
                );
                metrics_tx.try_send(SerializerMetrics::Stream(metrics.clone()))?;
                pending.pop_front();
            }
        }
    }

    if metrics.batches() > 0 {
        info!(
            "{:>17}: batches = {:<3} errors = {} lost = {} disk_files = {:<3} disk_utilized = {} write_memory = {} read_memory = {}",
            metrics.mode,
            metrics.batches,
            metrics.errors,
            metrics.lost_segments,
            metrics.disk_files,
            convert(metrics.disk_utilized as f64),
            convert(metrics.write_memory as f64),
            convert(metrics.read_memory as f64),
        );

        metrics_tx.try_send(SerializerMetrics::Main(Box::new(metrics.clone())))?;
        metrics.prepare_next();
    }

    Ok(())
}

/// Command to remotely trigger `Serializer` shutdown
pub(crate) struct SerializerShutdown;

/// Handle to send control messages to `Serializer`
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub(crate) inner: Sender<SerializerShutdown>,
}

impl CtrlTx {
    /// Triggers shutdown of `Serializer`
    pub async fn trigger_shutdown(&self) {
        self.inner.send_async(SerializerShutdown).await.unwrap()
    }
}

// TODO(RT): Test cases
// - Restart with no internet but files on disk

#[cfg(test)]
pub mod tests {
    use serde_json::Value;
    use tokio::{spawn, time::sleep};

    use crate::{
        config::MqttConfig,
        mock::{MockClient, MockCollector},
    };

    use super::*;

    fn read_from_storage(storage: &mut Storage, max_packet_size: usize) -> Publish {
        if let Err(storage::Error::Done) = storage.reload_on_eof() {
            panic!("No publishes found in storage");
        }

        match Packet::read(storage.inner.reader(), max_packet_size) {
            Ok(Packet::Publish(publish)) => return publish,
            v => {
                panic!("Failed to read publish from storage. read: {:?}", v);
            }
        }
    }

    pub fn default_config() -> Config {
        Config {
            streams: HashMap::new(),
            mqtt: MqttConfig { max_packet_size: 1024 * 1024, ..Default::default() },
            ..Default::default()
        }
    }

    pub fn defaults(
        config: Arc<Config>,
    ) -> (Serializer<MockClient>, Sender<Box<dyn Package>>, Receiver<Request>) {
        let (data_tx, data_rx) = bounded(1);
        let (net_tx, net_rx) = bounded(1);
        let (metrics_tx, _metrics_rx) = bounded(1);
        let client = MockClient { net_tx };

        (Serializer::new(config, data_rx, client, metrics_tx).unwrap(), data_tx, net_rx)
    }

    #[tokio::test]
    // Force runs serializer in normal mode, without persistence
    async fn normal_to_slow() {
        let config = default_config();
        let (mut serializer, data_tx, net_rx) = defaults(Arc::new(config));

        // Slow Network, takes packets only once in 10s
        spawn(async move {
            loop {
                sleep(Duration::from_secs(10)).await;
                net_rx.recv_async().await.unwrap();
            }
        });

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
        spawn(async move {
            for i in 1..3 {
                collector.send(i).await.unwrap();
            }
        });

        match serializer.normal().await.unwrap() {
            Status::SlowEventloop(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }, _) => {
                assert_eq!(topic, "hello/world");
                let recvd: Value = serde_json::from_slice(&payload).unwrap();
                let obj = &recvd.as_array().unwrap()[0];
                assert_eq!(obj.get("msg"), Some(&Value::from("Hello, World!")));
            }
            s => panic!("Unexpected status: {:?}", s),
        }
    }

    #[test]
    // Force write publish to storage and verify by reading back
    fn read_write_storage() {
        let config = Arc::new(default_config());

        let mut storage = Storage::new("hello/world", 1024, false);
        let mut publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        storage.write(publish.clone()).unwrap();

        let stored_publish = read_from_storage(&mut storage, config.mqtt.max_packet_size);

        // Ensure publish.pkid is 1, as written to disk
        publish.pkid = 1;
        assert_eq!(publish, stored_publish);
    }

    #[tokio::test]
    // Force runs serializer in disk mode, with network returning
    async fn disk_to_catchup() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, net_rx) = defaults(config);

        // Slow Network, takes packets only once in 10s
        spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                net_rx.recv_async().await.unwrap();
            }
        });

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
        // Faster collector, send data every 5s
        spawn(async move {
            for i in 1..10 {
                collector.send(i).await.unwrap();
                sleep(Duration::from_secs(3)).await;
            }
        });

        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}}]".as_bytes(),
        );
        let status = serializer.slow(publish, Arc::new(Default::default())).await.unwrap();

        assert_eq!(status, Status::EventLoopReady);
    }

    #[tokio::test]
    // Force runs serializer in disk mode, with crashed network
    async fn disk_to_crash() {
        let config = Arc::new(default_config());
        let (mut serializer, data_tx, _) = defaults(config);

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
        // Faster collector, send data every 5s
        spawn(async move {
            for i in 1..10 {
                collector.send(i).await.unwrap();
                sleep(Duration::from_secs(3)).await;
            }
        });

        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );

        match serializer
            .slow(
                publish,
                Arc::new(StreamConfig { topic: "hello/world".to_string(), ..Default::default() }),
            )
            .await
            .unwrap()
        {
            Status::EventLoopCrash(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }, _) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            s => panic!("Unexpected status: {:?}", s),
        }
    }

    #[tokio::test]
    // Force runs serializer in catchup mode, with empty persistence
    async fn catchup_to_normal_empty_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, _, _) = defaults(config);

        let status = serializer.catchup().await.unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[tokio::test]
    // Force runs serializer in catchup mode, with data already in persistence
    async fn catchup_to_normal_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, net_rx) = defaults(config);

        let storage = serializer
            .storage_handler
            .map
            .entry(Arc::new(Default::default()))
            .or_insert(Storage::new("hello/world", 1024, false));

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
        // Run a collector practically once
        spawn(async move {
            for i in 2..6 {
                collector.send(i).await.unwrap();
                sleep(Duration::from_secs(100)).await;
            }
        });

        // Decent network that lets collector push data once into storage
        spawn(async move {
            sleep(Duration::from_secs(5)).await;
            for i in 1..6 {
                match net_rx.recv_async().await.unwrap() {
                    Request::Publish(Publish { payload, .. }) => {
                        let recvd = String::from_utf8(payload.to_vec()).unwrap();
                        let expected = format!(
                            "[{{\"sequence\":{i},\"timestamp\":0,\"msg\":\"Hello, World!\"}}]",
                        );
                        assert_eq!(recvd, expected)
                    }
                    r => unreachable!("Unexpected request: {:?}", r),
                }
            }
        });

        // Force write a publish into storage
        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        storage.write(publish.clone()).unwrap();

        let status = serializer.catchup().await.unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[tokio::test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    async fn catchup_to_crash_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, _) = defaults(config);

        let storage = serializer
            .storage_handler
            .map
            .entry(Arc::new(StreamConfig {
                topic: "hello/world".to_string(),
                ..Default::default()
            }))
            .or_insert(Storage::new("hello/world", 1024, false));

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
        // Run a collector
        spawn(async move {
            for i in 2..6 {
                collector.send(i).await.unwrap();
                sleep(Duration::from_secs(10)).await;
            }
        });

        // Force write a publish into storage
        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        storage.write(publish.clone()).unwrap();

        match serializer.catchup().await.unwrap() {
            Status::EventLoopCrash(Publish { topic, payload, .. }, _) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            s => unreachable!("Unexpected status: {:?}", s),
        }
    }

    #[tokio::test]
    // Ensures that the data of streams are removed on the basis of preference
    async fn preferential_send_on_network() {
        let mut config = default_config();
        config.stream_metrics.timeout = Duration::from_secs(1000);
        config.streams.extend([
            (
                "one".to_owned(),
                StreamConfig { topic: "topic/one".to_string(), priority: 1, ..Default::default() },
            ),
            (
                "two".to_owned(),
                StreamConfig { topic: "topic/two".to_string(), priority: 2, ..Default::default() },
            ),
            (
                "top".to_owned(),
                StreamConfig {
                    topic: "topic/top".to_string(),
                    priority: u8::MAX,
                    ..Default::default()
                },
            ),
        ]);
        let config = Arc::new(config);

        let (mut serializer, _data_tx, req_rx) = defaults(config.clone());

        let publish = |topic: String, i: u32| Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic,
            pkid: 0,
            payload: Bytes::from(i.to_string()),
        };

        let one = serializer
            .storage_handler
            .map
            .entry(Arc::new(StreamConfig {
                topic: "topic/one".to_string(),
                priority: 1,
                ..Default::default()
            }))
            .or_insert_with(|| unreachable!());
        one.write(publish("topic/one".to_string(), 1)).unwrap();
        one.write(publish("topic/one".to_string(), 10)).unwrap();

        let top = serializer
            .storage_handler
            .map
            .entry(Arc::new(StreamConfig {
                topic: "topic/top".to_string(),
                priority: u8::MAX,
                ..Default::default()
            }))
            .or_insert_with(|| unreachable!());
        top.write(publish("topic/top".to_string(), 100)).unwrap();
        top.write(publish("topic/top".to_string(), 1000)).unwrap();

        let two = serializer
            .storage_handler
            .map
            .entry(Arc::new(StreamConfig {
                topic: "topic/two".to_string(),
                priority: 2,
                ..Default::default()
            }))
            .or_insert_with(|| unreachable!());
        two.write(publish("topic/two".to_string(), 3)).unwrap();

        let default = serializer
            .storage_handler
            .map
            .entry(Arc::new(StreamConfig {
                topic: "topic/default".to_string(),
                priority: 0,
                ..Default::default()
            }))
            .or_insert(Storage::new("topic/default", 1024, false));
        default.write(publish("topic/default".to_string(), 0)).unwrap();
        default.write(publish("topic/default".to_string(), 2)).unwrap();

        // run serializer in the background
        spawn(async { serializer.start().await.unwrap() });

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/top");
        assert_eq!(payload, "100");

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/top");
        assert_eq!(payload, "1000");

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/two");
        assert_eq!(payload, "3");

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/one");
        assert_eq!(payload, "1");

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/one");
        assert_eq!(payload, "10");

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/default");
        assert_eq!(payload, "0");

        let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/default");
        assert_eq!(payload, "2");
    }
}
