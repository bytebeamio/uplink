mod metrics;
pub(crate) mod storage;

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use flume::{Receiver, Sender};
use lz4_flex::frame::FrameEncoder;
use pretty_bytes::converter::convert;
use replace_with::replace_with_or_abort;
use rumqttc::*;
use tokio::{select, time::interval};

use crate::uplink_config::{Compression, StreamConfig};
use crate::{Config};
pub use metrics::{Metrics, SerializerMetrics, StreamMetrics};
use crate::base::bridge::stream::MessageBuffer;
use crate::base::clock;
use crate::base::serializer::storage::{Storage, StorageEnum};
use crate::utils::BTreeCursorMut;

const METRICS_INTERVAL: Duration = Duration::from_secs(10);

/// We attempt `MqttClient::try_publish` in normal mode. If it fails, we move to slow mode (mqtt queue is full)
/// We attempt `MqttClient::publish` in slow and catchup modes (blocking, async). If it fails, something went wrong in rumqtt and we move to crash mode
/// TODO: The above two method return the same error if the topic is invalid. That needs to be handled properly (ignore message or make sure topics are always valid)
#[derive(thiserror::Error, Debug)]
pub enum MqttError {
    #[error("SendError(..)")]
    Send(Publish),
    #[error("TrySendError(..)")]
    TrySend(Publish),
    #[error("Unknown error")]
    UnknownError,
}

impl From<ClientError> for MqttError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Request(Request::Publish(publish)) => MqttError::Send(publish),
            ClientError::TryRequest(Request::Publish(publish)) => MqttError::TrySend(publish),
            _ => MqttError::UnknownError,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Status {
    Normal,
    SlowEventloop(Publish, Arc<StreamConfig>),
    EventLoopReady,
    EventLoopCrash,
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

pub struct Serializer<C: MqttClient> {
    config: Arc<Config>,
    tenant_filter: String,
    collector_rx: Receiver<Box<MessageBuffer>>,
    client: C,
    metrics_tx: Sender<SerializerMetrics>,
    /// Serializer metrics
    metrics: Metrics,
    /// Updated in `construct_publish` function
    /// Read and reset in `send_stream_metrics`
    stream_metrics: HashMap<String, StreamMetrics>,
    /// a monotonically increasing counter
    /// used to track when was the last time live data for this stream was pushed
    /// when fetching packets, we sort by this and return the live data that has the most stale data
    /// if this isn't done, live data for a high frequency stream can block live data for other streams
    live_data_clock: usize,
    sorted_storages: BTreeMap<Arc<StreamConfig>, (StorageEnum, Option<Publish>, usize)>,
    ctrl_rx: Receiver<()>,
}

impl<C: MqttClient> Serializer<C> {
    /// Construct the uplink Serializer with the necessary configuration details, a receiver handle to accept data payloads from,
    /// the handle to an MQTT client(This is constructed as such for testing purposes) and a handle to update serailizer metrics.
    pub fn new(
        config: Arc<Config>,
        tenant_filter: String,
        collector_rx: Receiver<Box<MessageBuffer>>,
        client: C,
        metrics_tx: Sender<SerializerMetrics>,
        ctrl_rx: Receiver<()>,
    ) -> Serializer<C> {
        let mut result = Serializer {
            config,
            tenant_filter,
            collector_rx,
            client,
            metrics_tx,
            metrics: Metrics::new("catchup"),
            stream_metrics: Default::default(),
            live_data_clock: 0,
            sorted_storages: BTreeMap::new(),
            ctrl_rx,
        };
        result.initialize_storages();
        result
    }

    fn initialize_storages(&mut self) {
        for stream_config in self.config.streams.values() {
            self.sorted_storages.insert(Arc::new(stream_config.clone()), (self.create_storage_for_stream(stream_config), None, 0));
        }
    }

    fn create_storage_for_stream(&self, config: &StreamConfig) -> StorageEnum {
        if config.persistence.max_file_count == 0 {
            StorageEnum::InMemory(storage::InMemoryStorage::new(config.name.as_str(), config.persistence.max_file_size, self.config.mqtt.max_packet_size))
        } else {
            match storage::DirectoryStorage::new(
                self.config.persistence_path.join(config.name.as_str()),
                config.persistence.max_file_size, config.persistence.max_file_count,
                self.config.mqtt.max_packet_size,
            ) {
                Ok(s) => StorageEnum::Directory(s),
                Err(e) => {
                    log::error!("Failed to initialize disk backed storage for {} : {e}, falling back to in memory persistence", config.name);
                    StorageEnum::InMemory(storage::InMemoryStorage::new(config.name.as_str(), config.persistence.max_file_size, self.config.mqtt.max_packet_size))
                }
            }
        }
    }

    /// Returns None if nothing is left (time to move to normal mode)
    /// Prioritize live data over saved data
    /// Prioritize old live data over new live data, to ensure live data for all the streams is pushed
    fn fetch_next_packet_from_storage(&mut self) -> Option<(Publish, Arc<StreamConfig>)> {
        if let Some((sk, (_, live_data, live_data_version))) = self.sorted_storages.iter_mut()
            .filter(|(_, (_, live_data, _))| live_data.is_some())
            .min_by_key(|(_, (_, _, live_data_version))| *live_data_version) {
            self.live_data_clock += 1;
            *live_data_version = self.live_data_clock;
            return Some((live_data.take().unwrap(), sk.clone()));
        }
        let mut cursor = BTreeCursorMut::new(&mut self.sorted_storages);
        while let Some((sk, (storage, _, _))) = cursor.current.as_mut() {
            match storage.read_packet() {
                Ok(packet) => {
                    if packet.topic.starts_with(&self.tenant_filter) {
                        return Some((packet, sk.clone()));
                    } else {
                        log::warn!("found data for wrong tenant in persistence!");
                        continue;
                    }
                }
                Err(storage::StorageReadError::Empty) => {
                    cursor.bump();
                }
                Err(storage::StorageReadError::FileSystemError(e)) => {
                    log::error!("Encountered file system error when reading packet for stream({}): {e}, falling back to in memory persistence", storage.name());
                    replace_with_or_abort(storage, |s| {
                        s.to_in_memory()
                    });
                }
                Err(storage::StorageReadError::InvalidPacket(e)) => {
                    log::error!("Found invalid packet when reading from storage for stream({}): {e}", storage.name());
                }
                Err(storage::StorageReadError::UnsupportedPacketType) => {
                    log::error!("Found unsupported packet type when reading from storage for stream({})", storage.name());
                }
            }
        }
        None
    }

    fn write_package_to_storage(&mut self, data: Box<MessageBuffer>) {
        let stream_config = data.stream_config.clone();
        let publish = construct_publish(data, &mut self.stream_metrics);
        self.write_publish_to_storage(stream_config, publish);
    }

    fn write_publish_to_storage(&mut self, sk: Arc<StreamConfig>, mut publish: Publish) {
        publish.pkid = 1;
        if ! self.sorted_storages.contains_key(&sk) {
            self.sorted_storages.insert(sk.clone(), (self.create_storage_for_stream(&sk), None, 0));
        }

        let mut packet_to_write = Some(publish);
        let (storage, live_data, _) = self.sorted_storages.get_mut(&sk).unwrap();
        if self.config.prioritize_live_data {
            std::mem::swap(&mut packet_to_write, live_data);
        }
        if let Some(publish) = packet_to_write {
            match storage.write_packet(publish) {
                Ok(_) => {}
                Err(storage::StorageWriteError::FileSystemError(e)) => {
                    log::error!("Encountered file system error when reading packet for stream({}): {e}, falling back to in memory persistence", sk.name);
                    let (old_storage, old_last_value, old_live_data_version) = self.sorted_storages.remove(&sk).unwrap();
                    self.sorted_storages.insert(sk.clone(), (old_storage.to_in_memory(), old_last_value, old_live_data_version));
                }
                Err(storage::StorageWriteError::InvalidPacket(e)) => {
                    log::error!("Found invalid packet when writing to storage for stream({}): {e}", sk.name);
                }
            };
        }
    }

    fn flush_storage(&mut self) {
        for (_, (storage, live_data, _)) in self.sorted_storages.iter_mut() {
            if let Some(publish) = live_data.take() {
                if let Err(e) = storage.write_packet(publish) {
                    log::error!("Couldn't write live data to storage while flushing for stream({}) : {e:?}", storage.name());
                }
            }
            if let Err(storage::StorageFlushError::FileSystemError(e)) = storage.flush() {
                log::error!("Couldn't flush storage for stream({}) : {e}", storage.name());
            }
        }
    }

    /// TODO: send these metrics to storage
    fn send_serializer_metrics(&mut self) {
        let metrics = &mut self.metrics;
        metrics.sequence += 1;

        // Reset parameters derived from storage
        metrics.timestamp = clock();
        metrics.disk_files = 0;
        metrics.disk_utilized = 0;
        metrics.lost_segments = 0;
        metrics.read_memory = 0;
        metrics.write_memory = 0;
        // calculate parameters derived from storage
        for (storage, _, _) in self.sorted_storages.values() {
            let sm = storage.metrics();
            metrics.disk_files += sm.files_count as usize;
            metrics.disk_utilized += sm.bytes_on_disk as usize;
            metrics.lost_segments += sm.lost_files as usize;
            metrics.read_memory += sm.read_buffer_size as usize;
            metrics.write_memory += sm.write_buffer_size as usize;
        }

        if metrics.batches > 0 {
            log::info!(
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
            let _ = self.metrics_tx.try_send(SerializerMetrics::Main(Box::new(metrics.clone())));
        }

        metrics.batches = 0;
        metrics.sent_size = 0;
    }

    fn send_stream_metrics(&mut self) {
        for metrics in self.stream_metrics.values_mut() {
            metrics.prepare_snapshot();
            let log_message = format!(
                "{:>17}: serialized_data_size = {} compressed_data_size = {} avg_serialization_time = {}us avg_compression_time = {}us",
                metrics.stream,
                convert(metrics.serialized_data_size as f64),
                convert(metrics.compressed_data_size as f64),
                metrics.avg_serialization_time.as_micros(),
                metrics.avg_compression_time.as_micros()
            );
            if metrics.serialized_data_size == 0 {
                log::debug!("{}", log_message);
            } else {
                log::info!("{}", log_message);
            }
            let _ = self.metrics_tx.try_send(SerializerMetrics::Stream(Box::new(metrics.clone())));
            metrics.prepare_next();
        }
    }

    /// Write all data received, from here-on, to disk only, shutdown serializer
    /// after handling all data payloads.
    fn shutdown(&mut self) {
        log::debug!("Forced into shutdown mode, writing all incoming data to persistence.");

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(data) = self.collector_rx.recv_deadline(deadline) {
                self.write_package_to_storage(data);
            } else {
                self.flush_storage();
                break;
            }
        }
    }

    /// Write all data received, from here-on, to disk only.
    async fn crash(&mut self) -> Status {
        loop {
            if let Ok(data) = self.collector_rx.recv_async().await {
                self.write_package_to_storage(data);
            } else {
                self.flush_storage();
                return Status::Shutdown;
            }
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    async fn slow(&mut self, publish_in_transit: Publish, stream: Arc<StreamConfig>) -> Status {
        let mut interval = interval(METRICS_INTERVAL);
        // Reactlabs setup processes logs generated by uplink
        log::info!("Switching to slow eventloop mode!!");
        self.metrics.mode = "slow";

        let publish = send_publish(self.client.clone(), publish_in_transit.topic.clone(), publish_in_transit.payload.clone());
        tokio::pin!(publish);

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    match data {
                        Ok(data) => {
                            self.metrics.batches += 1;
                            self.write_package_to_storage(data);
                        }
                        Err(_) => {
                            self.write_publish_to_storage(stream, publish_in_transit);
                            return Status::Shutdown;
                        }
                    }
                }
                o = &mut publish => match o {
                    Ok(_) => break Status::EventLoopReady,
                    Err(MqttError::Send(publish)) => {
                        self.write_publish_to_storage(stream, publish);
                        return Status::EventLoopCrash;
                    }
                    _ => {},
                },
                _ = interval.tick() => {
                    self.send_serializer_metrics();
                    self.send_stream_metrics();
                }
                // Transition into crash mode when uplink is shutting down
                Ok(_) = self.ctrl_rx.recv_async() => {
                    self.write_publish_to_storage(stream, publish_in_transit);
                    return Status::Shutdown;
                }
            }
        }
    }

    /// Write new collector data to disk while sending existing data on
    /// disk to mqtt eventloop. Collector rx is selected with blocking
    /// `publish` instead of `try publish` to ensure that transient back
    /// pressure due to a lot of data on disk doesn't switch state to
    /// `Status::SlowEventLoop`
    async fn catchup(&mut self) -> Status {
        // Reactlabs setup processes logs generated by uplink
        log::info!("Switching to catchup mode!!");

        self.metrics.mode = "catchup";
        let mut interval = interval(METRICS_INTERVAL);

        // Write publish_in_transit to storage in case of shutdown or crash
        let (mut publish_in_transit, mut last_publish_stream) = if let Some(publish) = self.fetch_next_packet_from_storage() {
            publish
        } else {
            return Status::Normal;
        };
        let mut last_publish_sent_size = publish_in_transit.payload.len();

        let send = send_publish(self.client.clone(), publish_in_transit.topic.clone(), publish_in_transit.payload.clone());
        tokio::pin!(send);

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    match data {
                        Ok(data) => {
                            self.metrics.batches += 1;
                            self.write_package_to_storage(data);
                        }
                        Err(_) => {
                            self.write_publish_to_storage(last_publish_stream, publish_in_transit);
                            return Status::Shutdown
                        }
                    }
                }
                send_result = &mut send => {
                    match send_result {
                        Ok(_) => {},
                        Err(MqttError::Send(publish)) => {
                            self.write_publish_to_storage(last_publish_stream, publish);
                            return Status::EventLoopCrash;
                        },
                        Err(_) => {},
                    };
                    match self.fetch_next_packet_from_storage() {
                        Some((publish, stream)) => {
                            self.metrics.sent_size += last_publish_sent_size;
                            last_publish_stream = stream;
                            last_publish_sent_size = publish.payload.len();
                            send.set(send_publish(self.client.clone(), publish.topic.clone(), publish.payload.clone()));
                            publish_in_transit = publish;
                        }
                        None => {
                            return Status::Normal;
                        }
                    }
                }
                _ = interval.tick() => {
                    self.send_serializer_metrics();
                    self.send_stream_metrics();
                }
                Ok(_) = self.ctrl_rx.recv_async() => {
                    self.write_publish_to_storage(last_publish_stream, publish_in_transit);
                    return Status::Shutdown
                }
            }
        }
    }

    async fn normal(&mut self) -> Status {
        let mut interval = interval(METRICS_INTERVAL);
        self.metrics.mode = "normal";
        // Reactlabs setup processes logs generated by uplink
        log::info!("Switching to normal mode!!");

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = match data {
                        Ok(data) => data,
                        Err(_) => {
                            return Status::Shutdown;
                        }
                    };
                    self.metrics.batches += 1;
                    let stream = data.stream_config.clone();
                    let publish = construct_publish(data, &mut self.stream_metrics);
                    let payload_size = publish.payload.len();
                    match self.client.try_publish(&stream.topic, QoS::AtLeastOnce, false, publish.payload) {
                        Ok(_) => {
                            self.metrics.sent_size += payload_size;
                        }
                        Err(MqttError::TrySend(publish)) => {
                            return Status::SlowEventloop(publish, stream);
                        },
                        _ => {}
                    }
                }
                // On a regular interval, forwards metrics information to network
                _ = interval.tick() => {
                    self.send_serializer_metrics();
                    self.send_stream_metrics();
                }
                // Transition into crash mode when uplink is shutting down
                Ok(_) = self.ctrl_rx.recv_async() => {
                    return Status::Shutdown
                }
            }
        }
    }

    /// Starts operation of the uplink serializer, which can transition between the modes mentioned earlier.
    pub async fn start(mut self) {
        let mut status = Status::EventLoopReady;

        loop {
            let next_status = match status {
                Status::Normal => self.normal().await,
                Status::SlowEventloop(publish, stream) => self.slow(publish, stream).await,
                Status::EventLoopReady => self.catchup().await,
                Status::EventLoopCrash => self.crash().await,
                Status::Shutdown => break,
            };

            status = next_status;
        }

        self.shutdown();

        log::info!("Serializer has handled all pending packets, shutting down");
    }
}

async fn send_publish<C: MqttClient, V: Into<Vec<u8>> + Send>(
    client: C,
    topic: String,
    payload: V,
) -> Result<(), MqttError> {
    log::debug!("publishing on {topic}");
    client.publish(topic, QoS::AtLeastOnce, false, payload).await?;
    Ok(())
}

fn lz4_compress(payload: &mut Vec<u8>) {
    let mut compressor = FrameEncoder::new(vec![]);
    // Below functions fail in case of IO errors
    // so these unwraps are safe because we are doing in memory compression
    compressor.write_all(payload).unwrap();
    *payload = compressor.finish().unwrap();
}

#[allow(clippy::boxed_local)]
pub fn construct_publish(
    data: Box<MessageBuffer>,
    stream_metrics: &mut HashMap<String, StreamMetrics>,
) -> Publish {
    let stream_name = data.stream_name.as_ref().clone();
    let stream_config = data.stream_config.clone();
    let point_count = data.buffer.len();
    let batch_latency = data.latency();
    log::trace!("Data received on stream: {stream_name}; message count = {point_count}; batching latency = {batch_latency}");

    let topic = stream_config.topic.clone();

    let metrics = stream_metrics
        .entry(stream_name.clone())
        .or_insert_with(|| StreamMetrics::new(&stream_name));

    let serialization_start = Instant::now();
    let mut payload = data.serialize();
    let serialization_time = serialization_start.elapsed();
    metrics.add_serialization_time(serialization_time);

    let data_size = payload.len();
    let mut compressed_data_size = None;

    if let Compression::Lz4 = stream_config.compression {
        let compression_start = Instant::now();
        lz4_compress(&mut payload);
        let compression_time = compression_start.elapsed();
        metrics.add_compression_time(compression_time);

        compressed_data_size = Some(payload.len());
    }

    metrics.add_serialized_sizes(data_size, compressed_data_size);

    Publish::new(topic, QoS::AtLeastOnce, payload)
}

