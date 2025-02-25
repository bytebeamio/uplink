mod metrics;
pub(crate) mod storage;

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use flume::{Receiver, Sender};
use log::trace;
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
        let persistence = config.persistence.unwrap_or(self.config.default_persistence);
        if persistence.max_file_count == 0 {
            StorageEnum::InMemory(storage::InMemoryStorage::new(config.name.as_str(), persistence.max_file_size, self.config.mqtt.max_packet_size))
        } else {
            match storage::DirectoryStorage::new(
                self.config.persistence_path.join(config.name.as_str()),
                persistence.max_file_size, persistence.max_file_count,
                self.config.mqtt.max_packet_size,
            ) {
                Ok(s) => StorageEnum::Directory(s),
                Err(e) => {
                    log::error!("Failed to initialize disk backed storage for {} : {e}, falling back to in memory persistence", config.name);
                    StorageEnum::InMemory(storage::InMemoryStorage::new(config.name.as_str(), persistence.max_file_size, self.config.mqtt.max_packet_size))
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
                    if let Ok(payload_str) = std::str::from_utf8(publish.payload.as_ref()) {
                        trace!("publishing payload for stream: {} : {:?}", stream.name, payload_str);
                    }
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

#[cfg(test)]
pub mod tests {
    use std::path::PathBuf;
    use std::thread::JoinHandle;
    use bytes::Bytes;
    use flume::bounded;
    use serde::Deserialize;
    use serde_json::Value;
    use tokio::{spawn, time::sleep};

    use crate::{uplink_config::MqttConfig, hashmap, mock::{MockClient, MockCollector}};
    use crate::base::bridge::Payload;
    use crate::base::bridge::stream::MessageBuffer;
    use crate::uplink_config::Persistence;
    use super::*;
    use crate::uplink_config::StreamConfig;

    pub fn default_config() -> Config {
        Config {
            streams: HashMap::new(),
            mqtt: MqttConfig { max_packet_size: 1024 * 1024, max_inflight: 100, keep_alive: 30, network_timeout: 30 },
            ..Default::default()
        }
    }

    pub fn defaults(
        config: Arc<Config>,
    ) -> (Serializer<MockClient>, Sender<Box<MessageBuffer>>, Receiver<Request>, Sender<()>) {
        let (data_tx, data_rx) = bounded(0);
        let (net_tx, net_rx) = bounded(0);
        let (metrics_tx, _metrics_rx) = bounded(1);
        let (ctrl_tx, ctrl_rx) = bounded(1);
        let client = MockClient { net_tx };

        (Serializer::new(config, String::new(), data_rx, client, metrics_tx, ctrl_rx), data_tx, net_rx, ctrl_tx)
    }

    #[tokio::test]
    // Force runs serializer in normal mode, without persistence
    async fn normal_to_slow() {
        let config = default_config();
        let (mut serializer, data_tx, net_rx, _) = defaults(Arc::new(config));

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

        match serializer.normal().await {
            Status::SlowEventloop(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }, _) => {
                assert_eq!(topic, "hello/world");
                let recvd: Value = serde_json::from_slice(&payload).unwrap();
                let obj = &recvd.as_array().unwrap()[0];
                assert_eq!(obj.get("msg"), Some(&Value::from("Hello, World!")));
            }
            s => panic!("Unexpected status: {:?}", s),
        }
    }

    #[tokio::test]
    async fn directory_storage_queue_behavior() {
        stream_queue_behavior(10240000, 100, 100000).await;
    }

    #[tokio::test]
    async fn in_memory_storage_queue_behavior() {
        stream_queue_behavior(10000, 0, 150).await;
    }

    #[allow(unused)]
    #[derive(Debug, Deserialize)]
    struct SerializedPayload {
        pub sequence: u32,
        pub timestamp: u64,
        #[serde(flatten)]
        pub payload: Value,
    }

    /// create an in memory persistence stream
    /// Write some data to it
    /// Read a packet so that read and write buffers are swapped
    /// Write data to it and force it to convert to in memory storage
    /// Read all packets and verify the behavior
    #[tokio::test]
    async fn directory_to_in_memory_queue_behavior() {
        let temp_dir = tempdir::TempDir::new("uplink").unwrap();
        let sk = Arc::new(create_test_stream_config());
        let config = Config {
            persistence_path: temp_dir.path().to_owned(),
            streams: hashmap!(
                "test_stream".to_owned() => sk.as_ref().clone()
            ),
            mqtt: MqttConfig {
                max_packet_size: 2560000,
                max_inflight: 10,
                keep_alive: 30,
                network_timeout: 30,
            },
            ..Default::default()
        };
        let config = Arc::new(config);
        let (serializer, data_tx, net_rx, _) = defaults(config);

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .thread_name("mock serializer")
                .build()
                .unwrap()
                .block_on(serializer.start());
        });

        for i in 0..50 {
            data_tx.send(Box::new(create_test_buffer(i, sk.clone()))).unwrap();
        }
        std::fs::remove_dir_all(temp_dir.path()).unwrap();

        if let Request::Publish(publish) = net_rx.recv_async().await.unwrap() {
            let payloads = serde_json::from_slice::<Vec<SerializedPayload>>(publish.payload.as_ref()).unwrap();
            assert_eq!(payloads.len(), 1);
            let payload = payloads.first().unwrap();
            assert_eq!(payload.sequence, 0);
            assert_eq!(payload.payload, serde_json::json!({ "msg": "Hello, World!" }));
        } else {
            assert!(false);
        }

        for i in 50..148 {
            data_tx.send(Box::new(create_test_buffer(i, sk.clone()))).unwrap();
        }

        for i in 1..147 {
            if let Request::Publish(publish) = net_rx.recv_async().await.unwrap() {
                let payloads = serde_json::from_slice::<Vec<SerializedPayload>>(publish.payload.as_ref()).unwrap();
                assert_eq!(payloads.len(), 1);
                let payload = payloads.first().unwrap();
                assert_eq!(payload.sequence, i);
                assert_eq!(payload.payload, serde_json::json!({ "msg": "Hello, World!" }));
            } else {
                assert!(false);
            }
        }
    }

    fn create_test_buffer(i: u32, sk: Arc<StreamConfig>) -> MessageBuffer {
        let mut buffer = MessageBuffer::new(Arc::new("test_stream".to_owned()), sk);
        buffer.buffer.push(Payload {
            stream: Default::default(),
            sequence: i,
            timestamp: 0,
            payload: serde_json::json!({ "msg": "Hello, World!" }),
        });
        buffer
    }

    fn create_test_stream_config() -> StreamConfig {
        StreamConfig {
            name: "test_stream".to_string(),
            topic: "/tenants/demo/devices/1/events/test_stream/jsonarray".to_string(),
            batch_size: 2,
            flush_period: Duration::from_secs(1),
            compression: Compression::Disabled,
            persistence: Persistence {
                max_file_size: 10800,
                max_file_count: 10,
            },
            priority: 0,
        }
    }

    fn _get_test_buffer_size() {
        let sk = Arc::new(StreamConfig {
            name: "test_stream".to_string(),
            topic: "/tenants/demo/devices/1/events/test_stream/jsonarray".to_string(),
            batch_size: 2,
            flush_period: Duration::from_secs(1),
            compression: Compression::Disabled,
            persistence: Persistence {
                max_file_size: 100000,
                max_file_count: 100000,
            },
            priority: 0,
        });
        let data = Box::new(create_test_buffer(0, sk));
        let publish = construct_publish(data, &mut HashMap::new());
        dbg!(publish.size());
    }

    /// Each publish is 100 bytes
    async fn stream_queue_behavior(max_file_size: usize, max_file_count: usize, number_of_samples: u32) {
        let temp_dir = tempdir::TempDir::new("uplink").unwrap();
        let sk = Arc::new(StreamConfig {
            name: "test_stream".to_string(),
            topic: "/tenants/demo/devices/1/events/test_stream/jsonarray".to_string(),
            batch_size: 2,
            flush_period: Duration::from_secs(1),
            compression: Compression::Disabled,
            persistence: Persistence {
                max_file_size,
                max_file_count,
            },
            priority: 0,
        });
        let config = Config {
            persistence_path: temp_dir.path().to_owned(),
            streams: hashmap!(
                "test_stream".to_owned() => sk.as_ref().clone()
            ),
            mqtt: MqttConfig {
                max_packet_size: 25600,
                max_inflight: 10,
                keep_alive: 30,
                network_timeout: 30,
            },
            ..Default::default()
        };
        let config = Arc::new(config);

        let (serializer, data_tx, net_rx, _) = defaults(config);

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .thread_name("mock serializer")
                .build()
                .unwrap()
                .block_on(serializer.start());
        });

        for i in 0..number_of_samples {
            data_tx.send(Box::new(create_test_buffer(i, sk.clone()))).unwrap();
        }

        for i in 0..number_of_samples {
            let result = net_rx.recv().unwrap();
            match result {
                Request::Publish(publish) => {
                    let payloads = serde_json::from_slice::<Vec<SerializedPayload>>(publish.payload.as_ref()).unwrap();
                    assert_eq!(payloads.len(), 1);
                    let payload = payloads.first().unwrap();
                    assert_eq!(payload.sequence, i);
                    assert_eq!(payload.payload, serde_json::json!({ "msg": "Hello, World!" }));
                }
                _ => {
                    log::error!("invalid packet at sequence {i}");
                    assert!(false);
                }
            }
        }
    }

    #[tokio::test]
    /// Start serializer, write data to it, do shutdown, start another serializer, read data
    async fn shutdown_and_reload() {
        let max_file_size = 10000;
        let max_file_count = 10;
        let number_of_samples = 350;
        let temp_dir = PathBuf::from("/tmp/uplink/persistence");
        let _ = std::fs::remove_dir_all(temp_dir.as_path());
        let sk = Arc::new(StreamConfig {
            name: "test_stream".to_string(),
            topic: "/tenants/demo/devices/1/events/test_stream/jsonarray".to_string(),
            batch_size: 2,
            flush_period: Duration::from_secs(1),
            compression: Compression::Disabled,
            persistence: Persistence {
                max_file_size,
                max_file_count,
            },
            priority: 0,
        });
        let config = Config {
            persistence_path: temp_dir,
            streams: hashmap!(
                "test_stream".to_owned() => sk.as_ref().clone()
            ),
            mqtt: MqttConfig {
                max_packet_size: 25600,
                max_inflight: 10,
                keep_alive: 30,
                network_timeout: 30,
            },
            ..Default::default()
        };
        let config = Arc::new(config);

        let (serializer, data_tx, _net_rx, ctrl_tx) = defaults(config.clone());

        let first_serializer = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .thread_name("mock serializer")
                .build()
                .unwrap()
                .block_on(serializer.start());
        });

        for i in 0..number_of_samples {
            data_tx.send(Box::new(create_test_buffer(i, sk.clone()))).unwrap();
        }

        ctrl_tx.send(()).unwrap();

        first_serializer.join().unwrap();

        let (serializer, _data_tx, net_rx, _ctrl_tx) = defaults(config);

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .thread_name("mock serializer")
                .build()
                .unwrap()
                .block_on(serializer.start());
        });

        // Packets can come out of order
        // so we compare this sum to the expected sum
        let mut sum = 0;
        for i in 0..number_of_samples {
            match net_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(Request::Publish(publish)) => {
                    let payloads = serde_json::from_slice::<Vec<SerializedPayload>>(publish.payload.as_ref()).unwrap();
                    assert_eq!(payloads.len(), 1);
                    let payload = payloads.first().unwrap();
                    sum += payload.sequence;
                    assert_eq!(payload.payload, serde_json::json!({ "msg": "Hello, World!" }));
                }
                Err(_) => {}
                e => {
                    println!("invalid packet at sequence {i} : {e:?}");
                    assert!(false);
                }
            }
        }

        // mitochondria is the powerhouse of cell
        let expected_sum = number_of_samples * (number_of_samples - 1) / 2;
        assert_eq!(sum, expected_sum);
    }

    #[tokio::test]
    // Force runs serializer in disk mode, with network returning
    async fn disk_to_catchup() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, net_rx, _) = defaults(config);

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
        let status = serializer.slow(publish, Arc::new(Default::default())).await;

        assert_eq!(status, Status::EventLoopReady);
    }

    #[tokio::test]
    // Force runs serializer in disk mode, with crashed network
    async fn disk_to_crash() {
        let config = Arc::new(default_config());
        let (mut serializer, data_tx, _, _) = defaults(config);

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config.clone(), data_tx);
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
        {
            Status::EventLoopCrash => {
                let mut last_packet = None;
                while let Some((publish, _)) = serializer.fetch_next_packet_from_storage() {
                    last_packet = Some(publish);
                }
                assert!(last_packet.is_some());
                let Publish { topic, payload, ..} = last_packet.unwrap();
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

        let (mut serializer, _, _, _) = defaults(config);

        let status = serializer.catchup().await;
        assert_eq!(status, Status::Normal);
    }

    #[tokio::test]
    // Force runs serializer in catchup mode, with data already in persistence
    async fn catchup_to_normal_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, net_rx, _) = defaults(config);

        let (stream_name, stream_config) = (
            "hello",
            StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
        );
        let mut collector = MockCollector::new(stream_name, stream_config.clone(), data_tx);
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
        serializer.write_publish_to_storage(Arc::new(stream_config), publish);

        let status = serializer.catchup().await;
        assert_eq!(status, Status::Normal);
    }

    #[tokio::test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    async fn catchup_to_crash_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, _data_tx, _, _) = defaults(config);
        let sk = Arc::new(StreamConfig {
            topic: "test/topic".to_string(),
            ..Default::default()
        });

        serializer.write_publish_to_storage(sk.clone(), create_publish("test/topic", 0));
        serializer.write_publish_to_storage(sk.clone(), create_publish("test/topic", 1));
        serializer.write_publish_to_storage(sk.clone(), create_publish("test/topic", 2));

        match serializer.catchup().await {
            Status::EventLoopCrash => {
                let mut last_packet = None;
                while let Some((publish, _)) = serializer.fetch_next_packet_from_storage() {
                    last_packet = Some(publish);
                }
                assert!(last_packet.is_some());
                let Publish { topic, payload, .. } = last_packet.unwrap();
                assert_eq!(topic, "test/topic");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "0");
            }
            s => unreachable!("Unexpected status: {:?}", s),
        }
    }

    #[tokio::test]
    /// Checks that data is sent to mqtt module:
    /// * Based on the stream priority
    /// * In the same order in which it's inserted for data in same stream
    async fn preferential_send_on_network() {
        let mut config = default_config();
        config.stream_metrics.timeout = Duration::from_secs(1000);
        config.streams.extend([
            (
                "two".to_owned(),
                StreamConfig { topic: "topic/two".to_string(), priority: 2, ..Default::default() },
            ),
            (
                "one".to_owned(),
                StreamConfig { topic: "topic/one".to_string(), priority: 1, ..Default::default() },
            ),
            (
                "top".to_owned(),
                StreamConfig { topic: "topic/top".to_string(), priority: u8::MAX, ..Default::default() },
            ),
        ]);
        let config = Arc::new(config);
        let config_stream_one = Arc::new(StreamConfig { topic: "topic/one".to_string(), priority: 1, ..Default::default() });
        let config_stream_two = Arc::new(StreamConfig { topic: "topic/two".to_string(), priority: 2, ..Default::default() });
        let config_stream_three = Arc::new(StreamConfig {
            topic: "topic/top".to_string(),
            priority: u8::MAX,
            ..Default::default()
        });

        let (mut serializer, _data_tx, mqtt_rx, _) = defaults(config.clone());

        serializer.write_publish_to_storage(config_stream_one.clone(), create_publish("topic/one", 0));
        serializer.write_publish_to_storage(config_stream_one.clone(), create_publish("topic/one", 1));
        serializer.write_publish_to_storage(config_stream_three.clone(), create_publish("topic/three", 0));
        serializer.write_publish_to_storage(config_stream_three.clone(), create_publish("topic/three", 1));
        serializer.write_publish_to_storage(config_stream_two.clone(), create_publish("topic/two", 0));
        serializer.write_publish_to_storage(config_stream_two.clone(), create_publish("topic/two", 1));

        spawn_named_thread("Serializer", move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async move {
                serializer.start().await;
            })
        });

        let Request::Publish(Publish { topic, payload, .. }) = mqtt_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/three");
        assert_eq!(payload, "0");

        let Request::Publish(Publish { topic, payload, .. }) = mqtt_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/three");
        assert_eq!(payload, "1");

        let Request::Publish(Publish { topic, payload, .. }) = mqtt_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/two");
        assert_eq!(payload, "0");

        let Request::Publish(Publish { topic, payload, .. }) = mqtt_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/two");
        assert_eq!(payload, "1");

        let Request::Publish(Publish { topic, payload, .. }) = mqtt_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/one");
        assert_eq!(payload, "0");

        let Request::Publish(Publish { topic, payload, .. }) = mqtt_rx.recv_async().await.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(topic, "topic/one");
        assert_eq!(payload, "1");
    }

    #[tokio::test]
    async fn crash_test() {
        let mut config = default_config();
        config.stream_metrics.timeout = Duration::from_secs(1000);
        config.streams.insert("test_stream".to_owned(), create_test_stream_config());
        let config = Arc::new(config);
        let sk = Arc::new(create_test_stream_config());
        let (serializer, data_tx, mqtt_rx, _ctrl_tx) = defaults(config.clone());
        let st = spawn_named_thread("Serializer", move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async move {
                serializer.start().await;
            });
        });

        std::thread::sleep(Duration::from_secs(1));

        data_tx.send(Box::new(create_test_buffer(0, sk.clone()))).unwrap();
        data_tx.send(Box::new(create_test_buffer(1, sk.clone()))).unwrap();

        drop(mqtt_rx);
        drop(data_tx);
        st.join().unwrap();

        let (serializer, _data_tx, mqtt_rx, ctrl_tx) = defaults(config.clone());
        let st = spawn_named_thread("Serializer", move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async move {
                serializer.start().await;
            });
        });

        match mqtt_rx.recv().unwrap() {
            Request::Publish(p) => {
                assert_eq!(p.topic, sk.topic);
                let payload = serde_json::from_slice::<[SerializedPayload; 1]>(p.payload.as_ref()).unwrap();
                assert_eq!(payload[0].sequence, 1);
            }
            _ => {
                panic!("boo");
            }
        }
        match mqtt_rx.recv().unwrap() {
            Request::Publish(p) => {
                assert_eq!(p.topic, sk.topic);
                let payload = serde_json::from_slice::<[SerializedPayload; 1]>(p.payload.as_ref()).unwrap();
                assert_eq!(payload[0].sequence, 0);
            }
            _ => {
                panic!("boo");
            }
        }

        ctrl_tx.send(()).unwrap();
        st.join().unwrap();
    }

    fn create_publish(topic: &str, i: u32) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.to_owned(),
            pkid: 1,
            payload: Bytes::from(i.to_string()),
        }
    }

    pub fn spawn_named_thread<F>(name: &str, f: F) -> JoinHandle<()>
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::Builder::new().name(name.to_string()).spawn(f).unwrap()
    }
}
