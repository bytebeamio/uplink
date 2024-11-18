mod metrics;
pub(crate) mod storage;

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use bytes::Bytes;
use flume::{Receiver, Sender};
use lz4_flex::frame::FrameEncoder;
use pretty_bytes::converter::convert;
use rumqttc::*;
use tokio::{select, time::interval};

use crate::config::{Compression, StreamConfig};
use crate::{Config, Package};
pub use metrics::{Metrics, SerializerMetrics, StreamMetrics};
use crate::base::clock;
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
    config: Arc<Config>,
    tenant_filter: String,
    collector_rx: Receiver<Box<dyn Package>>,
    client: C,
    metrics_tx: Sender<SerializerMetrics>,
    /// Serializer metrics
    metrics: Metrics,
    /// Updated in `construct_publish` function
    /// Read and reset in `send_stream_metrics`
    stream_metrics: HashMap<String, StreamMetrics>,
    sorted_storages: BTreeMap<Arc<StreamConfig>, Box<dyn storage::Storage>>,
    ctrl_rx: Receiver<()>,
}

impl<C: MqttClient> Serializer<C> {
    /// Construct the uplink Serializer with the necessary configuration details, a receiver handle to accept data payloads from,
    /// the handle to an MQTT client(This is constructed as such for testing purposes) and a handle to update serailizer metrics.
    pub fn new(
        config: Arc<Config>,
        tenant_filter: String,
        collector_rx: Receiver<Box<dyn Package>>,
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
            sorted_storages: BTreeMap::new(),
            ctrl_rx,
        };
        result.initialize_storages();
        result
    }

    fn initialize_storages(&mut self) {
        self.sorted_storages.insert(Arc::new(self.config.action_status.clone()), self.create_storage_for_stream(&self.config.action_status));
        for stream_config in self.config.streams.values() {
            self.sorted_storages.insert(Arc::new(stream_config.clone()), self.create_storage_for_stream(&stream_config));
        }
    }

    fn create_storage_for_stream(&self, config: &StreamConfig) -> Box<dyn storage::Storage> {
        if config.persistence.max_file_count == 0 {
            Box::new(storage::InMemoryStorage::new(config.name.as_str(), config.persistence.max_file_size, self.config.mqtt.max_packet_size))
        } else {
            match storage::DirectoryStorage::new(
                self.config.persistence_path.join(config.name.as_str()),
                config.persistence.max_file_size, config.persistence.max_file_count,
                self.config.mqtt.max_packet_size,
            ) {
                Ok(s) => Box::new(s),
                Err(e) => {
                    log::error!("Failed to initialize disk backed storage for {} : {e}, falling back to in memory persistence", config.name);
                    Box::new(storage::InMemoryStorage::new(config.name.as_str(), config.persistence.max_file_size, self.config.mqtt.max_packet_size))
                }
            }
        }
    }

    /// Returns None if nothing is left (time to move to normal mode)
    fn fetch_next_packet_from_storage(&mut self) -> Option<(Publish, Arc<StreamConfig>)> {
        let mut cursor = BTreeCursorMut::new(&mut self.sorted_storages);
        while let Some((sk, storage)) = cursor.current.as_mut() {
            match storage.read_packet() {
                Ok(packet) => {
                    return Some((packet, sk.clone()));
                }
                Err(storage::StorageReadError::Empty) => {
                    cursor.bump();
                }
                Err(storage::StorageReadError::FileSystemError(e)) => {
                    log::error!("Encountered file system error when reading packet for stream({}): {e}, falling back to in memory persistence", storage.name());
                    **storage = storage.to_in_memory()
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

    fn write_package_to_storage(&mut self, data: Box<dyn Package>) {
        let stream_config = data.stream_config();
        let mut publish = construct_publish(data, &mut self.stream_metrics);
        publish.pkid = 1;
        self.write_publish_to_storage(stream_config, publish);
    }

    fn write_publish_to_storage(&mut self, sk: Arc<StreamConfig>, publish: Publish) {
        if ! self.sorted_storages.contains_key(&sk) {
            self.sorted_storages.insert(sk.clone(), self.create_storage_for_stream(&sk));
        }

        match self.sorted_storages.get_mut(&sk).unwrap().write_packet(publish) {
            Ok(_) => {}
            Err(storage::StorageWriteError::FileSystemError(e)) => {
                log::error!("Encountered file system error when reading packet for stream({}): {e}, falling back to in memory persistence", sk.name);
                let old_value = self.sorted_storages.remove(&sk).unwrap();
                self.sorted_storages.insert(sk.clone(), old_value.to_in_memory());
            }
            Err(storage::StorageWriteError::InvalidPacket(e)) => {
                log::error!("Found invalid packet when writing to storage for stream({}): {e}", sk.name);
            }
        };
    }

    fn flush_storage(&mut self) {
        for (_, storage) in self.sorted_storages.iter_mut() {
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
        for storage in self.sorted_storages.values() {
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
            log::info!(
                "{:>17}: serialized_data_size = {} compressed_data_size = {} avg_serialization_time = {}us avg_compression_time = {}us",
                metrics.stream,
                convert(metrics.serialized_data_size as f64),
                convert(metrics.compressed_data_size as f64),
                metrics.avg_serialization_time.as_micros(),
                metrics.avg_compression_time.as_micros()
            );
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
    async fn slow(&mut self, publish: Publish, stream: Arc<StreamConfig>) -> Status {
        let mut interval = interval(METRICS_INTERVAL);
        // Reactlabs setup processes logs generated by uplink
        log::info!("Switching to slow eventloop mode!!");
        self.metrics.mode = "slow";

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let payload = Bytes::copy_from_slice(&publish.payload[..]);
        let publish = send_publish(self.client.clone(), publish.topic, payload);
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

        let (publish, mut last_publish_stream) = if let Some(publish) = self.fetch_next_packet_from_storage() {
            publish
        } else {
            return Status::Normal;
        };

        let mut last_publish_sent_size = publish.payload.len();
        let send = send_publish(self.client.clone(), publish.topic, publish.payload);
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
                            if publish.topic.starts_with(&self.tenant_filter) {
                                self.metrics.sent_size += last_publish_sent_size;
                                last_publish_stream = stream;
                                last_publish_sent_size = publish.payload.len();
                                send.set(send_publish(self.client.clone(), publish.topic, publish.payload));
                            } else {
                                log::warn!("found data for wrong tenant in persistence!!");
                            }
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
                    let stream = data.stream_config();
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

async fn send_publish<C: MqttClient>(
    client: C,
    topic: String,
    payload: Bytes,
) -> Result<(), MqttError> {
    log::debug!("publishing on {topic} with size = {}", payload.len());
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

// Constructs a [Publish] packet given a [Package] element. Updates stream metrics as necessary.
fn construct_publish(
    data: Box<dyn Package>,
    stream_metrics: &mut HashMap<String, StreamMetrics>,
) -> Publish {
    let stream_name = data.stream_name().as_ref().to_owned();
    let stream_config = data.stream_config();
    let point_count = data.len();
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
    use flume::bounded;
    use serde_json::Value;
    use tokio::{spawn, time::sleep};

    use crate::{config::MqttConfig, mock::{MockClient, MockCollector}, spawn_named_thread};
    use crate::base::bridge::stream::Buffer;
    use super::*;

    fn read_from_storage<T>(storage: &mut T, max_packet_size: usize) -> Publish {
        // if storage.reload_on_eof().unwrap() {
        //     panic!("No publishes found in storage");
        // }
        //
        // match Packet::read(storage.reader(), max_packet_size) {
        //     Ok(Packet::Publish(publish)) => return publish,
        //     v => {
        //         panic!("Failed to read publish from storage. read: {:?}", v);
        //     }
        // }
        Publish::new("my/topic", QoS::AtLeastOnce, "test payload")
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
        let (_ctrl_tx, ctrl_rx) = bounded(1);
        let client = MockClient { net_tx };

        (Serializer::new(config, String::new(), data_rx, client, metrics_tx, ctrl_rx), data_tx, net_rx)
    }

    // #[tokio::test]
    // // Force runs serializer in normal mode, without persistence
    // async fn normal_to_slow() {
    //     let config = default_config();
    //     let (mut serializer, data_tx, net_rx) = defaults(Arc::new(config));
    //
    //     // Slow Network, takes packets only once in 10s
    //     spawn(async move {
    //         loop {
    //             sleep(Duration::from_secs(10)).await;
    //             net_rx.recv_async().await.unwrap();
    //         }
    //     });
    //
    //     let (stream_name, stream_config) = (
    //         "hello",
    //         StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
    //     );
    //     let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
    //     spawn(async move {
    //         for i in 1..3 {
    //             collector.send(i).await.unwrap();
    //         }
    //     });
    //
    //     match serializer.normal().await {
    //         Status::SlowEventloop(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }, _) => {
    //             assert_eq!(topic, "hello/world");
    //             let recvd: Value = serde_json::from_slice(&payload).unwrap();
    //             let obj = &recvd.as_array().unwrap()[0];
    //             assert_eq!(obj.get("msg"), Some(&Value::from("Hello, World!")));
    //         }
    //         s => panic!("Unexpected status: {:?}", s),
    //     }
    // }
    //
    // #[test]
    // // Force write publish to storage and verify by reading back
    // fn read_write_storage() {
    //     let config = Arc::new(default_config());
    //
    //     let (serializer, _, _) = defaults(config);
    //     let mut storage = Storage::new("hello/world", 1024);
    //
    //     let mut publish = Publish::new(
    //         "hello/world",
    //         QoS::AtLeastOnce,
    //         "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
    //     );
    //     write_to_storage(publish.clone(), &mut storage).unwrap();
    //
    //     let stored_publish =
    //         read_from_storage(&mut storage, serializer.config.mqtt.max_packet_size);
    //
    //     // Ensure publish.pkid is 1, as written to disk
    //     publish.pkid = 1;
    //     assert_eq!(publish, stored_publish);
    // }
    //
    // #[tokio::test]
    // // Force runs serializer in disk mode, with network returning
    // async fn disk_to_catchup() {
    //     let config = Arc::new(default_config());
    //
    //     let (mut serializer, data_tx, net_rx) = defaults(config);
    //
    //     // Slow Network, takes packets only once in 10s
    //     spawn(async move {
    //         loop {
    //             sleep(Duration::from_secs(5)).await;
    //             net_rx.recv_async().await.unwrap();
    //         }
    //     });
    //
    //     let (stream_name, stream_config) = (
    //         "hello",
    //         StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
    //     );
    //     let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
    //     // Faster collector, send data every 5s
    //     spawn(async move {
    //         for i in 1..10 {
    //             collector.send(i).await.unwrap();
    //             sleep(Duration::from_secs(3)).await;
    //         }
    //     });
    //
    //     let publish = Publish::new(
    //         "hello/world",
    //         QoS::AtLeastOnce,
    //         "[{{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}}]".as_bytes(),
    //     );
    //     let status = serializer.slow(publish, Arc::new(Default::default())).await.unwrap();
    //
    //     assert_eq!(status, Status::EventLoopReady);
    // }
    //
    // #[tokio::test]
    // // Force runs serializer in disk mode, with crashed network
    // async fn disk_to_crash() {
    //     let config = Arc::new(default_config());
    //     let (mut serializer, data_tx, _) = defaults(config);
    //
    //     let (stream_name, stream_config) = (
    //         "hello",
    //         StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
    //     );
    //     let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
    //     // Faster collector, send data every 5s
    //     spawn(async move {
    //         for i in 1..10 {
    //             collector.send(i).await.unwrap();
    //             sleep(Duration::from_secs(3)).await;
    //         }
    //     });
    //
    //     let publish = Publish::new(
    //         "hello/world",
    //         QoS::AtLeastOnce,
    //         "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
    //     );
    //
    //     match serializer
    //         .slow(
    //             publish,
    //             Arc::new(StreamConfig { topic: "hello/world".to_string(), ..Default::default() }),
    //         )
    //         .await
    //         .unwrap()
    //     {
    //         Status::EventLoopCrash(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }, _) => {
    //             assert_eq!(topic, "hello/world");
    //             let recvd = std::str::from_utf8(&payload).unwrap();
    //             assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
    //         }
    //         s => panic!("Unexpected status: {:?}", s),
    //     }
    // }
    //
    // #[tokio::test]
    // // Force runs serializer in catchup mode, with empty persistence
    // async fn catchup_to_normal_empty_persistence() {
    //     let config = Arc::new(default_config());
    //
    //     let (mut serializer, _, _) = defaults(config);
    //
    //     let status = serializer.catchup().await.unwrap();
    //     assert_eq!(status, Status::Normal);
    // }
    //
    // #[tokio::test]
    // // Force runs serializer in catchup mode, with data already in persistence
    // async fn catchup_to_normal_with_persistence() {
    //     let config = Arc::new(default_config());
    //
    //     let (mut serializer, data_tx, net_rx) = defaults(config);
    //
    //     let mut storage = serializer
    //         .storage_handler
    //         .map
    //         .entry(Arc::new(Default::default()))
    //         .or_insert(Storage::new("hello/world", 1024));
    //
    //     let (stream_name, stream_config) = (
    //         "hello",
    //         StreamConfig { topic: "hello/world".to_string(), batch_size: 1, ..Default::default() },
    //     );
    //     let mut collector = MockCollector::new(stream_name, stream_config, data_tx);
    //     // Run a collector practically once
    //     spawn(async move {
    //         for i in 2..6 {
    //             collector.send(i).await.unwrap();
    //             sleep(Duration::from_secs(100)).await;
    //         }
    //     });
    //
    //     // Decent network that lets collector push data once into storage
    //     spawn(async move {
    //         sleep(Duration::from_secs(5)).await;
    //         for i in 1..6 {
    //             match net_rx.recv_async().await.unwrap() {
    //                 Request::Publish(Publish { payload, .. }) => {
    //                     let recvd = String::from_utf8(payload.to_vec()).unwrap();
    //                     let expected = format!(
    //                         "[{{\"sequence\":{i},\"timestamp\":0,\"msg\":\"Hello, World!\"}}]",
    //                     );
    //                     assert_eq!(recvd, expected)
    //                 }
    //                 r => unreachable!("Unexpected request: {:?}", r),
    //             }
    //         }
    //     });
    //
    //     // Force write a publish into storage
    //     let publish = Publish::new(
    //         "hello/world",
    //         QoS::AtLeastOnce,
    //         "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
    //     );
    //     write_to_storage(publish.clone(), &mut storage).unwrap();
    //
    //     let status = serializer.catchup().await.unwrap();
    //     assert_eq!(status, Status::Normal);
    // }

    #[tokio::test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    async fn catchup_to_crash_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, _) = defaults(config);
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

        let (mut serializer, _data_tx, mqtt_rx) = defaults(config.clone());

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
}
