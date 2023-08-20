mod metrics;

use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use flume::{Receiver, RecvError, Sender};
use log::{debug, error, info, trace};
use lz4_flex::frame::FrameEncoder;
use rumqttc::*;
use storage::Storage;
use thiserror::Error;
use tokio::{select, time::interval};

use crate::base::Compression;
use crate::{Config, Package};
pub use metrics::SerializerMetrics;

use super::default_file_size;

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
    #[error("Disk error {0}")]
    Disk(#[from] storage::Error),
    #[error("Mqtt client error {0}")]
    Client(#[from] MqttError),
    #[error("Storage is disabled/missing")]
    MissingPersistence,
    #[error("LZ4 compression error: {0}")]
    Lz4(#[from] lz4_flex::frame::Error),
    #[error("Empty storage")]
    EmptyStorage,
    #[error("Permission denied while accessing persistence directory \"{0}\"")]
    Persistence(String),
}

#[derive(Debug, PartialEq)]
enum Status {
    Normal,
    SlowEventloop(Publish),
    EventLoopReady,
    EventLoopCrash(Publish),
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
        S: Into<String>,
        V: Into<Vec<u8>>;
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

struct StorageHandler {
    map: HashMap<String, Storage>,
}

impl StorageHandler {
    fn new(config: Arc<Config>) -> Result<Self, Error> {
        let mut map = HashMap::with_capacity(2 * config.streams.len());
        for (stream_name, stream_config) in config.streams.iter() {
            let mut storage = Storage::new(stream_config.persistence.max_file_size);
            if stream_config.persistence.max_file_count > 0 {
                let mut path = config.persistence_path.clone();
                path.push(stream_name);

                std::fs::create_dir_all(&path).map_err(|_| {
                    Error::Persistence(config.persistence_path.to_string_lossy().to_string())
                })?;
                storage.set_persistence(&path, stream_config.persistence.max_file_count)?;

                debug!(
                    "Disk persistance is enabled for stream: \"{stream_name}\"; path: {}",
                    path.display()
                );
            }
            map.insert(stream_config.topic.clone(), storage);
        }

        Ok(Self { map })
    }

    fn select(&mut self, topic: &str) -> &mut Storage {
        self.map.entry(topic.to_owned()).or_insert_with(|| Storage::new(default_file_size()))
    }

    fn next(&mut self, metrics: &mut SerializerMetrics) -> Option<&mut Storage> {
        let storages = self.map.values_mut();

        for storage in storages {
            match storage.reload_on_eof() {
                // Done reading all the pending files
                Ok(true) => continue,
                Ok(false) => return Some(storage),
                // Reload again on encountering a corrupted file
                Err(e) => {
                    metrics.increment_errors();
                    metrics.increment_lost_segments();
                    error!("Failed to reload from storage. Error = {e}");
                    continue;
                }
            }
        }

        None
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
/// [`start()`]: Serializer::start
/// [`try_publish()`]: AsyncClient::try_publish
/// [`publish()`]: AsyncClient::publish
pub struct Serializer<C: MqttClient> {
    config: Arc<Config>,
    collector_rx: Receiver<Box<dyn Package>>,
    client: C,
    storage_handler: StorageHandler,
    metrics: SerializerMetrics,
    metrics_tx: Sender<SerializerMetrics>,
    pending_metrics: VecDeque<SerializerMetrics>,
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
        let storage_handler = StorageHandler::new(config.clone())?;

        Ok(Serializer {
            config,
            collector_rx,
            client,
            storage_handler,
            metrics: SerializerMetrics::new("catchup"),
            metrics_tx,
            pending_metrics: VecDeque::with_capacity(3),
        })
    }

    /// Write all data received, from here-on, to disk only.
    async fn crash(&mut self, publish: Publish) -> Result<Status, Error> {
        let storage = self.storage_handler.select(&publish.topic);
        // Write failed publish to disk first, metrics don't matter
        match write_to_disk(publish, storage) {
            Ok(Some(deleted)) => debug!("Lost segment = {deleted}"),
            Ok(_) => {}
            Err(e) => error!("Crash loop: write error = {:?}", e),
        }

        loop {
            // Collect next data packet and write to disk
            let data = self.collector_rx.recv_async().await?;
            let publish = construct_publish(data)?;
            let storage = self.storage_handler.select(&publish.topic);
            match write_to_disk(publish, storage) {
                Ok(Some(deleted)) => debug!("Lost segment = {deleted}"),
                Ok(_) => {}
                Err(e) => error!("Crash loop: write error = {:?}", e),
            }
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    // TODO: Handle errors. Don't return errors
    async fn slow(&mut self, publish: Publish) -> Result<Status, Error> {
        let mut interval = interval(METRICS_INTERVAL);
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to slow eventloop mode!!");
        self.metrics.set_mode("slow");

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let payload = Bytes::copy_from_slice(&publish.payload[..]);
        let publish = send_publish(self.client.clone(), publish.topic, payload);
        tokio::pin!(publish);

        let v: Result<Status, Error> = loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let publish = construct_publish(data)?;
                    let storage = self.storage_handler.select(&publish.topic);
                    match write_to_disk(publish, storage) {
                        Ok(Some(deleted)) => {
                            debug!("Lost segment = {deleted}");
                            self.metrics.increment_lost_segments();
                        }
                        Ok(_) => {},
                        Err(e) => {
                            error!("Storage write error = {:?}", e);
                            self.metrics.increment_errors();
                        }
                    };

                    // Update metrics
                    self.metrics.add_batch();
                }
                o = &mut publish => match o {
                    Ok(_) => {
                        break Ok(Status::EventLoopReady)
                    }
                    Err(MqttError::Send(Request::Publish(publish))) => {
                        break Ok(Status::EventLoopCrash(publish));
                    },
                    Err(e) => {
                        unreachable!("Unexpected error: {}", e);
                    }
                },
                _ = interval.tick() => {
                    check_metrics(&mut self.metrics, &self.storage_handler);
                }
            }
        };

        save_and_prepare_next_metrics(
            &mut self.pending_metrics,
            &mut self.metrics,
            &self.storage_handler,
        );
        let v = v?;
        Ok(v)
    }

    /// Write new collector data to disk while sending existing data on
    /// disk to mqtt eventloop. Collector rx is selected with blocking
    /// `publish` instead of `try publish` to ensure that transient back
    /// pressure due to a lot of data on disk doesn't switch state to
    /// `Status::SlowEventLoop`
    async fn catchup(&mut self) -> Result<Status, Error> {
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to catchup mode!!");
        let mut interval = interval(METRICS_INTERVAL);
        self.metrics.set_mode("catchup");

        let max_packet_size = self.config.mqtt.max_packet_size;
        let client = self.client.clone();

        let storage = match self.storage_handler.next(&mut self.metrics) {
            Some(s) => s,
            _ => return Ok(Status::Normal),
        };

        // TODO(RT): This can fail when packet sizes > max_payload_size in config are written to disk.
        // This leads to force switching to normal mode. Increasing max_payload_size to bypass this
        let publish = match read(storage.reader(), max_packet_size) {
            Ok(Packet::Publish(publish)) => publish,
            Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
            Err(e) => {
                self.metrics.increment_errors();
                error!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                save_and_prepare_next_metrics(
                    &mut self.pending_metrics,
                    &mut self.metrics,
                    &self.storage_handler,
                );
                return Ok(Status::Normal);
            }
        };

        let mut last_publish_payload_size = publish.payload.len();
        let send = send_publish(client, publish.topic, publish.payload);
        tokio::pin!(send);

        let v: Result<Status, Error> = loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let publish = construct_publish(data)?;
                    let storage = self.storage_handler.select(&publish.topic);
                    match write_to_disk(publish, storage) {
                        Ok(Some(deleted)) => {
                            debug!("Lost segment = {deleted}");
                            self.metrics.increment_lost_segments();
                        }
                        Ok(_) => {},
                        Err(e) => {
                            error!("Storage write error = {:?}", e);
                            self.metrics.increment_errors();
                        }
                    };

                    // Update metrics
                    self.metrics.add_batch();
                }
                o = &mut send => {
                    self.metrics.add_sent_size(last_publish_payload_size);
                    // Send failure implies eventloop crash. Switch state to
                    // indefinitely write to disk to not loose data
                    let client = match o {
                        Ok(c) => c,
                        Err(MqttError::Send(Request::Publish(publish))) => break Ok(Status::EventLoopCrash(publish)),
                        Err(e) => unreachable!("Unexpected error: {}", e),
                    };

                    let storage = match self.storage_handler.next(&mut self.metrics) {
                        Some(s) => s,
                        _ => return Ok(Status::Normal),
                    };

                    let publish = match read(storage.reader(), max_packet_size) {
                        Ok(Packet::Publish(publish)) => publish,
                        Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
                        Err(e) => {
                            error!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                            break Ok(Status::Normal)
                        }
                    };

                    self.metrics.add_batch();

                    let payload = publish.payload;
                    last_publish_payload_size = payload.len();
                    send.set(send_publish(client, publish.topic, payload));
                }
                // On a regular interval, forwards metrics information to network
                _ = interval.tick() => {
                    let _ = check_and_flush_metrics(&mut self.pending_metrics, &mut self.metrics, &self.metrics_tx, &self.storage_handler);
                }
            }
        };

        save_and_prepare_next_metrics(
            &mut self.pending_metrics,
            &mut self.metrics,
            &self.storage_handler,
        );
        let v = v?;
        Ok(v)
    }

    async fn normal(&mut self) -> Result<Status, Error> {
        let mut interval = interval(METRICS_INTERVAL);
        self.metrics.set_mode("normal");
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to normal mode!!");

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let publish = construct_publish(data)?;
                    let payload_size = publish.payload.len();
                    debug!("publishing on {} with size = {}", publish.topic, payload_size);
                    match self.client.try_publish(publish.topic, QoS::AtLeastOnce, false, publish.payload) {
                        Ok(_) => {
                            self.metrics.add_batch();
                            self.metrics.add_sent_size(payload_size);
                            continue;
                        }
                        Err(MqttError::TrySend(Request::Publish(publish))) => return Ok(Status::SlowEventloop(publish)),
                        Err(e) => unreachable!("Unexpected error: {}", e),
                    }

                }
                // On a regular interval, forwards metrics information to network
                _ = interval.tick() => {
                    // Check in storage stats every tick. TODO: Make storage object always
                    // available. It can be inmemory storage

                    if let Err(e) = check_and_flush_metrics(&mut self.pending_metrics, &mut self.metrics, &self.metrics_tx, &self.storage_handler) {
                        debug!("Failed to flush serializer metrics (normal). Error = {}", e);
                    }
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
                Status::SlowEventloop(publish) => self.slow(publish).await?,
                Status::EventLoopReady => self.catchup().await?,
                Status::EventLoopCrash(publish) => self.crash(publish).await?,
            };

            status = next_status;
        }
    }
}

async fn send_publish<C: MqttClient>(
    client: C,
    topic: String,
    payload: Bytes,
) -> Result<C, MqttError> {
    debug!("publishing on {topic} with size = {}", payload.len());
    client.publish(topic, QoS::AtLeastOnce, false, payload).await?;
    Ok(client)
}

fn lz4_compress(payload: &mut Vec<u8>) -> Result<(), Error> {
    let mut compressor = FrameEncoder::new(vec![]);
    compressor.write_all(payload)?;
    *payload = compressor.finish()?;

    Ok(())
}

// Constructs a [Publish] packet given a [Package] element. Updates stream metrics as necessary.
fn construct_publish(data: Box<dyn Package>) -> Result<Publish, Error> {
    let stream = data.stream().as_ref().to_owned();
    let point_count = data.len();
    let batch_latency = data.latency();
    trace!("Data received on stream: {stream}; message count = {point_count}; batching latency = {batch_latency}");

    let topic = data.topic().to_string();
    let mut payload = data.serialize()?;

    if let Compression::Lz4 = data.compression() {
        lz4_compress(&mut payload)?;
    }

    Ok(Publish::new(topic, QoS::AtLeastOnce, payload))
}

// Writes the provided publish packet to disk with [Storage], after setting its pkid to 1.
// Updates serializer metrics with appropriate values on success, if asked to do so.
// Returns size in memory, size in disk, number of files in disk,
fn write_to_disk(
    mut publish: Publish,
    storage: &mut Storage,
) -> Result<Option<u64>, storage::Error> {
    publish.pkid = 1;
    if let Err(e) = publish.write(storage.writer()) {
        error!("Failed to fill disk buffer. Error = {:?}", e);
        return Ok(None);
    }

    let deleted = storage.flush_on_overflow()?;
    Ok(deleted)
}

fn check_metrics(metrics: &mut SerializerMetrics, storage_handler: &StorageHandler) {
    use pretty_bytes::converter::convert;
    let mut inmemory_write_size = 0;
    let mut inmemory_read_size = 0;
    let mut file_count = 0;

    for storage in storage_handler.map.values() {
        inmemory_read_size += storage.inmemory_read_size();
        inmemory_write_size += storage.inmemory_write_size();
        file_count += storage.file_count();
    }

    metrics.set_write_memory(inmemory_write_size);
    metrics.set_read_memory(inmemory_read_size);
    metrics.set_disk_files(file_count);

    info!(
        "{:>17}: batches = {:<3} errors = {} lost = {} disk_files = {:<3} write_memory = {} read_memory = {}",
        metrics.mode,
        metrics.batches,
        metrics.errors,
        metrics.lost_segments,
        metrics.disk_files,
        convert(metrics.write_memory as f64),
        convert(metrics.read_memory as f64),
    );
}

fn save_and_prepare_next_metrics(
    pending: &mut VecDeque<SerializerMetrics>,
    metrics: &mut SerializerMetrics,
    storage_handler: &StorageHandler,
) {
    let mut inmemory_write_size = 0;
    let mut inmemory_read_size = 0;
    let mut file_count = 0;

    for storage in storage_handler.map.values() {
        inmemory_write_size += storage.inmemory_write_size();
        inmemory_read_size += storage.inmemory_read_size();
        file_count += storage.file_count();
    }

    metrics.set_write_memory(inmemory_write_size);
    metrics.set_read_memory(inmemory_read_size);
    metrics.set_disk_files(file_count);

    let m = metrics.clone();
    pending.push_back(m);
    metrics.prepare_next();
}

// // Enable actual metrics timers when there is data. This method is called every minute by the bridge
fn check_and_flush_metrics(
    pending: &mut VecDeque<SerializerMetrics>,
    metrics: &mut SerializerMetrics,
    metrics_tx: &Sender<SerializerMetrics>,
    storage_handler: &StorageHandler,
) -> Result<(), flume::TrySendError<SerializerMetrics>> {
    use pretty_bytes::converter::convert;

    let mut inmemory_write_size = 0;
    let mut inmemory_read_size = 0;
    let mut file_count = 0;

    for storage in storage_handler.map.values() {
        inmemory_write_size += storage.inmemory_write_size();
        inmemory_read_size += storage.inmemory_read_size();
        file_count += storage.file_count();
    }

    metrics.set_write_memory(inmemory_write_size);
    metrics.set_read_memory(inmemory_read_size);
    metrics.set_disk_files(file_count);

    // Send pending metrics. This signifies state change
    while let Some(metrics) = pending.get(0) {
        // Always send pending metrics. They represent state changes
        info!(
            "{:>17}: batches = {:<3} errors = {} lost = {} disk_files = {:<3} write_memory = {} read_memory = {}",
            metrics.mode,
            metrics.batches,
            metrics.errors,
            metrics.lost_segments,
            metrics.disk_files,
            convert(metrics.write_memory as f64),
            convert(metrics.read_memory as f64),
        );
        metrics_tx.try_send(metrics.clone())?;
        pending.pop_front();
    }

    if metrics.batches() > 0 {
        info!(
            "{:>17}: batches = {:<3} errors = {} lost = {} disk_files = {:<3} write_memory = {} read_memory = {}",
            metrics.mode,
            metrics.batches,
            metrics.errors,
            metrics.lost_segments,
            metrics.disk_files,
            convert(metrics.write_memory as f64),
            convert(metrics.read_memory as f64),
        );

        metrics_tx.try_send(metrics.clone())?;
        metrics.prepare_next();
    }

    Ok(())
}

// TODO(RT): Test cases
// - Restart with no internet but files on disk

#[cfg(test)]
mod test {
    use serde_json::Value;

    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::base::bridge::stream::Stream;
    use crate::base::MqttConfig;
    use crate::Payload;

    #[derive(Clone)]
    pub struct MockClient {
        pub net_tx: flume::Sender<Request>,
    }

    #[async_trait::async_trait]
    impl MqttClient for MockClient {
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
            let mut publish = Publish::new(topic, qos, payload);
            publish.retain = retain;
            let publish = Request::Publish(publish);
            self.net_tx.send_async(publish).await.map_err(|e| MqttError::Send(e.into_inner()))?;
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
            let mut publish = Publish::new(topic, qos, payload);
            publish.retain = retain;
            let publish = Request::Publish(publish);
            self.net_tx.try_send(publish).map_err(|e| MqttError::TrySend(e.into_inner()))?;
            Ok(())
        }
    }

    fn read_from_storage(storage: &mut Storage, max_packet_size: usize) -> Publish {
        if storage.reload_on_eof().unwrap() {
            panic!("No publishes found in storage");
        }

        match read(storage.reader(), max_packet_size) {
            Ok(Packet::Publish(publish)) => return publish,
            v => {
                panic!("Failed to read publish from storage. read: {:?}", v);
            }
        }
    }

    fn default_config() -> Config {
        Config {
            broker: "localhost".to_owned(),
            port: 1883,
            device_id: "123".to_owned(),
            streams: HashMap::new(),
            mqtt: MqttConfig { max_packet_size: 1024 * 1024, ..Default::default() },
            ..Default::default()
        }
    }

    fn defaults(
        config: Arc<Config>,
    ) -> (Serializer<MockClient>, flume::Sender<Box<dyn Package>>, Receiver<Request>) {
        let (data_tx, data_rx) = flume::bounded(1);
        let (net_tx, net_rx) = flume::bounded(1);
        let (metrics_tx, _metrics_rx) = flume::bounded(1);
        let client = MockClient { net_tx };

        (Serializer::new(config, data_rx, client, metrics_tx).unwrap(), data_tx, net_rx)
    }

    #[derive(Error, Debug)]
    pub enum Error {
        #[error("Serde error {0}")]
        Serde(#[from] serde_json::Error),
        #[error("Stream error {0}")]
        Base(#[from] crate::base::bridge::stream::Error),
    }

    struct MockCollector {
        stream: Stream<Payload>,
    }

    impl MockCollector {
        fn new(data_tx: flume::Sender<Box<dyn Package>>) -> MockCollector {
            MockCollector {
                stream: Stream::new("hello", "hello/world", 1, data_tx, Compression::Disabled),
            }
        }

        fn send(&mut self, i: u32) -> Result<(), Error> {
            let payload = Payload {
                stream: "hello".to_owned(),
                sequence: i,
                timestamp: 0,
                payload: serde_json::from_str("{\"msg\": \"Hello, World!\"}")?,
            };
            self.stream.push(payload)?;

            Ok(())
        }
    }

    #[test]
    // Force runs serializer in normal mode, without persistence
    fn normal_to_slow() {
        let config = default_config();
        let (mut serializer, data_tx, net_rx) = defaults(Arc::new(config));

        // Slow Network, takes packets only once in 10s
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(10));
            net_rx.recv().unwrap();
        });

        let mut collector = MockCollector::new(data_tx);
        std::thread::spawn(move || {
            for i in 1..3 {
                collector.send(i).unwrap();
            }
        });

        match tokio::runtime::Runtime::new().unwrap().block_on(serializer.normal()).unwrap() {
            Status::SlowEventloop(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }) => {
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

        let (serializer, _, _) = defaults(config);
        let mut storage = Storage::new(1024);

        let mut publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        write_to_disk(publish.clone(), &mut storage).unwrap();

        let stored_publish =
            read_from_storage(&mut storage, serializer.config.mqtt.max_packet_size);

        // Ensure publish.pkid is 1, as written to disk
        publish.pkid = 1;
        assert_eq!(publish, stored_publish);
    }

    #[test]
    // Force runs serializer in disk mode, with network returning
    fn disk_to_catchup() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, net_rx) = defaults(config);

        // Slow Network, takes packets only once in 10s
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(5));
            net_rx.recv().unwrap();
        });

        let mut collector = MockCollector::new(data_tx);
        // Faster collector, send data every 5s
        std::thread::spawn(move || {
            for i in 1..10 {
                collector.send(i).unwrap();
                std::thread::sleep(Duration::from_secs(3));
            }
        });

        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}}]".as_bytes(),
        );
        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.slow(publish)).unwrap();

        assert_eq!(status, Status::EventLoopReady);
    }

    #[test]
    // Force runs serializer in disk mode, with crashed network
    fn disk_to_crash() {
        let config = Arc::new(default_config());
        let (mut serializer, data_tx, _) = defaults(config);

        let mut collector = MockCollector::new(data_tx);
        // Faster collector, send data every 5s
        std::thread::spawn(move || {
            for i in 1..10 {
                collector.send(i).unwrap();
                std::thread::sleep(Duration::from_secs(3));
            }
        });

        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );

        match tokio::runtime::Runtime::new().unwrap().block_on(serializer.slow(publish)).unwrap() {
            Status::EventLoopCrash(Publish { qos: QoS::AtLeastOnce, topic, payload, .. }) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            s => panic!("Unexpected status: {:?}", s),
        }
    }

    #[test]
    // Force runs serializer in catchup mode, with empty persistence
    fn catchup_to_normal_empty_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, _, _) = defaults(config);

        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with data already in persistence
    fn catchup_to_normal_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, net_rx) = defaults(config);

        let mut storage = serializer
            .storage_handler
            .map
            .entry("hello/world".to_string())
            .or_insert(Storage::new(1024));

        let mut collector = MockCollector::new(data_tx);
        // Run a collector practically once
        std::thread::spawn(move || {
            for i in 2..6 {
                collector.send(i).unwrap();
                std::thread::sleep(Duration::from_secs(100));
            }
        });

        // Decent network that lets collector push data once into storage
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(5));
            for i in 1..6 {
                match net_rx.recv().unwrap() {
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
        write_to_disk(publish.clone(), &mut storage).unwrap();

        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    fn catchup_to_crash_with_persistence() {
        let config = Arc::new(default_config());

        let (mut serializer, data_tx, _) = defaults(config);

        let mut storage = serializer
            .storage_handler
            .map
            .entry("hello/world".to_string())
            .or_insert(Storage::new(1024));

        let mut collector = MockCollector::new(data_tx);
        // Run a collector
        std::thread::spawn(move || {
            for i in 2..6 {
                collector.send(i).unwrap();
                std::thread::sleep(Duration::from_secs(10));
            }
        });

        // Force write a publish into storage
        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        write_to_disk(publish.clone(), &mut storage).unwrap();

        match tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap() {
            Status::EventLoopCrash(Publish { topic, payload, .. }) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            s => unreachable!("Unexpected status: {:?}", s),
        }
    }
}
