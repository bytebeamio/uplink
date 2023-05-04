mod metrics;

pub use metrics::SerializerMetrics;

use std::sync::Arc;
use std::time::Duration;
use std::{collections::VecDeque, io};

use bytes::Bytes;
use flume::{Receiver, RecvError, Sender};
use log::{debug, error, info, trace};
use rumqttc::*;
use storage::Storage;
use thiserror::Error;
use tokio::{select, time};

use crate::{Config, Package};

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
    storage: Storage,
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
        let mut storage = Storage::new(config.persistence.max_file_size);
        if let Some(persistence) = &config.persistence.disk {
            storage.set_persistence(&persistence.path, persistence.max_file_count)?;
        }

        Ok(Serializer {
            config,
            collector_rx,
            client,
            storage,
            metrics: SerializerMetrics::new("catchup"),
            metrics_tx,
            pending_metrics: VecDeque::with_capacity(3),
        })
    }

    /// Write all data received, from here-on, to disk only.
    async fn crash(&mut self, publish: Publish) -> Result<Status, Error> {
        // Write failed publish to disk first, metrics don't matter
        match write_to_disk(publish, &mut self.storage) {
            Ok(Some(deleted)) => debug!("Lost segment = {deleted}"),
            Ok(_) => {}
            Err(e) => error!("Crash loop: write error = {:?}", e),
        }

        loop {
            // Collect next data packet and write to disk
            let data = self.collector_rx.recv_async().await?;
            let publish = construct_publish(data)?;
            match write_to_disk(publish, &mut self.storage) {
                Ok(Some(deleted)) => debug!("Lost segment = {deleted}"),
                Ok(_) => {}
                Err(e) => error!("Crash loop: write error = {:?}", e),
            }
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    // TODO: Handle errors. Don't return errors
    async fn slow(&mut self, publish: Publish) -> Result<Status, Error> {
        let mut interval = time::interval(METRICS_INTERVAL);
        // Reactlabs setup processes logs generated by uplink
        info!("Switching to slow eventloop mode!!");
        self.metrics.set_mode("slow");

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let payload = &publish.payload[..];
        let publish = self.client.publish(&publish.topic, QoS::AtLeastOnce, false, payload);
        tokio::pin!(publish);

        let v: Result<Status, Error> = loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;
                    let publish = construct_publish(data)?;
                    match write_to_disk(publish, &mut self.storage) {
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
                    check_metrics(&mut self.metrics, &self.storage);
                }
            }
        };

        save_and_prepare_next_metrics(&mut self.pending_metrics, &mut self.metrics, &self.storage);
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
        let mut interval = time::interval(METRICS_INTERVAL);
        self.metrics.set_mode("catchup");

        let max_packet_size = self.config.mqtt.max_packet_size;
        let client = self.client.clone();

        loop {
            match self.storage.reload_on_eof() {
                // Done reading all the pending files
                Ok(true) => return Ok(Status::Normal),
                Ok(false) => break,
                // Reload again on encountering a corrupted file
                Err(e) => {
                    self.metrics.increment_errors();
                    self.metrics.increment_lost_segments();
                    error!("Failed to reload from storage. Error = {e}");
                    continue;
                }
            }
        }

        // TODO(RT): This can fail when packet sizes > max_payload_size in config are written to disk.
        // This leads to force switching to normal mode. Increasing max_payload_size to bypass this
        let publish = match read(self.storage.reader(), max_packet_size) {
            Ok(Packet::Publish(publish)) => publish,
            Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
            Err(e) => {
                self.metrics.increment_errors();
                error!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                save_and_prepare_next_metrics(
                    &mut self.pending_metrics,
                    &mut self.metrics,
                    &self.storage,
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
                    match write_to_disk(publish, &mut self.storage) {
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

                    loop {
                        match self.storage.reload_on_eof() {
                            // Done reading all the pending files
                            Ok(true) => return Ok(Status::Normal),
                            Ok(false) => break,
                            // Reload again on encountering a corrupted file
                            Err(e) => {
                                self.metrics.increment_errors();
                                self.metrics.increment_lost_segments();
                                error!("Failed to reload from storage. Error = {e}");
                                continue
                            }
                        }
                    }

                    let publish = match read(self.storage.reader(), max_packet_size) {
                        Ok(Packet::Publish(publish)) => publish,
                        Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
                        Err(e) => {
                            error!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                            break Ok(Status::Normal)
                        }
                    };

                    self.metrics.add_batch();
                    self.metrics.set_write_memory(self.storage.inmemory_read_size());
                    self.metrics.set_disk_files(self.storage.file_count());

                    let payload = publish.payload;
                    last_publish_payload_size = payload.len();
                    send.set(send_publish(client, publish.topic, payload));
                }
                // On a regular interval, forwards metrics information to network
                _ = interval.tick() => {
                    // TODO(RT): Move storage updates into `check_and_flush`
                    self.metrics.set_write_memory(self.storage.inmemory_write_size());
                    self.metrics.set_read_memory(self.storage.inmemory_read_size());
                    self.metrics.set_disk_files(self.storage.file_count());
                    let _ = check_and_flush_metrics(&mut self.pending_metrics, &mut self.metrics, &self.metrics_tx);
                }
            }
        };

        save_and_prepare_next_metrics(&mut self.pending_metrics, &mut self.metrics, &self.storage);
        let v = v?;
        Ok(v)
    }

    async fn normal(&mut self) -> Result<Status, Error> {
        let mut interval = time::interval(METRICS_INTERVAL);
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
                        self.metrics.set_write_memory(self.storage.inmemory_write_size());
                        self.metrics.set_read_memory(self.storage.inmemory_read_size());
                        self.metrics.set_disk_files(self.storage.file_count());


                    if let Err(e) = check_and_flush_metrics(&mut self.pending_metrics, &mut self.metrics, &self.metrics_tx) {
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

// Constructs a [Publish] packet given a [Package] element. Updates stream metrics as necessary.
fn construct_publish(data: Box<dyn Package>) -> Result<Publish, Error> {
    let stream = data.stream().as_ref().to_owned();
    let point_count = data.len();
    let batch_latency = data.latency();
    trace!("Data received on stream: {stream}; message count = {point_count}; batching latency = {batch_latency}");

    let topic = data.topic();
    let payload = data.serialize()?;

    Ok(Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload))
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

fn check_metrics(metrics: &mut SerializerMetrics, storage: &Storage) {
    use pretty_bytes::converter::convert;

    metrics.set_write_memory(storage.inmemory_write_size());
    metrics.set_read_memory(storage.inmemory_read_size());
    metrics.set_disk_files(storage.file_count());

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
    storage: &Storage,
) {
    metrics.set_write_memory(storage.inmemory_write_size());
    metrics.set_read_memory(storage.inmemory_read_size());
    metrics.set_disk_files(storage.file_count());

    let m = metrics.clone();
    pending.push_back(m);
    metrics.prepare_next();
}

// Enable actual metrics timers when there is data. This method is called every minute by the bridge
fn check_and_flush_metrics(
    pending: &mut VecDeque<SerializerMetrics>,
    metrics: &mut SerializerMetrics,
    metrics_tx: &Sender<SerializerMetrics>,
) -> Result<(), flume::TrySendError<SerializerMetrics>> {
    use pretty_bytes::converter::convert;

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
    use tempdir::TempDir;

    use std::collections::HashMap;

    use super::*;
    use crate::base::bridge::stream::Stream;
    use crate::base::{Disk, MqttConfig};
    use crate::{config::Persistence, Payload};

    fn init_backup_folders(suffix: &str) -> TempDir {
        let path = format!("/tmp/uplink_test/{suffix}");
        let backup = TempDir::new(&path).unwrap();

        if !backup.path().is_dir() {
            panic!("Folder does not exist");
        }

        backup
    }

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

    fn config_with_persistence(path: String) -> Arc<Config> {
        std::fs::create_dir_all(&path).unwrap();
        let mut config = default_config();
        config.persistence = Persistence {
            max_file_size: 10 * 1024 * 1024,
            disk: Some(Disk { path: path.clone(), max_file_count: 3 }),
        };

        Arc::new(config)
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
            MockCollector { stream: Stream::new("hello", "hello/world", 1, data_tx) }
        }

        fn send(&mut self, i: u32) -> Result<(), Error> {
            let payload = Payload {
                stream: "hello".to_owned(),
                device_id: None,
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
            std::thread::sleep(time::Duration::from_secs(10));
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
        let backup_dir = init_backup_folders("disk");
        let path = format!("{}", backup_dir.path().display());
        let config = config_with_persistence(path);
        std::fs::create_dir_all(&config.persistence.disk.as_ref().unwrap().path).unwrap();

        let (serializer, _, _) = defaults(config);
        let mut storage = serializer.storage;

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
        let backup_dir = init_backup_folders("disk_catchup");
        let path = format!("{}", backup_dir.path().display());
        let config = config_with_persistence(path);

        let (mut serializer, data_tx, net_rx) = defaults(config);

        // Slow Network, takes packets only once in 10s
        std::thread::spawn(move || loop {
            std::thread::sleep(time::Duration::from_secs(5));
            net_rx.recv().unwrap();
        });

        let mut collector = MockCollector::new(data_tx);
        // Faster collector, send data every 5s
        std::thread::spawn(move || {
            for i in 1..10 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(3));
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
        let backup_dir = init_backup_folders("disk_crash");
        let path = format!("{}", backup_dir.path().display());
        let config = config_with_persistence(path);
        let (mut serializer, data_tx, _) = defaults(config);

        let mut collector = MockCollector::new(data_tx);
        // Faster collector, send data every 5s
        std::thread::spawn(move || {
            for i in 1..10 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(3));
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
        let backup_dir = init_backup_folders("catchup_empty");
        let path = format!("{}", backup_dir.path().display());
        let config = config_with_persistence(path);

        let (mut serializer, _, _) = defaults(config);

        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with data already in persistence
    fn catchup_to_normal_with_persistence() {
        let backup_dir = init_backup_folders("catchup_normal");
        let path = format!("{}", backup_dir.path().display());
        let config = config_with_persistence(path);

        let (mut serializer, data_tx, net_rx) = defaults(config);
        let mut storage = serializer.storage;

        let mut collector = MockCollector::new(data_tx);
        // Run a collector practically once
        std::thread::spawn(move || {
            for i in 2..6 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(100));
            }
        });

        // Decent network that lets collector push data once into storage
        std::thread::spawn(move || {
            std::thread::sleep(time::Duration::from_secs(5));
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

        // Replace storage into serializer
        serializer.storage = storage;
        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    fn catchup_to_crash_with_persistence() {
        let backup_dir = init_backup_folders("catchup_crash");
        let path = format!("{}", backup_dir.path().display());
        let config = config_with_persistence(path);

        std::fs::create_dir_all(&config.persistence.disk.as_ref().unwrap().path).unwrap();

        let (mut serializer, data_tx, _) = defaults(config);
        let mut storage = serializer.storage;

        let mut collector = MockCollector::new(data_tx);
        // Run a collector
        std::thread::spawn(move || {
            for i in 2..6 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(10));
            }
        });

        // Force write a publish into storage
        let publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        write_to_disk(publish.clone(), &mut storage).unwrap();

        // Replace storage into serializer
        serializer.storage = storage;
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
