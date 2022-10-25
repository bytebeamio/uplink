use crate::base::{Buffer, Config, Package};
use crate::{Point, Stream};

use bytes::Bytes;
use disk::Storage;
use flume::{Receiver, RecvError};
use log::{error, info};
use rumqttc::*;
use serde::Serialize;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::{select, time};

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

#[async_trait::async_trait]
pub trait MqttClient: Clone {
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
    async fn publish_bytes<S>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<(), MqttError>
    where
        S: Into<String> + Send;
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
    async fn publish_bytes<S>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<(), MqttError>
    where
        S: Into<String> + Send,
    {
        self.publish_bytes(topic, qos, retain, payload).await?;
        Ok(())
    }
}

/// The uplink Serializer is the component that deals with sending data to the Bytebeam platform.
/// In case of network issues, the Serializer enters various states depending on severeness, managed by `Serializer::start()`.                                                                                       
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
pub struct Serializer<C: MqttClient> {
    config: Arc<Config>,
    collector_rx: Receiver<Box<dyn Package>>,
    client: C,
    storage: Option<Storage>,
    metrics: Metrics,
    metrics_stream: Stream<Metrics>,
}

impl<C: MqttClient> Serializer<C> {
    pub fn new(
        config: Arc<Config>,
        collector_rx: Receiver<Box<dyn Package>>,
        metrics_stream: Stream<Metrics>,
        client: C,
    ) -> Result<Serializer<C>, Error> {
        let storage = match &config.persistence {
            Some(persistence) => {
                let storage = Storage::new(
                    &persistence.path,
                    persistence.max_file_size,
                    persistence.max_file_count,
                )?;
                Some(storage)
            }
            None => None,
        };

        Ok(Serializer {
            config,
            collector_rx,
            client,
            storage,
            metrics: Metrics::new(),
            metrics_stream,
        })
    }

    /// Write all data received, from here-on, to disk only.
    async fn crash(&mut self, mut publish: Publish) -> Result<Status, Error> {
        let storage = match &mut self.storage {
            Some(s) => s,
            None => return Err(Error::MissingPersistence),
        };
        // Write failed publish to disk first
        publish.pkid = 1;

        if let Err(e) = publish.write(storage.writer()) {
            error!("Failed to fill write buffer during bad network. Error = {:?}", e);
        }

        if let Err(e) = storage.flush_on_overflow() {
            error!("Failed to flush write buffer to disk during bad network. Error = {:?}", e);
        }

        loop {
            // Collect next data packet to write to disk
            let data = self.collector_rx.recv_async().await?;
            let topic = data.topic();
            let payload = data.serialize()?;

            let mut publish = Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload);
            publish.pkid = 1;

            if let Err(e) = publish.write(storage.writer()) {
                error!("Failed to fill write buffer during bad network. Error = {:?}", e);
                continue;
            }

            if let Err(e) = storage.flush_on_overflow() {
                error!("Failed to flush write buffer to disk during bad network. Error = {:?}", e);
                continue;
            }
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    async fn slow(&mut self, publish: Publish) -> Result<Status, Error> {
        info!("Switching to slow eventloop mode!!");

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let publish =
            self.client.publish(&publish.topic, QoS::AtLeastOnce, false, &publish.payload[..]);
        tokio::pin!(publish);

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let storage = match &mut self.storage {
                        Some(s) => s,
                        None => {
                            error!("Data loss, no disk to handle network backpressure: {:?}", data);
                            continue;
                        }
                    };

                      let data = data?;
                      if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                      }

                      let topic = data.topic();
                      let payload = data.serialize()?;
                      let payload_size = payload.len();
                      let mut publish = Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload);
                      publish.pkid = 1;

                      match publish.write(storage.writer()) {
                           Ok(_) => self.metrics.add_total_disk_size(payload_size),
                           Err(e) => {
                               error!("Failed to fill disk buffer. Error = {:?}", e);
                               continue
                           }
                      }

                      match storage.flush_on_overflow() {
                            Ok(deleted) => if deleted.is_some() {
                                self.metrics.increment_lost_segments();
                            },
                            Err(e) => {
                                error!("Failed to flush disk buffer. Error = {:?}", e);
                                continue
                            }
                      }
                }
                o = &mut publish => match o {
                    Ok(_) => return Ok(Status::EventLoopReady),
                    Err(MqttError::Send(Request::Publish(publish))) =>{
                        return Ok(Status::EventLoopCrash(publish))
                    },
                    Err(e) => unreachable!("Unexpected error: {}", e),
                }
            }
        }
    }

    /// Write new collector data to disk while sending existing data on
    /// disk to mqtt eventloop. Collector rx is selected with blocking
    /// `publish` instead of `try publish` to ensure that transient back
    /// pressure due to a lot of data on disk doesn't switch state to
    /// `Status::SlowEventLoop`
    async fn catchup(&mut self) -> Result<Status, Error> {
        let storage = match &mut self.storage {
            Some(s) => s,
            None => return Ok(Status::Normal),
        };
        info!("Switching to catchup mode!!");

        let max_packet_size = self.config.max_packet_size;
        let client = self.client.clone();

        // Done reading all the pending files
        if storage.reload_on_eof().unwrap() {
            return Ok(Status::Normal);
        }

        let publish = match read(storage.reader(), max_packet_size) {
            Ok(Packet::Publish(publish)) => publish,
            Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
            Err(e) => {
                error!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                return Ok(Status::Normal);
            }
        };

        let send = send_publish(client, publish.topic, publish.payload);
        tokio::pin!(send);

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                      let data = data?;
                      if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                      }

                      let topic = data.topic();
                      let payload = data.serialize()?;
                      let payload_size = payload.len();
                      let mut publish = Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload);
                      publish.pkid = 1;

                      match publish.write(storage.writer()) {
                           Ok(_) => self.metrics.add_total_disk_size(payload_size),
                           Err(e) => {
                               error!("Failed to fill disk buffer. Error = {:?}", e);
                               continue
                           }
                      }

                      match storage.flush_on_overflow() {
                            Ok(deleted) => if deleted.is_some() {
                                self.metrics.increment_lost_segments();
                            },
                            Err(e) => {
                                error!("Failed to flush write buffer to disk during catchup. Error = {:?}", e);
                                continue
                            }
                      }
                }
                o = &mut send => {
                    // Send failure implies eventloop crash. Switch state to
                    // indefinitely write to disk to not loose data
                    let client = match o {
                        Ok(c) => c,
                        Err(MqttError::Send(Request::Publish(publish))) => return Ok(Status::EventLoopCrash(publish)),
                        Err(e) => unreachable!("Unexpected error: {}", e),
                    };

                    match storage.reload_on_eof() {
                        // Done reading all pending files
                        Ok(true) => return Ok(Status::Normal),
                        Ok(false) => {},
                        Err(e) => {
                            error!("Failed to reload storage. Forcing into Normal mode. Error = {:?}", e);
                            return Ok(Status::Normal)
                        }
                    }

                    let publish = match read(storage.reader(), max_packet_size) {
                        Ok(Packet::Publish(publish)) => publish,
                        Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
                        Err(e) => {
                            error!("Failed to read from storage. Forcing into Normal mode. Error = {:?}", e);
                            return Ok(Status::Normal)
                        }
                    };


                    let payload = publish.payload;
                    let payload_size = payload.len();
                    self.metrics.sub_total_disk_size(payload_size);
                    self.metrics.add_total_sent_size(payload_size);
                    send.set(send_publish(client, publish.topic, payload));
                }
            }
        }
    }

    async fn normal(&mut self) -> Result<Status, Error> {
        info!("Switching to normal mode!!");
        let mut interval = time::interval(time::Duration::from_secs(10));

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;

                    // Extract anomalies detected by package during collection
                    if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                    }

                    let topic = data.topic();
                    let payload = data.serialize()?;
                    let payload_size = payload.len();
                    match self.client.try_publish(topic.as_ref(), QoS::AtLeastOnce, false, payload) {
                        Ok(_) => {
                            self.metrics.add_total_sent_size(payload_size);
                            continue;
                        }
                        Err(MqttError::TrySend(Request::Publish(publish))) => return Ok(Status::SlowEventloop(publish)),
                        Err(e) => unreachable!("Unexpected error: {}", e),
                    }

                }
                _ = interval.tick() => {
                    let metrics = self.metrics.next();
                    self.metrics_stream.fill(metrics).await.unwrap();
                }
            }
        }
    }

    /// The Serializer writes data directly to network in [normal mode] by [`try_publish()`]in on the MQTT client. In case
    /// of the network being slow, this fails and we are forced into [slow mode], where in new data is written into ['Storage']
    /// while consequently we await on a [`publish()`]. If the [`publish()`] succeeds, we move into [catchup mode] or otherwise,
    /// if it fails we move to [crash mode]. In [catchup mode], we continuously write to ['Storage'] while also pushing data
    /// onto network by [`publish()`]. If a [`publish()`] succeds, we load the next [`Publish`] packet from [`storage`], whereas
    /// if it fails, we transition into [crash mode] where we merely write all data received, directly into disk.
    ///
    /// [`try_publish()`]: AsyncClient::try_publish
    /// [`publish()`]: AsyncClient::publish
    /// [normal mode]: Serializer::normal
    /// [catchup mode]: Serializer::catchup
    /// [slow mode]: Serializer::slow
    /// [crash mode]: Serializer::crash
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
    client.publish_bytes(topic, QoS::AtLeastOnce, false, payload).await?;
    Ok(client)
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct Metrics {
    sequence: u32,
    timestamp: u64,
    total_sent_size: usize,
    total_disk_size: usize,
    lost_segments: usize,
    errors: String,
    error_count: usize,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics { errors: String::with_capacity(1024), ..Default::default() }
    }

    pub fn add_total_sent_size(&mut self, size: usize) {
        self.total_sent_size = self.total_sent_size.saturating_add(size);
    }

    pub fn add_total_disk_size(&mut self, size: usize) {
        self.total_disk_size = self.total_disk_size.saturating_add(size);
    }

    pub fn sub_total_disk_size(&mut self, size: usize) {
        self.total_disk_size = self.total_disk_size.saturating_sub(size);
    }

    pub fn increment_lost_segments(&mut self) {
        self.lost_segments += 1;
    }

    // pub fn add_error<S: Into<String>>(&mut self, error: S) {
    //     self.error_count += 1;
    //     if self.errors.len() > 1024 {
    //         return;
    //     }
    //
    //     self.errors.push_str(", ");
    //     self.errors.push_str(&error.into());
    // }

    pub fn add_errors<S: Into<String>>(&mut self, error: S, count: usize) {
        self.error_count += count;
        if self.errors.len() > 1024 {
            return;
        }

        self.errors.push_str(&error.into());
        self.errors.push_str(" | ");
    }

    pub fn next(&mut self) -> Metrics {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        self.timestamp = timestamp.as_millis() as u64;
        self.sequence += 1;

        let metrics = self.clone();

        self.errors.clear();
        self.lost_segments = 0;

        metrics
    }
}

impl Point for Metrics {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Metrics> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.buffer)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}

#[cfg(test)]
mod test {
    use serde_json::Value;

    use super::*;
    use crate::{
        base::{Stream, StreamConfig, DEFAULT_TIMEOUT},
        config::Persistence,
        Payload,
    };
    use std::collections::HashMap;

    #[derive(Clone)]
    pub struct MockClient {
        pub net_tx: flume::Sender<Request>,
    }

    const PERSIST_FOLDER: &str = "/tmp/uplink_test";

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

        async fn publish_bytes<S>(
            &self,
            topic: S,
            qos: QoS,
            retain: bool,
            payload: Bytes,
        ) -> Result<(), MqttError>
        where
            S: Into<String> + Send,
        {
            let mut publish = Publish::from_bytes(topic, qos, payload);
            publish.retain = retain;
            let publish = Request::Publish(publish);
            self.net_tx.send_async(publish).await.map_err(|e| MqttError::Send(e.into_inner()))?;
            Ok(())
        }
    }

    fn write_to_storage(storage: &mut Storage, publish: &Publish) {
        if let Err(e) = publish.write(storage.writer()) {
            panic!("Failed to fill write buffer. Error = {:?}", e);
        }

        if let Err(e) = storage.flush_on_overflow() {
            panic!("Failed to flush write buffer to disk. Error = {:?}", e);
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
            max_packet_size: 1024 * 1024,
            ..Default::default()
        }
    }

    fn config_with_persistence(path: String) -> Config {
        std::fs::create_dir_all(&path).unwrap();
        let mut config = default_config();
        config.persistence = Some(Persistence {
            path: path.clone(),
            max_file_size: 10 * 1024 * 1024,
            max_file_count: 3,
        });

        config
    }

    fn defaults(
        config: Arc<Config>,
    ) -> (Serializer<MockClient>, flume::Sender<Box<dyn Package>>, Receiver<Request>) {
        let (data_tx, data_rx) = flume::bounded(1);
        let (net_tx, net_rx) = flume::bounded(1);
        let client = MockClient { net_tx };

        let metrics_config = StreamConfig {
            topic: Default::default(),
            buf_size: 100,
            flush_period: DEFAULT_TIMEOUT,
        };
        let metrics_stream = Stream::with_config(
            &"metrics".to_owned(),
            &config.project_id,
            &config.device_id,
            &metrics_config,
            data_tx.clone(),
        );

        (Serializer::new(config, data_rx, metrics_stream, client).unwrap(), data_tx, net_rx)
    }

    #[derive(Error, Debug)]
    pub enum Error {
        #[error("Serde error {0}")]
        Serde(#[from] serde_json::Error),
        #[error("Stream error {0}")]
        Base(#[from] crate::base::Error),
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
        let config = Arc::new(config_with_persistence(format!("{}/disk", PERSIST_FOLDER)));
        std::fs::create_dir_all(&config.persistence.as_ref().unwrap().path).unwrap();

        let (mut serializer, _, _) = defaults(config);
        let mut storage = serializer.storage.take().unwrap();

        let mut publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        publish.pkid = 1;

        write_to_storage(&mut storage, &publish);

        let stored_publish = read_from_storage(&mut storage, serializer.config.max_packet_size);

        assert_eq!(publish, stored_publish);
    }

    #[test]
    // Force runs serializer in disk mode, with network returning
    fn disk_to_catchup() {
        let config = Arc::new(config_with_persistence(format!("{}/disk_catchup", PERSIST_FOLDER)));

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
        let config = Arc::new(config_with_persistence(format!("{}/disk_crash", PERSIST_FOLDER)));

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
        let config = Arc::new(config_with_persistence(format!("{}/catchup_empty", PERSIST_FOLDER)));

        let (mut serializer, _, _) = defaults(config);

        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with data already in persistence
    fn catchup_to_normal_with_persistence() {
        let config =
            Arc::new(config_with_persistence(format!("{}/catchup_normal", PERSIST_FOLDER)));

        let (mut serializer, data_tx, net_rx) = defaults(config);
        let mut storage = serializer.storage.take().unwrap();

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
        let mut publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        publish.pkid = 1;

        write_to_storage(&mut storage, &publish);

        // Replace storage into serializer
        serializer.storage = Some(storage);
        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    fn catchup_to_crash_with_persistence() {
        let config = Arc::new(config_with_persistence(format!("{}/catchup_crash", PERSIST_FOLDER)));

        std::fs::create_dir_all(&config.persistence.as_ref().unwrap().path).unwrap();

        let (mut serializer, data_tx, _) = defaults(config);
        let mut storage = serializer.storage.take().unwrap();

        let mut collector = MockCollector::new(data_tx);
        // Run a collector
        std::thread::spawn(move || {
            for i in 2..6 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(10));
            }
        });

        // Force write a publish into storage
        let mut publish = Publish::new(
            "hello/world",
            QoS::AtLeastOnce,
            "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]".as_bytes(),
        );
        publish.pkid = 1;

        write_to_storage(&mut storage, &publish);

        // Replace storage into serializer
        serializer.storage = Some(storage);
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
