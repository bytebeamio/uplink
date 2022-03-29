use crate::base::{Config, Package};

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

#[derive(thiserror::Error, Debug)]
pub enum MqttError {
    #[error("Client error {0}")]
    Client(#[from] ClientError),
    #[error("Net Send error {0}")]
    Send(#[from] flume::SendError<Request>),
    #[error("Net TrySend error {0}")]
    TrySend(#[from] flume::TrySendError<Request>),
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
///        ┌───────────────────┐
///        │Serializer::start()│
///        └─────────┬─────────┘
///                  │
///                  │ State transitions happen
///                  │ within the loop{}             Load data in Storage from
///                  │                               previouse sessions/iterations                  AsyncClient has crashed
///          ┌───────▼──────┐                       ┌─────────────────────┐                      ┌───────────────────────┐
///          │EventLoopReady├───────────────────────►Serializer::catchup()├──────────────────────►EventLoopCrash(publish)│
///          └───────▲──────┘                       └──────────┬──────────┘                      └───────────┬───────────┘
///                  │                                         │                                             │
///                  │                                         │ No more data left in Storage                │
///                  │                                         │                                             │
///     ┌────────────┴────────────┐                        ┌───▼──┐                             ┌────────────▼─────────────┐
///     │Serializer::disk(publish)│                        │Normal│                             │Serializer::crash(publish)├──┐
///     └────────────▲────────────┘                        └───┬──┘                             └─────────────────────────▲┘  │
///                  │                                         │                                 Write all data to Storage└───┘
///                  │                                         │
///                  │                                         │
///      ┌───────────┴──────────┐                   ┌──────────▼─────────┐
///      │SlowEventloop(publish)◄───────────────────┤Serializer::normal()│
///      └──────────────────────┘                   └────────────────────┘
///       Slow Network,                             Forward all data to Bytebeam,
///       save to Storage before forwarding         through AsyncClient
///
///```
pub struct Serializer<C: MqttClient> {
    config: Arc<Config>,
    collector_rx: Receiver<Box<dyn Package>>,
    client: C,
    storage: Option<Storage>,
    metrics: Metrics,
}

impl<C: MqttClient> Serializer<C> {
    pub fn new(
        config: Arc<Config>,
        collector_rx: Receiver<Box<dyn Package>>,
        client: C,
    ) -> Result<Serializer<C>, Error> {
        let metrics_config = config.streams.get("metrics").unwrap();
        let metrics = Metrics::new(&metrics_config.topic);

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

        Ok(Serializer { config, collector_rx, client, storage, metrics })
    }

    /// Write all data received, from here-on, to disk only.
    async fn crash(&mut self, mut publish: Publish) -> Result<Status, Error> {
        let storage = match &mut self.storage {
            Some(s) => s,
            None => return Err(Error::MissingPersistence),
        };
        // Write failed publish to disk first
        publish.pkid = 1;

        loop {
            let data = self.collector_rx.recv_async().await?;
            let topic = data.topic();
            let payload = data.serialize();

            let mut publish = Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload);
            publish.pkid = 1;

            if let Err(e) = publish.write(&mut storage.writer()) {
                error!("Failed to fill write buffer during bad network. Error = {:?}", e);
                continue;
            }

            match storage.flush_on_overflow() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        "Failed to flush write buffer to disk during bad network. Error = {:?}",
                        e
                    );
                    continue;
                }
            }
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    async fn disk(&mut self, publish: Publish) -> Result<Status, Error> {
        let storage = match &mut self.storage {
            Some(s) => s,
            None => return Err(Error::MissingPersistence),
        };
        info!("Switching to slow eventloop mode!!");

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let publish =
            self.client.publish(&publish.topic, QoS::AtLeastOnce, false, &publish.payload[..]);
        tokio::pin!(publish);

        loop {
            select! {
                data = self.collector_rx.recv_async() => {
                      let data = data?;
                      if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                      }

                      let topic = data.topic();
                      let payload = data.serialize();
                      let payload_size = payload.len();
                      let mut publish = Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload);
                      publish.pkid = 1;

                      match publish.write(&mut storage.writer()) {
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
                o = &mut publish => {
                    o?;
                    return Ok(Status::EventLoopReady)
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
            None => return Err(Error::MissingPersistence),
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
            Ok(packet) => unreachable!("{:?}", packet),
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
                      let payload = data.serialize();
                      let payload_size = payload.len();
                      let mut publish = Publish::new(topic.as_ref(), QoS::AtLeastOnce, payload);
                      publish.pkid = 1;

                      match publish.write(&mut storage.writer()) {
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
                        Err(MqttError::Send(request)) => match request.into_inner() {
                            Request::Publish(publish) => return Ok(Status::EventLoopCrash(publish)),
                            request => unreachable!("{:?}", request),
                        },
                        Err(MqttError::Client(ClientError::Request(request)))=> match request.into_inner() {
                            Request::Publish(publish) => return Ok(Status::EventLoopCrash(publish)),
                            request => unreachable!("{:?}", request),
                        },
                        Err(e) => return Err(e.into()),
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
                        Ok(packet) => unreachable!("{:?}", packet),
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
            let (payload_size, result) = select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;

                    // Extract anomalies detected by package during collection
                    if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                    }

                    let topic = data.topic();
                    let payload = data.serialize();
                     (payload.len(), self.client.try_publish(topic.as_ref(), QoS::AtLeastOnce, false, payload))

                }
                _ = interval.tick() => {
                    let (topic, payload) = self.metrics.next();
                    (payload.len(), self.client.try_publish(topic, QoS::AtLeastOnce, false, payload))
                }
            };

            let failed = match result {
                Ok(_) => {
                    self.metrics.add_total_sent_size(payload_size);
                    continue;
                }
                Err(MqttError::TrySend(request)) => request.into_inner(),
                Err(MqttError::Client(ClientError::TryRequest(request))) => request.into_inner(),
                Err(e) => return Err(e.into()),
            };

            match failed {
                Request::Publish(publish) => return Ok(Status::SlowEventloop(publish)),
                request => unreachable!("{:?}", request),
            };
        }
    }

    /// Direct mode is used in case uplink is used with persistence disabled.
    /// It is operated differently from all other modes. Failure is terminal.
    async fn direct(&mut self) -> Result<(), Error> {
        let mut interval = time::interval(time::Duration::from_secs(10));

        loop {
            let payload_size = select! {
                data = self.collector_rx.recv_async() => {
                    let data = data?;

                    // Extract anomalies detected by package during collection
                    if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                    }

                    let topic = data.topic();
                    let payload = data.serialize();
                    let payload_size = payload.len();
                    self.client.publish(topic.as_ref(), QoS::AtLeastOnce, false, payload).await?;
                    payload_size
                }
                _ = interval.tick() => {
                    let (topic, payload) = self.metrics.next();
                    let payload_size = payload.len();
                    self.client.publish(topic, QoS::AtLeastOnce, false, payload).await?;
                    payload_size
                }
            };

            self.metrics.add_total_sent_size(payload_size);
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        if self.storage.is_none() {
            return self.direct().await;
        }

        let mut status = Status::EventLoopReady;

        loop {
            let next_status = match status {
                Status::Normal => self.normal().await?,
                Status::SlowEventloop(publish) => self.disk(publish).await?,
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

#[derive(Debug, Default, Serialize)]
struct Metrics {
    #[serde(skip_serializing)]
    topic: String,
    sequence: u32,
    timestamp: u64,
    total_sent_size: usize,
    total_disk_size: usize,
    lost_segments: usize,
    errors: String,
    error_count: usize,
}

impl Metrics {
    pub fn new<T: Into<String>>(topic: T) -> Metrics {
        Metrics { topic: topic.into(), errors: String::with_capacity(1024), ..Default::default() }
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

    pub fn next(&mut self) -> (&str, Vec<u8>) {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        self.timestamp = timestamp.as_millis() as u64;
        self.sequence += 1;

        let payload = serde_json::to_vec(&vec![&self]).unwrap();
        self.errors.clear();
        self.lost_segments = 0;
        (&self.topic, payload)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        base::{Stream, StreamConfig},
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
            self.net_tx.send_async(publish).await?;
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
            self.net_tx.try_send(publish)?;
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
            self.net_tx.send_async(publish).await?;
            Ok(())
        }
    }

    fn default_config() -> Config {
        let mut streams = HashMap::new();
        streams.insert(
            "metrics".to_owned(),
            StreamConfig { topic: Default::default(), buf_size: 100 },
        );
        Config {
            broker: "localhost".to_owned(),
            port: 1883,
            device_id: "123".to_owned(),
            streams,
            ..Default::default()
        }
    }

    fn config_with_persistence(path: String) -> Config {
        std::fs::create_dir_all(&path).unwrap();
        let mut config = default_config();
        config.persistence =
            Some(Persistence { path: path.clone(), max_file_size: 2048, max_file_count: 1024 });

        config
    }

    fn defaults(
        config: Arc<Config>,
    ) -> (Serializer<MockClient>, flume::Sender<Box<dyn Package>>, Receiver<Request>) {
        let (data_tx, data_rx) = flume::bounded(1);
        let (net_tx, net_rx) = flume::bounded(1);
        let client = MockClient { net_tx };

        (Serializer::new(config, data_rx, client).unwrap(), data_tx, net_rx)
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
    // Runs serializer in direct mode, without persistence
    fn normal_working() {
        let config = default_config();
        let (mut serializer, data_tx, net_rx) = defaults(Arc::new(config));

        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.start())
        });

        let mut collector = MockCollector::new(data_tx);
        std::thread::spawn(move || {
            collector.send(1).unwrap();
        });

        match net_rx.recv().unwrap() {
            Request::Publish(Publish { qos: QoS::AtLeastOnce, payload, topic, .. }) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            r => panic!("Unexpected value returned: {:?}", r),
        }
    }

    #[test]
    // Force runs serializer in normal mode, without persistence
    fn normal_to_slow() {
        let config = default_config();
        let (mut serializer, data_tx, net_rx) = defaults(Arc::new(config));

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
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            s => panic!("Unexpected status: {:?}", s),
        }
    }

    #[test]
    // Force runs serializer in disk mode, with persistence
    fn write_to_disk() {
        let config = Arc::new(config_with_persistence(format!("{}/disk", PERSIST_FOLDER)));

        let (mut serializer, data_tx, net_rx) = defaults(config);

        std::thread::spawn(move || loop {
            std::thread::sleep(time::Duration::from_secs(10));
            net_rx.recv().unwrap();
        });

        let mut collector = MockCollector::new(data_tx);
        std::thread::spawn(move || {
            for i in 1..6 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(10));
            }
        });

        let status = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                match serializer.normal().await.unwrap() {
                    Status::SlowEventloop(publish) => serializer.disk(publish).await,
                    s => panic!("Unexpected status: {:?}", s),
                }
            })
            .unwrap();
        assert_eq!(status, Status::EventLoopReady);

        // Verify the contents of persistence
        match read(serializer.storage.unwrap().reader(), 1024).unwrap() {
            Packet::Publish(Publish { topic, payload, .. }) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    // Force runs serializer in catchup mode, with empty persistence
    fn catchup_to_normal_empty_persistence() {
        let config = Arc::new(config_with_persistence(format!("{}/catchup", PERSIST_FOLDER)));

        let (mut serializer, _, _) = defaults(config);

        let status =
            tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap();
        assert_eq!(status, Status::Normal);
    }

    #[test]
    // Force runs serializer in catchup mode, with persistence and crashed network
    fn catchup_to_crash_persistence() {
        let config = Arc::new(config_with_persistence(format!("{}/catchup_crash", PERSIST_FOLDER)));

        std::fs::create_dir_all(&config.persistence.as_ref().unwrap().path).unwrap();

        let (mut serializer, data_tx, net_rx) = defaults(config);

        if let Some(storage) = &mut serializer.storage {
            for i in 1..3 {
                Publish {
                    dup: false,
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    topic: "hello/world".to_owned(),
                    pkid: i,
                    payload: Bytes::from("{\"msg\": \"Hello, World!\"}"),
                }
                .write(storage.writer())
                .unwrap();
            }
        }

        std::thread::spawn(move || loop {
            std::thread::sleep(time::Duration::from_secs(100));
            net_rx.recv().unwrap();
        });

        let mut collector = MockCollector::new(data_tx);
        std::thread::spawn(move || {
            for i in 1..6 {
                collector.send(i).unwrap();
                std::thread::sleep(time::Duration::from_secs(10));
            }
        });

        match tokio::runtime::Runtime::new().unwrap().block_on(serializer.catchup()).unwrap() {
            Status::EventLoopCrash(Publish { topic, payload, .. }) => {
                assert_eq!(topic, "hello/world");
                let recvd = std::str::from_utf8(&payload).unwrap();
                assert_eq!(recvd, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
            }
            s => panic!("Unexpected status: {:?}", s),
        }
    }
}
