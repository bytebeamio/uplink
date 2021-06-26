use crate::base::{Config, Package};

use async_channel::{Receiver, RecvError};
use bytes::Bytes;
use disk::Storage;
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
    Client(#[from] ClientError),
}

enum Status {
    Normal,
    SlowEventloop(Publish),
    EventLoopReady,
    EventLoopCrash(Publish),
}

pub struct Serializer {
    config: Arc<Config>,
    collector_rx: Receiver<Box<dyn Package>>,
    client: AsyncClient,
    storage: Storage,
    metrics: Metrics,
}

impl Serializer {
    pub fn new(
        config: Arc<Config>,
        collector_rx: Receiver<Box<dyn Package>>,
        client: AsyncClient,
        storage: Storage,
    ) -> Result<Serializer, Error> {
        let metrics_config = config.streams.get("metrics").unwrap();
        let metrics = Metrics::new(&metrics_config.topic);

        Ok(Serializer { config, collector_rx, client, storage, metrics })
    }

    async fn crash(&mut self, mut publish: Publish) -> Result<Status, Error> {
        // Write failed publish to disk first
        publish.pkid = 1;

        loop {
            let data = self.collector_rx.recv().await?;
            let (topic, payload) = match topic_and_payload(&self.config, data) {
                Some(v) => v,
                None => continue,
            };

            let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
            publish.pkid = 1;

            if let Err(e) = publish.write(&mut self.storage.writer()) {
                error!("Failed to fill write buffer during bad network. Error = {:?}", e);
                continue;
            }

            match self.storage.flush_on_overflow() {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to flush write buffer to disk during bad network. Error = {:?}", e);
                    continue;
                }
            }
        }
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    async fn disk(&mut self, publish: Publish) -> Result<Status, Error> {
        info!("Switching to slow eventloop mode!!");

        // Note: self.client.publish() is executing code before await point
        // in publish method every time. Verify this behaviour later
        let publish = self.client.publish(&publish.topic, QoS::AtLeastOnce, false, &publish.payload[..]);
        tokio::pin!(publish);

        loop {
            select! {
                data = self.collector_rx.recv() => {
                      let data = data?;
                      if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                      }

                      let (topic, payload) = match topic_and_payload(&self.config, data) {
                            Some(v) => v,
                            None => continue
                      };

                      let payload_size = payload.len();
                      let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
                      publish.pkid = 1;

                      match publish.write(&mut self.storage.writer()) {
                           Ok(_) => self.metrics.add_total_disk_size(payload_size),
                           Err(e) => {
                               error!("Failed to fill disk buffer. Error = {:?}", e);
                               continue
                           }
                      }

                      match self.storage.flush_on_overflow() {
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
        info!("Switching to catchup mode!!");

        let max_packet_size = self.config.max_packet_size;
        let storage = &mut self.storage;
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
                data = self.collector_rx.recv() => {
                      let data = data?;
                      if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                      }

                      let (topic, payload) = match topic_and_payload(&self.config, data) {
                            Some(v) => v,
                            None => continue
                      };

                      let payload_size = payload.len();
                      let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
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
                        Err(ClientError::Request(request)) => match request.into_inner() {
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
        let mut interval = time::interval(time::Duration::from_secs(2));

        loop {
            let (topic, payload) = select! {
                data = self.collector_rx.recv() => {
                    let data = data?;

                    // Extract anomalies detected by package during collection
                    if let Some((errors, count)) = data.anomalies() {
                        self.metrics.add_errors(errors, count);
                    }

                    let (topic, payload) = match topic_and_payload(&self.config, data) {
                        Some(v) => v,
                        None => continue,
                    };

                    (topic, payload)
                }
                _ = interval.tick() => {
                    let (topic, payload) = self.metrics.next();
                    (topic, payload)
                }
            };

            let payload_size = payload.len();
            let failed = match self.client.try_publish(topic, QoS::AtLeastOnce, false, payload) {
                Ok(_) => {
                    self.metrics.add_total_sent_size(payload_size);
                    continue;
                }
                Err(ClientError::TryRequest(request)) => request,
                Err(e) => return Err(e.into()),
            };

            match failed.into_inner() {
                Request::Publish(publish) => return Ok(Status::SlowEventloop(publish)),
                request => unreachable!("{:?}", request),
            };
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
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

async fn send_publish(client: AsyncClient, topic: String, payload: Bytes) -> Result<AsyncClient, ClientError> {
    client.publish_bytes(topic, QoS::AtLeastOnce, false, payload).await?;
    Ok(client)
}

fn topic_and_payload(config: &Arc<Config>, data: Box<dyn Package>) -> Option<(String, Vec<u8>)> {
    let stream = &data.stream();
    let topic = match config.streams.get(stream) {
        Some(s) => s.topic.clone(),
        None => {
            error!("Unconfigured stream = {}", stream);
            return None;
        }
    };

    let payload = data.serialize();
    if payload.is_empty() {
        warn!("Empty payload. Stream = {}", stream);
        return None;
    }

    Some((topic, payload))
}

#[derive(Debug, Default, Clone, Serialize)]
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

    pub fn next(&mut self) -> (String, Vec<u8>) {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        self.timestamp = timestamp.as_millis() as u64;
        self.sequence += 1;

        let payload = serde_json::to_vec(&vec![&self]).unwrap();
        self.errors.clear();
        self.lost_segments = 0;
        (self.topic.clone(), payload)
    }
}
