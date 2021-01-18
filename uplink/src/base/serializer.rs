use crate::base::{Config, Package};

use async_channel::{Receiver, RecvError};
use bytes::Bytes;
use disk::Storage;
use rumqttc::*;
use std::io;
use std::sync::Arc;
use thiserror::Error;
use tokio::select;

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
    #[error("Mqtt serialization error")]
    Mqtt(rumqttc::Error),
}

enum Status {
    Normal,
    SlowEventloop(Publish),
    EventLoopReady,
}

pub struct Serializer {
    config: Arc<Config>,
    collector_rx: Receiver<Box<dyn Package>>,
    client: AsyncClient,
    storage: Storage,
}

impl Serializer {
    pub fn new(
        config: Arc<Config>,
        collector_rx: Receiver<Box<dyn Package>>,
        client: AsyncClient,
        storage: Storage,
    ) -> Result<Serializer, Error> {
        Ok(Serializer { config, collector_rx, client, storage })
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
                      let stream = &data.stream();
                      let topic = self.config.streams.get(stream).unwrap().topic.clone();
                      let payload = data.serialize();

                      let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
                      publish.pkid = 1;
                      publish.write(&mut self.storage.writer()).map_err(|e| Error::Mqtt(e))?;
                      self.storage.flush_on_overflow()?;
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

        let publish = match read(storage.reader(), max_packet_size).map_err(|e| Error::Mqtt(e))? {
            Packet::Publish(publish) => publish,
            packet => unreachable!("{:?}", packet),
        };

        let send = send_publish(client, publish.topic, publish.payload);
        tokio::pin!(send);

        loop {
            select! {
                data = self.collector_rx.recv() => {
                      let data = data?;
                      let stream = &data.stream();
                      let topic = self.config.streams.get(stream).unwrap().topic.clone();
                      let payload = data.serialize();

                      let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
                      publish.pkid = 1;
                      publish.write(&mut storage.writer()).map_err(|e| Error::Mqtt(e))?;
                      storage.flush_on_overflow()?;
                }
                o = &mut send => {
                    let client = o?;

                    // Done reading all the pending files
                    if storage.reload_on_eof()? {
                        return Ok(Status::Normal);
                    }

                    let publish = match read(storage.reader(), max_packet_size).map_err(|e| Error::Mqtt(e))? {
                        Packet::Publish(publish) => publish,
                        packet => unreachable!("{:?}", packet),
                    };

                    send.set(send_publish(client, publish.topic, publish.payload));
                }
            }
        }
    }

    async fn normal(&mut self) -> Result<Status, Error> {
        info!("Switching to normal mode!!");

        loop {
            let data = self.collector_rx.recv().await?;
            let stream = &data.stream();
            let topic = self.config.streams.get(stream).unwrap().topic.clone();
            let payload = data.serialize();

            match self.client.try_publish(topic, QoS::AtLeastOnce, false, payload) {
                Ok(_) => continue,
                Err(ClientError::TryRequest(request)) => match request.into_inner() {
                    Request::Publish(publish) => return Ok(Status::SlowEventloop(publish)),
                    request => unreachable!("{:?}", request),
                },
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut status = Status::EventLoopReady;

        loop {
            let next_status = match status {
                Status::Normal => self.normal().await?,
                Status::SlowEventloop(publish) => self.disk(publish).await?,
                Status::EventLoopReady => self.catchup().await?,
            };

            status = next_status;
        }
    }
}

async fn send_publish(client: AsyncClient, topic: String, payload: Bytes) -> Result<AsyncClient, ClientError> {
    client.publish_bytes(topic, QoS::AtLeastOnce, false, payload).await?;
    Ok(client)
}
