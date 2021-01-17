use crate::base::{Config, Package};

use async_channel::{Receiver, RecvError};
use disk::Storage;
use rumqttc::*;
use std::io;
use std::path::Path;
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
    pub fn new(config: Arc<Config>, collector_rx: Receiver<Box<dyn Package>>, client: AsyncClient) -> Result<Serializer, Error> {
        let storage = Storage::new(&Path::new("/tmp/persist"), 10240, 10)?;
        Ok(Serializer { config, collector_rx, client, storage })
    }

    /// Write new data to disk until back pressure due to slow n/w is resolved
    async fn disk(&mut self, publish: Publish) -> Result<Status, Error> {
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
                o = self.client.publish(&publish.topic, QoS::AtLeastOnce, false, &publish.payload[..]) => {
                    o?;
                    return Ok(Status::EventLoopReady)
                }
            }
        }
    }

    /// Write new collector data to disk while sending existing data on
    /// disk to mqtt eventloop
    async fn catchup(&mut self) -> Result<Status, Error> {
        loop {
            if let Ok(data) = self.collector_rx.try_recv() {
                let stream = &data.stream();
                let topic = self.config.streams.get(stream).unwrap().topic.clone();
                let payload = data.serialize();

                let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
                publish.pkid = 1;
                publish.write(&mut self.storage.writer()).map_err(|e| Error::Mqtt(e))?;
                self.storage.flush_on_overflow()?;
            }

            // Done reading all the pending files
            if self.storage.reload_on_eof().unwrap() {
                return Ok(Status::Normal);
            }

            match read(self.storage.reader(), 1048).map_err(|e| Error::Mqtt(e))? {
                Packet::Publish(publish) => {
                    self.client.try_publish(publish.topic, QoS::AtLeastOnce, false, &publish.payload[..])?;
                }
                packet => unreachable!("{:?}", packet),
            }
        }
    }

    async fn normal(&mut self) -> Result<Status, Error> {
        loop {
            let data = self.collector_rx.recv().await?;
            let stream = &data.stream();
            let topic = self.config.streams.get(stream).unwrap().topic.clone();
            let payload = data.serialize();
            self.client.try_publish(topic, QoS::AtLeastOnce, false, payload)?;
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut status = Status::Normal;

        loop {
            let next_status = match status {
                Status::Normal => self.normal().await,
                Status::SlowEventloop(publish) => self.disk(publish).await,
                Status::EventLoopReady => self.catchup().await,
            };

            let next_status = match next_status {
                Ok(s) => s,
                Err(Error::Client(ClientError::TryRequest(request))) => match request.into_inner() {
                    Request::Publish(publish) => Status::SlowEventloop(publish),
                    v => unreachable!("{:?}", v),
                },
                Err(e) => return Err(e),
            };

            status = next_status;
        }
    }
}
