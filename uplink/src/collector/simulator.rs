use async_channel::Sender;
use rand::Rng;
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use tokio::time::Duration;

use std::{collections::HashMap, io, sync::Arc};

use crate::base::{timestamp, Config, Package, Stream};
use crate::collector::tcpjson::Payload;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

pub struct Simulator {
    config: Arc<Config>,
    partitions: HashMap<String, Stream<Payload>>,
    data_tx: Sender<Box<dyn Package>>,
}

impl Simulator {
    pub fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>) -> Self {
        let mut partitions = HashMap::new();
        for (stream, config) in config.streams.clone() {
            partitions.insert(
                stream.clone(),
                Stream::new(stream, config.topic, config.buf_size, data_tx.clone()),
            );
        }

        Simulator { config, partitions, data_tx }
    }

    pub(crate) async fn start(&mut self) {
        let mut gps_timestamp = timestamp();
        let mut can_timestamp = timestamp();

        for i in 0..1_000_000 {
            let sleep_millis = 10;
            tokio::time::sleep(Duration::from_millis(sleep_millis)).await;
            gps_timestamp += sleep_millis;
            can_timestamp += sleep_millis;

            let stream = "can";
            let can = Payload {
                stream: stream.to_string(),
                sequence: i,
                timestamp: can_timestamp,
                payload: json!(Can::new()),
            };
            let partition = match self.partitions.get_mut(stream) {
                Some(partition) => partition,
                None => {
                    let s = Stream::dynamic(
                        stream,
                        &self.config.project_id,
                        &self.config.device_id,
                        self.data_tx.clone(),
                    );
                    self.partitions.entry(stream.to_owned()).or_insert(s)
                }
            };

            partition.fill(can).await.unwrap();

            let stream = "gps";
            let partition = match self.partitions.get_mut(stream) {
                Some(partition) => partition,
                None => {
                    let s = Stream::dynamic(
                        stream,
                        &self.config.project_id,
                        &self.config.device_id,
                        self.data_tx.clone(),
                    );
                    self.partitions.entry(stream.to_owned()).or_insert(s)
                }
            };

            let gps = Payload {
                stream: stream.to_string(),
                sequence: i,
                timestamp: gps_timestamp,
                payload: json!(Gps::new()),
            };
            partition.fill(gps).await.unwrap();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Gps {
    lat: f64,
    lon: f64,
}

impl Gps {
    pub fn new() -> Gps {
        let mut rng = rand::thread_rng();
        Gps { lat: rng.gen_range(40f64, 45f64), lon: rng.gen_range(95f64, 96f64) }
    }
}

#[derive(Debug, Serialize)]
pub struct Can {
    data: u64,
}

impl Can {
    pub fn new() -> Can {
        Can { data: 10 }
    }
}
