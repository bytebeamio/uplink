use crate::base::{Config, Package, Partitions};
use async_channel::Sender;
use serde::Serialize;
use serde_json::json;
use std::io;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

pub struct Simulator {
    _config: Arc<Config>,
    partitions: Partitions<Payload>,
}

impl Simulator {
    pub fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>) -> Self {
        let streams = config.streams.iter();
        let streams: Vec<(String, usize)> = streams.map(|(s, c)| (s.to_owned(), c.buf_size as usize)).collect();
        let partitions = Partitions::new(data_tx, streams.clone());
        Simulator { _config: config, partitions }
    }

    pub(crate) async fn start(&mut self) {
        let mut gps_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut can_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        for i in 0..10000 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            gps_timestamp += 100;
            can_timestamp += 100;

            let can = Payload { stream: "can".to_string(), sequence: i, timestamp: can_timestamp, payload: json!(Can::new()) };
            self.partitions.fill("can", can).await.unwrap();

            let payload =
                Payload { stream: "gps".to_string(), sequence: i, timestamp: gps_timestamp, payload: json!(Gps::new()) };
            self.partitions.fill("gps", payload).await.unwrap();
        }
    }
}

use crate::collector::tcpjson::Payload;
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;

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
