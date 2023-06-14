use std::time::Duration;

use log::{error, warn};
use prometheus_parse::Scrape;
use reqwest::{Client, Method};
use serde_json::{Map, Number, Value};

use crate::base::{
    bridge::{BridgeTx, Payload},
    clock, PrometheusConfig,
};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Reqwest client error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct Prometheus {
    config: PrometheusConfig,
    client: Client,
    tx: BridgeTx,
}

impl Prometheus {
    pub fn new(config: PrometheusConfig, tx: BridgeTx) -> Self {
        Self { client: Client::new(), config, tx }
    }

    #[tokio::main]
    pub async fn start(self) {
        let mut sequence = 0;
        loop {
            match self.query().await {
                Ok(payload) => {
                    sequence += 1;
                    let payload = Payload {
                        stream: self.config.stream_name.clone(),
                        device_id: None,
                        sequence,
                        timestamp: clock() as u64,
                        payload,
                    };
                    self.tx.send_payload(payload).await
                }
                Err(e) => error!("{e}"),
            }

            tokio::time::sleep(Duration::from_secs(self.config.interval)).await;
        }
    }

    async fn query(&self) -> Result<Value, Error> {
        let resp = self.client.request(Method::GET, &self.config.endpoint).send().await?;
        let lines: Vec<_> = resp.text().await?.lines().map(|s| Ok(s.to_owned())).collect();
        let metrics = Scrape::parse(lines.into_iter())?;

        let mut json = Map::new();
        for sample in metrics.samples {
            let num = match sample.value {
                prometheus_parse::Value::Counter(c) => c,
                prometheus_parse::Value::Gauge(g) => g,
                v => {
                    warn!("Unexpected value: {v:?}");
                    continue;
                }
            };
            let num = match Number::from_f64(num) {
                Some(c) => c,
                _ => {
                    warn!("Couldn't parse number: {num}");
                    continue;
                }
            };
            json.insert(sample.metric, Value::Number(num));
        }

        Ok(Value::Object(json))
    }
}
