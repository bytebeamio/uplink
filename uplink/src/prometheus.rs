use std::time::Duration;

use log::{error, warn};
use prometheus_parse::Scrape;
use reqwest::{Client, Method};
use serde_json::{Map, Number, Value};

use crate::base::PrometheusConfig;

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
}

impl Prometheus {
    pub fn new(config: PrometheusConfig) -> Self {
        Self { client: Client::new(), config }
    }

    #[tokio::main]
    pub async fn start(self) {
        loop {
            if let Err(e) = self.query().await {
                error!("{e}")
            }

            tokio::time::sleep(Duration::from_secs(self.config.interval)).await;
        }
    }

    async fn query(&self) -> Result<(), Error> {
        let resp = self.client.request(Method::GET, &self.config.endpoint).send().await?;
        let lines: Vec<_> = resp.text().await?.lines().map(|s| Ok(s.to_owned())).collect();
        let metrics = Scrape::parse(lines.into_iter())?;

        let mut json = Map::new();
        for sample in metrics.samples {
            let count = match sample.value {
                prometheus_parse::Value::Counter(c) => c,
                v => {
                    warn!("Unexpected value: {v:?}");
                    continue;
                }
            };
            let count = match Number::from_f64(count) {
                Some(c) => c,
                _ => {
                    warn!("Couldn't parse number: {count}");
                    continue;
                }
            };
            json.insert(sample.metric, Value::Number(count));
        }

        Ok(())
    }
}
