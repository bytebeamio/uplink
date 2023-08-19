use clickhouse::{error::Error, Client};
use log::error;
use serde_json::Value;
use tokio::time::{interval, Duration};

use crate::base::{
    bridge::{BridgeTx, Payload},
    clock, ClickhouseConfig,
};

#[derive(clickhouse::Row, serde::Deserialize)]
struct Row {
    #[serde(flatten)]
    payload: Value,
}

impl From<Row> for Payload {
    fn from(value: Row) -> Self {
        Payload {
            stream: Default::default(),
            device_id: None,
            sequence: Default::default(),
            timestamp: Default::default(),
            payload: value.payload,
        }
    }
}

pub struct ClickhouseReader {
    config: ClickhouseConfig,
    bridge_tx: BridgeTx,
    sequence: u32,
}

impl ClickhouseReader {
    pub fn new(config: ClickhouseConfig, bridge_tx: BridgeTx) -> Self {
        Self { config, bridge_tx, sequence: 0 }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        let url = format!("http://{}:{}", self.config.host, self.config.port);
        let client = clickhouse::Client::default()
            .with_url(url)
            .with_user(&self.config.username)
            .with_password(&self.config.password);

        let columns = self.config.cloumns.join(",");
        let table = &self.config.table;
        let query_time_threshold = self.config.query_time_threshold;
        let sync_interval = self.config.interval;
        let query = format!(
            r#"SELECT {columns} FROM {table} 
            WHERE query_duration_ms > {query_time_threshold} 
            AND event_time > (now() - toIntervalSecond({sync_interval}));"#
        );
        let mut interval = interval(Duration::from_secs(self.config.interval));
        loop {
            if let Err(e) = self.run(&client, &query).await {
                error!("Clickhouse query failed: {e}");
            };

            interval.tick().await;
        }
    }

    async fn run(&mut self, client: &Client, query: &str) -> Result<(), Error> {
        let mut cursor = client.query(&query).fetch::<Row>()?;

        while let Some(row) = cursor.next().await? {
            let mut payload: Payload = row.into();
            payload.timestamp = clock() as u64;
            payload.stream = self.config.stream.to_owned();
            self.sequence += 1;
            payload.sequence = self.sequence;
            self.bridge_tx.send_payload(payload).await;
        }

        Ok(())
    }
}
