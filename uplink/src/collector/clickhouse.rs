use clickhouse::{error::Error, Client};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use tokio::{
    task::JoinSet,
    time::{interval, Duration},
};

use crate::{
    base::{
        bridge::{BridgeTx, Payload},
        clock,
    },
    config::{ClickhouseConfig, QueryLogConfig},
};

#[derive(Debug, clickhouse::Row, Serialize, Deserialize)]
struct QueryLog {
    event_time: u32,
    query_duration_ms: u64,
    read_rows: u64,
    written_rows: u64,
    result_rows: u64,
    memory_usage: u64,
    current_database: String,
    query_id: String,
    query: String,
    query_kind: String,
    user: String,
    exception_code: i32,
    exception: String,
}

impl From<QueryLog> for Payload {
    fn from(value: QueryLog) -> Self {
        Payload {
            stream: Default::default(),
            sequence: Default::default(),
            timestamp: Default::default(),
            payload: serde_json::to_value(value).unwrap(),
        }
    }
}

pub struct ClickhouseReader {
    config: ClickhouseConfig,
    bridge_tx: BridgeTx,
}

impl ClickhouseReader {
    pub fn new(config: ClickhouseConfig, bridge_tx: BridgeTx) -> Self {
        Self { config, bridge_tx }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        let url = format!("http://{}:{}", self.config.host, self.config.port);
        let client = clickhouse::Client::default()
            .with_url(url)
            .with_user(&self.config.username)
            .with_password(&self.config.password);

        let mut join_set = JoinSet::new();

        if let Some(config) = self.config.query_log {
            let reader = QueryLogReader::new(config, client.clone(), self.bridge_tx.clone());
            join_set.spawn(reader.start());
        }

        while let Some(r) = join_set.join_next().await {
            if let Err(e) = r {
                error!("Error running reader: {e}")
            }
        }
    }
}

pub struct QueryLogReader {
    config: QueryLogConfig,
    client: Client,
    query: String,
    bridge_tx: BridgeTx,
    sequence: u32,
}

impl QueryLogReader {
    fn new(config: QueryLogConfig, client: Client, bridge_tx: BridgeTx) -> Self {
        // Construct clickhouse query string
        let where_clause = &config.where_clause;
        let sync_interval = config.interval;
        let query = format!(
            "SELECT ?fields FROM system.query_log WHERE {where_clause} AND event_time > (now() - toIntervalSecond({sync_interval}))"
        );
        info!("Query: {query}");
        Self { config, client, query, bridge_tx, sequence: 0 }
    }

    async fn start(mut self) {
        // Execute query on an interval basis
        let mut interval = interval(Duration::from_secs(self.config.interval));
        loop {
            if let Err(e) = self.run().await {
                error!("Clickhouse query failed: {e}");
            };

            interval.tick().await;
        }
    }

    /// Read rows from clikchouse, construct payload and push onto relevant stream
    async fn run(&mut self) -> Result<(), Error> {
        let mut cursor = self.client.query(&self.query).fetch::<QueryLog>()?;

        while let Some(row) = cursor.next().await? {
            debug!("Row: {row:?}");
            let mut payload: Payload = row.into();
            payload.timestamp = clock() as u64;
            self.config.stream.clone_into(&mut payload.stream);
            self.sequence += 1;
            payload.sequence = self.sequence;
            self.bridge_tx.send_payload(payload).await;
        }

        Ok(())
    }
}
