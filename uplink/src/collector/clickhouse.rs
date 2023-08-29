use clickhouse::{error::Error, Client};
use log::{error, info};
use tokio::{
    task::JoinSet,
    time::{interval, Duration},
};

use crate::base::{
    bridge::{BridgeTx, Payload},
    clock, ClickhouseConfig, TableConfig,
};

impl From<Vec<u8>> for Payload {
    fn from(value: Vec<u8>) -> Self {
        Payload {
            stream: Default::default(),
            device_id: None,
            sequence: Default::default(),
            timestamp: Default::default(),
            payload: serde_json::from_slice(&value).unwrap(),
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

        for (stream, table_config) in self.config.tables {
            let reader =
                TableReader::new(table_config, stream, client.clone(), self.bridge_tx.clone());
            join_set.spawn(reader.start());
        }

        while let Some(r) = join_set.join_next().await {
            if let Err(e) = r {
                error!("Error running reader: {e}")
            }
        }
    }
}

pub struct TableReader {
    config: TableConfig,
    client: Client,
    stream: String,
    query: String,
    bridge_tx: BridgeTx,
    sequence: u32,
}

impl TableReader {
    fn new(config: TableConfig, stream: String, client: Client, bridge_tx: BridgeTx) -> Self {
        // Construct clickhouse query string
        let columns = config.cloumns.join(",");
        let table = &config.name;
        let where_clause = &config.where_clause;
        let sync_interval = config.interval;
        let query = format!(
            "SELECT {columns} FROM {table} WHERE {where_clause} AND event_time > (now() - toIntervalSecond({sync_interval})) FORMAT JSON"
        );
        info!("Query: {query}");
        info!("Stream_name: {stream}");
        Self { config, client, stream, query, bridge_tx, sequence: 0 }
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
        let mut cursor = self.client.query(&self.query).fetch::<Vec<u8>>()?;

        while let Some(row) = cursor.next().await? {
            let mut payload: Payload = row.into();
            payload.timestamp = clock() as u64;
            payload.stream = self.stream.to_owned();
            self.sequence += 1;
            payload.sequence = self.sequence;
            self.bridge_tx.send_payload(payload).await;
        }

        Ok(())
    }
}
