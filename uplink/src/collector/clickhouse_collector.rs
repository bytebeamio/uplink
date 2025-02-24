use std::time::{Duration, SystemTime};
use anyhow::Context;
use clickhouse::Row;
use flume::Sender;
use serde::Deserialize;
use serde_json::{json, Value};
use time::OffsetDateTime;
use tokio::time::sleep;
use crate::base::bridge::Payload;
use crate::base::clock;

#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseCollectorConfig {
    pub url: String,
    pub username: String,
    pub password: String,
}

#[derive(Clone)]
pub struct ClickhouseCollector {
    client: clickhouse::Client,
    data_tx: Sender<Payload>,
}

impl ClickhouseCollector {
    pub fn new(config: &ClickhouseCollectorConfig, data_tx: Sender<Payload>) -> Self {
        let client = clickhouse::Client::default()
            .with_url(&config.url)
            .with_user(&config.username)
            .with_password(&config.password);
            // .with_database("system");

        Self { client, data_tx }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        let _ = tokio::join!(
            tokio::task::spawn(self.clone().query_log_monitor()),
            tokio::task::spawn(self.clone().active_processes_monitor()),
        );
    }

    async fn query_log_monitor(self) {
        let empty_panel_info = PanelInfo {
            dashboard_id: "missing".to_owned(),
            panel_id: "missing".to_owned(),
            user_email: "missing".to_owned(),
        };
        let invalid_panel_info = PanelInfo {
            dashboard_id: "invalid".to_owned(),
            panel_id: "invalid".to_owned(),
            user_email: "invalid".to_owned(),
        };

        let mut clickhouse_queries_seq = 0;
        let mut offset = clock() as u64 - 15000;
        loop {
            let queries = self.client.query(FETCH_QUERY_LOG)
                .bind(offset)
                .fetch_all::<QueryLogEntry>().await
                .unwrap();
            dbg!(&queries);
            for event in queries {
                let mut payload = json!({
                    // "query_id": event.query_id,
                    // "db_user": event.db_user,
                    // "database": event.database,
                    // "table": event.table,
                    "finished_at": event.event_time,
                    "query_duration_ms": event.query_duration_ms,
                    "read_bytes": event.read_bytes,
                    "read_rows": event.read_rows,
                    "result_bytes": event.result_bytes,
                    "memory_usage": event.memory_usage,
                    // "event_type": event.event_type,
                    // "query_kind": event.query_kind,
                    // "error": event.error,
                    // "query": event.query,
                    // "interface": event.interface
                });
                {
                    // let panel_info = if event.log_comment.is_empty() {
                    //     empty_panel_info.clone()
                    // } else {
                    //     base64::decode(event.log_comment.as_bytes())
                    //         .context("invalid base64 in log_comment")
                    //         .and_then(|bytes| serde_json::from_slice::<PanelInfo>(bytes.as_slice())
                    //             .context("invalid json in log_comment"))
                    //         .unwrap_or_else(|_| invalid_panel_info.clone())
                    // };
                    // if let Value::Object(payload) = &mut payload {
                    //     payload.insert("user_email".to_owned(), Value::String(panel_info.user_email.clone()));
                    //     payload.insert("dashboard_id".to_owned(), Value::String(panel_info.dashboard_id.clone()));
                    //     payload.insert("panel_id".to_owned(), Value::String(panel_info.panel_id.clone()));
                    // }
                }
                let _ = self.data_tx.send_async(dbg!(Payload {
                    stream: "clickhouse_queries".to_string(),
                    sequence: clickhouse_queries_seq,
                    timestamp: clock() as _,
                    payload,
                })).await;
                clickhouse_queries_seq += 1;
                if event.event_time > offset {
                    offset = event.event_time;
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn active_processes_monitor(self) {
        let mut active_processes_seq = 0;
        loop {
            break;
        }
    }
}

// language=clickhouse
const FETCH_QUERY_LOG: &'static str = "
SELECT query_id, user AS db_user, arrayElement(databases, 1) AS database, arrayElement(tables, 1) AS table, arrayElement(views, 1) AS view,
       toUnixTimestamp64Milli(event_time_microseconds) AS event_time, query_duration_ms,
       read_bytes, read_rows, result_bytes, memory_usage,
       type AS event_type, log_comment, query_kind, exception AS error, query,
       multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', 'UNKNOWN') AS interface
FROM system.query_log
WHERE query NOT ILIKE '%system.query_log%'
  AND type != 'QueryStart'
  AND (event_date = today() OR event_date = yesterday())
  AND toUnixTimestamp64Milli(query_log.event_time_microseconds) > ?
ORDER BY event_time DESC
";

// language=clickhouse
const FETCH_PROCESSES_STATS: &'static str = "
SELECT count(*) as active_queries,
       max(elapsed) as longest_query_duration,
       argMax(query_id, elapsed) as longest_query_id,
       max(memory_usage) AS max_memory_usage,
       argMax(query_id, memory_usage) AS max_memory_query_id
FROM system.processes
";


#[derive(Deserialize, Row, Debug)]
struct QueryLogEntry {
    // pub query_id: String,
    // pub db_user: String,
    // pub database: String,
    // TODO: test these two
    // pub table: String,

    pub event_time: u64,
    pub query_duration_ms: u64,

    pub read_bytes: usize,
    pub read_rows: usize,
    pub result_bytes: usize,
    pub memory_usage: usize,

    // pub event_type: String,
    // pub query_kind: String,
    // pub error: String,
    // pub query: String,
    // pub interface: String,
    // pub log_comment: String,
}

#[derive(Deserialize, Clone)]
struct PanelInfo {
    dashboard_id: String,
    panel_id: String,
    user_email: String,
}
