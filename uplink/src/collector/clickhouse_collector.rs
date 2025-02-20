use std::time::Duration;
use anyhow::Context;
use clickhouse::Row;
use flume::Sender;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde_json::{json, Map, Number, Value};
use time::OffsetDateTime;
use tokio::time::sleep;
use crate::base::bridge::Payload;
use crate::base::clock;

pub struct ClickhouseCollectorConfig {
    pub url: String,
    pub username: String,
    pub password: String,
}

pub struct ClickhouseCollector {
    config: ClickhouseCollectorConfig,
    client: clickhouse::Client,
    data_tx: Sender<Payload>,
}

impl ClickhouseCollector {
    pub fn new(config: ClickhouseCollectorConfig, data_tx: Sender<Payload>) -> Self {
        let client = clickhouse::Client::default()
            .with_url(&config.url)
            .with_user(&config.username)
            .with_password(&config.password)
            .with_database("system");

        Self { config, client, data_tx }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        tokio::task::spawn(self.query_log_monitor());
        tokio::task::spawn(self.active_processes_monitor());
    }

    async fn query_log_monitor(&self) {
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
        let mut offset = OffsetDateTime::now_utc();
        loop {
            let queries = self.client.query(
                &FETCH_QUERY_LOG.replace("%OFFSET%", &offset.to_string())
            ).fetch_all::<QueryLogEntry>().await.unwrap();
            for event in queries {
                let mut payload = json!({
                    "query_id": event.query_id,
                    "db_user": event.db_user,
                    "database": event.database,
                    "table": event.table,
                    "event_time": event.event_time,
                    "query_start_time": event.query_start_time,
                    "query_duration_ms": event.query_duration_ms,
                    "read_bytes": event.read_bytes,
                    "read_rows": event.read_rows,
                    "result_bytes": event.result_bytes,
                    "memory_usage": event.memory_usage,
                    "event_type": event.event_type,
                    "query_kind": event.query_kind,
                    "error": event.error,
                    "query": event.query,
                    "interface": event.interface
                });
                {
                    let panel_info = if event.log_comment.is_empty() {
                        empty_panel_info.clone()
                    } else {
                        base64::decode(event.log_comment.as_bytes())
                            .context("invalid base64 in log_comment")
                            .and_then(|bytes| serde_json::from_slice::<PanelInfo>(bytes.as_slice())
                                .context("invalid json in log_comment"))
                            .unwrap_or_else(|_| invalid_panel_info.clone())
                    };
                    if let Value::Object(payload) = &mut payload {
                        payload.insert("user_email".to_owned(), Value::String(panel_info.user_email.clone()));
                        payload.insert("dashboard_id".to_owned(), Value::String(panel_info.dashboard_id.clone()));
                        payload.insert("panel_id".to_owned(), Value::String(panel_info.panel_id.clone()));
                    }
                }
                let _ = self.data_tx.send_async(Payload {
                    stream: "clickhouse_queries".to_string(),
                    sequence: clickhouse_queries_seq,
                    timestamp: clock() as _,
                    payload,
                }).await;
                clickhouse_queries_seq += 1;
                if event.event_time > offset {
                    offset = event.event_time;
                }
            }
            sleep(Duration::from_secs(30)).await;
        }
    }

    async fn active_processes_monitor(&self) {
        let mut active_processes_seq = 0;
        loop {
        }
    }
}

// language=clickhouse
const FETCH_QUERY_LOG: &'static str = "
SELECT query_id, user AS db_user, arrayElement(databases, 1) AS database, arrayElement(tables, 1) AS table, arrayElement(views, 1) AS view,
       event_time, query_start_time, query_duration_ms,
       read_bytes, read_rows, result_bytes, memory_usage,
       type AS event_type, log_comment, query_kind, exception AS error, query,
       multiIf(interface = 1, 'TCP', interface = 2, 'HTTP') AS interface
FROM system.query_log
WHERE query LIKE '%BYTEBEAM_USER_ID=%'
  AND query NOT ILIKE '%system.query_log%'
  AND type != 'QueryStart'
  AND (event_date = today() OR event_date = yesterday())
  AND query_log.event_time_microseconds > now() - toDateTime64('%OFFSET%', 3)
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


#[derive(Deserialize, Row)]
struct QueryLogEntry {
    pub query_id: String,
    pub db_user: String,
    pub database: String,
    // TODO: test these two
    pub table: String,
    pub view: String,

    pub event_time: OffsetDateTime,
    pub query_start_time: OffsetDateTime,
    pub query_duration_ms: u64,

    pub read_bytes: usize,
    pub read_rows: usize,
    pub result_bytes: usize,
    pub memory_usage: usize,

    pub event_type: String,
    pub log_comment: String,
    pub query_kind: String,
    pub error: String,
    pub query: String,
    pub interface: String,
}

#[derive(Deserialize, Clone)]
struct PanelInfo {
    dashboard_id: String,
    panel_id: String,
    user_email: String,
}
