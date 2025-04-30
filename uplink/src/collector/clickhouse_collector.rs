use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use anyhow::Context;
use flume::Sender;
use serde::Deserialize;
use serde_json::{json, Number, Value};
use time::format_description;
use tokio::time::sleep;
use crate::base::bridge::Payload;
use crate::base::clock;
use serde_with::{serde_as, DisplayFromStr};

#[derive(Clone, Debug, Deserialize)]
pub struct ClickhouseCollectorConfig {
    pub url: String,
    pub username: String,
    pub client_name: String,
    pub compose_file: String,
}

#[derive(Clone)]
pub struct ClickhouseCollector {
    http_client: reqwest::Client,
    data_tx: Sender<Payload>,
    password: String,
    config: ClickhouseCollectorConfig,
}

impl ClickhouseCollector {
    pub fn new(config: ClickhouseCollectorConfig, data_tx: Sender<Payload>) -> Self {
        let password = std::fs::read_to_string(&config.compose_file)
            .expect("couldn't read compose.yaml")
            .lines()
            .find(|line| line.contains("CLICKHOUSE_PASSWORD"))
            .expect("couldn't find CLICKHOUSE_PASSWORD in compose.yaml")
            .trim()
            .get(21..)
            .expect("invalid CLICKHOUSE_PASSWORD")
            .to_owned();
        // let password = "".to_owned();
        let http_client = reqwest::Client::new();

        Self { http_client, data_tx, config, password }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        let _ = tokio::join!(
            tokio::task::spawn(self.clone().query_log_monitor()),
            tokio::task::spawn(self.clone().active_processes_monitor()),
            tokio::task::spawn(self.clone().part_log_monitor()),
            tokio::task::spawn(self.clone().large_tables_monitor()),
            tokio::task::spawn(self.clone().fragmented_tables_monitor()),
            tokio::task::spawn(self.clone().query_delta_monitor()),
            // tokio::task::spawn(self.clone().monitor_log_tables()),
            // tokio::task::spawn(self.clone().monitor_snapshot_tables()),
        );
    }

    async fn query_log_monitor(self) {
        let empty_panel_info = PanelInfo {
            dashboard_id: Some("missing".to_owned()),
            panel_id: Some("missing".to_owned()),
            user_email: Some("missing".to_owned()),
        };
        let invalid_panel_info = PanelInfo {
            dashboard_id: Some("invalid".to_owned()),
            panel_id: Some("invalid".to_owned()),
            user_email: Some("invalid".to_owned()),
        };

        let mut clickhouse_queries_seq = 1;
        let mut offset = clock() as u64 * 1000;
        loop {
            sleep(Duration::from_secs(5)).await;
            let queries = match self.execute_query(&FETCH_QUERY_LOG.replace("?", &offset.to_string())).await {
                Ok(rows) => {
                    let mut result = Vec::new();
                    for row in rows.into_iter() {
                        match serde_json::from_str::<QueryLogEntry>(&row) {
                            Ok(row) => result.push(row),
                            Err(e) => {
                                log::error!("received invalid row when fetching query log: {row:?} {e:?}");
                                continue;
                            }
                        }
                    }
                    result
                },
                Err(e) => {
                    log::error!("couldn't fetch queries: {e:?}");
                    continue;
                }
            };
            for event in queries {
                if event.event_time_us > offset {
                    offset = event.event_time_us;
                }
                if event.read_bytes <= 1000 {
                    continue;
                }

                let mut payload = json!({
                    "query_id": event.query_id,
                    "db_user": event.db_user,
                    "database": event.database,
                    "table": event.table,
                    "query_duration_ms": event.query_duration_ms,
                    "read_bytes": event.read_bytes,
                    "read_rows": event.read_rows,
                    "result_bytes": event.result_bytes,
                    "memory_usage": event.memory_usage,
                    "event_type": event.event_type,
                    "query_kind": event.query_kind,
                    "query": event.query,
                    "interface": event.interface
                });
                {
                    let panel_info = if event.log_comment.is_empty() {
                        empty_panel_info.clone()
                    } else {
                        #[allow(deprecated)]
                        match base64::decode(event.log_comment.as_bytes())
                            .context("invalid base64 in log_comment")
                            .and_then(|bytes| serde_json::from_slice::<PanelInfo>(bytes.as_slice())
                                .context("invalid json in log_comment")) {
                            Ok(t) => t,
                            Err(_) => {
                                invalid_panel_info.clone()
                            }
                        }
                    };
                    if let Value::Object(payload) = &mut payload {
                        payload.insert("user_email".to_owned(), Value::String(panel_info.user_email.unwrap_or("missing".to_owned()).clone()));
                        payload.insert("dashboard_id".to_owned(), Value::String(panel_info.dashboard_id.unwrap_or("missing".to_owned()).clone()));
                        payload.insert("panel_id".to_owned(), Value::String(panel_info.panel_id.unwrap_or("missing".to_owned()).clone()));

                        payload.insert("error".to_owned(), if event.error == "" { Value::Null } else { Value::String(event.error) });
                    }
                }
                clickhouse_queries_seq += 1;
                let _ = self.data_tx.send_async(Payload {
                    stream: "clickhouse_queries".to_string(),
                    sequence: clickhouse_queries_seq,
                    timestamp: event.event_time_us / 1000,
                    payload,
                }).await;
            }
        }
    }

    async fn active_processes_monitor(self) {
        let mut active_processes_seq = 0;
        loop {
            sleep(Duration::from_secs(1)).await;
            match self.execute_query(FETCH_PROCESSES_STATS).await {
                Ok(rows) => {
                    if rows.len() == 0 {
                        log::error!("fetch processes stats query returned nothing!");
                        continue;
                    }
                    let row = rows.get(0).unwrap().as_str();
                    match serde_json::from_str::<ProcessesSnapshot>(row) {
                        Ok(stats) => {
                            active_processes_seq += 1;
                            let payload = Payload {
                                stream: "clickhouse_processes".to_owned(),
                                sequence: active_processes_seq,
                                timestamp: clock() as _,
                                payload: json!({
                                    "processes_count": stats.processes_count,
                                    "max_elapsed_ms": (stats.max_elapsed_secs * 1000.0) as u64,
                                    "max_elapsed_query_id": stats.max_elapsed_query_id,
                                    "total_memory_usage": stats.total_memory_usage,
                                    "max_memory_usage": stats.max_memory_usage,
                                    "max_memory_query_id": stats.max_memory_query_id,
                                }),
                            };
                            let _ = self.data_tx.send_async(payload).await;
                        }
                        Err(e) => {
                            log::error!("received invalid json when fetching processes stats: {row:?} {e:?}");
                        }
                    }
                }
                Err(e) => {
                    log::error!("couldn't fetch processes stats: {e:?}");
                    continue;
                }
            }
        }
    }

    async fn large_tables_monitor(self) {
        let mut sequence = 0;
        loop {
            match self.execute_query(FETCH_LARGE_TABLES).await {
                Ok(rows) => {
                    let mut t_idx = 0;
                    let mut payload = serde_json::Map::new();
                    for row in rows {
                        match serde_json::from_str::<PartsTableInfo>(&row) {
                            Ok(pti) => {
                                sequence += 1;
                                t_idx += 1;
                                payload.insert(format!("table_{}", t_idx), Value::String(format!("{}.`{}`", pti.database, pti.table)));
                                payload.insert(format!("table_{}_size", t_idx), Value::Number(Number::from_i128(pti.size_on_disk as _).unwrap()));
                                payload.insert(format!("table_{}_parts_count", t_idx), Value::Number(Number::from_i128(pti.parts_count as _).unwrap()));
                                payload.insert(format!("table_{}_max_parts_count", t_idx), Value::Number(Number::from_i128(pti.max_parts_count as _).unwrap()));
                                payload.insert(format!("table_{}_parts_fraction", t_idx), Value::Number(Number::from_f64(pti.parts_fraction as _).unwrap()));
                            }
                            Err(_) => {
                                log::error!("received invalid json from clickhouse: {row:?}");
                            }
                        }
                    }
                    let payload = Payload {
                        stream: "clickhouse_large_tables".to_owned(),
                        sequence,
                        timestamp: clock() as _,
                        payload: Value::Object(payload),
                    };
                    let _ = self.data_tx.send_async(payload).await;
                }
                Err(e) => {
                    log::error!("couldn't fetch table size stats: {e:?}");
                }
            }
            sleep(Duration::from_secs(60)).await;
        }
    }

    async fn fragmented_tables_monitor(self) {
        let mut sequence = 0;
        loop {
            match self.execute_query(FETCH_FRAGMENTED_TABLES).await {
                Ok(rows) => {
                    let mut t_idx = 0;
                    let mut payload = serde_json::Map::new();
                    for row in rows {
                        match serde_json::from_str::<PartsTableInfo>(&row) {
                            Ok(pti) => {
                                sequence += 1;
                                t_idx += 1;
                                payload.insert(format!("table_{}", t_idx), Value::String(format!("{}.`{}`", pti.database, pti.table)));
                                payload.insert(format!("table_{}_size", t_idx), Value::Number(Number::from_i128(pti.size_on_disk as _).unwrap()));
                                payload.insert(format!("table_{}_parts_count", t_idx), Value::Number(Number::from_i128(pti.parts_count as _).unwrap()));
                                payload.insert(format!("table_{}_max_parts_count", t_idx), Value::Number(Number::from_i128(pti.max_parts_count as _).unwrap()));
                                payload.insert(format!("table_{}_parts_fraction", t_idx), Value::Number(Number::from_f64(pti.parts_fraction as _).unwrap()));
                            }
                            Err(_) => {
                                log::error!("received invalid json from clickhouse: {row:?}");
                            }
                        }
                    }
                    let payload = Payload {
                        stream: "clickhouse_fragmented_tables".to_owned(),
                        sequence,
                        timestamp: clock() as _,
                        payload: Value::Object(payload),
                    };
                    let _ = self.data_tx.send_async(payload).await;
                }
                Err(e) => {
                    log::error!("couldn't fetch table size stats: {e:?}");
                }
            }
            sleep(Duration::from_secs(60)).await;
        }
    }

    async fn part_log_monitor(self) {
        let mut sequence = 1;
        let mut offset = clock() as u64 - 10000;
        loop {
            sleep(Duration::from_secs(30)).await;
            let merges = match self.execute_query(&FETCH_MERGE_INFO.replace("?", &offset.to_string())).await {
                Ok(rows) => {
                    let mut result = Vec::new();
                    for row in rows.into_iter() {
                        match serde_json::from_str::<PartLogRow>(&row) {
                            Ok(row) => result.push(row),
                            Err(e) => {
                                log::error!("received invalid row when fetching merge info: {row:?} {e:?}");
                                continue;
                            }
                        }
                    }
                    result
                }
                Err(e) => {
                    log::error!("couldn't fetch queries: {e:?}");
                    continue;
                }
            };

            let mut new_parts = 0;
            let mut merges_count = 0;
            let mut mutations_count = 0;
            let mut max_memory_usage = 0;
            let mut max_memory_query_id = "";
            let mut max_duration = 0;
            let mut max_duration_query_id = "";
            let mut max_size = 0;
            let mut max_size_query_id = "";
            let mut max_read_bytes = 0;
            let mut max_read_bytes_query_id = "";
            let mut total_read_bytes = 0;
            for merge in merges.iter() {
                if merge.event_time_ms > offset {
                    offset = merge.event_time_ms;
                }
                match merge.event_type {
                    1 => new_parts += 1,
                    2 => merges_count += 1,
                    5 => mutations_count += 1,
                    _ => {}
                }
                if merge.peak_memory_usage > max_memory_usage {
                    max_memory_usage = merge.peak_memory_usage;
                    max_memory_query_id = &merge.query_id;
                }
                if merge.duration_ms > max_duration {
                    max_duration = merge.duration_ms;
                    max_duration_query_id = &merge.query_id;
                }
                if merge.size_in_bytes > max_size {
                    max_size = merge.size_in_bytes;
                    max_size_query_id = &merge.query_id;
                }
                if merge.read_bytes > max_read_bytes {
                    max_read_bytes = merge.read_bytes;
                    max_read_bytes_query_id = &merge.query_id;
                }
                total_read_bytes += merge.read_bytes;
            }
            sequence += 1;
            let payload = Payload {
                stream: "clickhouse_part_logs".to_owned(),
                sequence,
                timestamp: clock() as _,
                payload: json!({
                    "new_parts": new_parts,
                    "merges": merges_count,
                    "mutations": mutations_count,
                    "max_memory_usage": max_memory_usage,
                    "max_memory_query_id": max_memory_query_id,
                    "max_duration": max_duration,
                    "max_duration_query_id": max_duration_query_id,
                    "max_size": max_size,
                    "max_size_query_id": max_size_query_id,
                    "max_read_bytes": max_read_bytes,
                    "max_read_bytes_query_id": max_read_bytes_query_id,
                    "total_read_bytes": total_read_bytes,
                }),
            };
            let _ = self.data_tx.send_async(payload).await;
        }
    }

    async fn query_delta_monitor(self) {
        let mut sequence = 0;
        loop {
            let now = clock() as u64;
            let window_start = now - 60000;

            let query = FETCH_QUERY_COUNTS.replace("?", &window_start.to_string());
            match self.execute_query(&query).await {
                Ok(rows) => {
                    if let Some(row) = rows.first() {
                        match serde_json::from_str::<QueryCounts>(row) {
                            Ok(counts) => {
                                sequence += 1;
                                let backlog = counts.started_count.saturating_sub(counts.finished_count);

                                let payload = Payload {
                                    stream: "clickhouse_query_delta".to_owned(),
                                    sequence,
                                    timestamp: now,
                                    payload: json!({
                                        "query_delta": backlog,
                                    }),
                                };
                                let _ = self.data_tx.send_async(payload).await;
                            },
                            Err(e) => {
                                log::error!("couldn't parse query counts: {e:?}");
                            }
                        }
                    } else {
                        log::error!("no query counts returned");
                    }
                }
                Err(e) => {
                    log::error!("couldn't fetch query counts: {e:?}");
                }
            }
            sleep(Duration::from_secs(60)).await;
        }
    }

    async fn monitor_log_tables(self) {
        #[derive(Default)]
        struct TableState {
            name: &'static str,
            filter: &'static str,
            columns: &'static str,
            offset: u64,
            sequence: u32,
        }
        let beginning = clock() as u64 - 10000;
        let mut tables = vec![
            TableState { name: "metric_log", filter: "TRUE", columns: "event_time, event_date, ProfileEvent_Query, ProfileEvent_OSCPUVirtualTimeMicroseconds, CurrentMetric_Query, CurrentMetric_Merge, ProfileEvent_SelectedBytes, ProfileEvent_OSIOWaitMicroseconds, ProfileEvent_OSCPUWaitMicroseconds, ProfileEvent_OSReadBytes, ProfileEvent_OSReadChars, CurrentMetric_MemoryTracking, ProfileEvent_SelectedRows, ProfileEvent_InsertedRows, ProfileEvent_ReadBufferFromS3Microseconds, ProfileEvent_ReadBufferFromS3RequestsErrors, ProfileEvent_ReadBufferFromS3Bytes, CurrentMetric_FilesystemCacheSize, ProfileEvent_DiskS3PutObject, ProfileEvent_DiskS3UploadPart, ProfileEvent_DiskS3CreateMultipartUpload, ProfileEvent_DiskS3CompleteMultipartUpload, ProfileEvent_DiskS3GetObject, ProfileEvent_DiskS3HeadObject, ProfileEvent_DiskS3ListObjects, ProfileEvent_CachedReadBufferReadFromCacheBytes, ProfileEvent_CachedReadBufferReadFromSourceBytes, CurrentMetric_TCPConnection, CurrentMetric_MySQLConnection, CurrentMetric_HTTPConnection, CurrentMetric_InterserverConnection, hostname, ProfileEvent_Query, CurrentMetric_Query, ProfileEvent_QueryTimeMicroseconds, ProfileEvent_OSCPUWaitMicroseconds, ProfileEvent_QueryMemoryLimitExceeded", offset: beginning, ..Default::default() },
            TableState { name: "query_log", filter: "read_bytes > 100000 OR exception != '' OR type > 2 OR query_duration_ms > 1000", columns: "tables, type, event_time, query_duration_ms, query_id, query, read_bytes, current_database, query_kind, exception_code, exception, stack_trace, databases, written_rows, memory_usage", offset: beginning, ..Default::default() },
            TableState { name: "part_log", filter: "TRUE", columns: "query_id, event_type, merge_algorithm, event_time, database, table, part_name, part_type, disk_name, rows, error, exception", offset: beginning, ..Default::default() },
        ];

        loop {
            for table in tables.iter_mut() {
                let select_clause = table.columns;
                let query = format!(
                    "SELECT {}, toUnixTimestamp64Milli(event_time_microseconds) as event_time_millis FROM system.{} WHERE event_time_microseconds > toDateTime64({} / 1000.0, 3) AND ({})",
                    select_clause, table.name, table.offset, table.filter
                );
                match self.execute_query(&query).await
                {
                    Ok(rows) => {
                        for row in rows {
                            let json: Value = serde_json::from_str(&row).unwrap();
                            let event_time = json["event_time_millis"].as_str()
                                .and_then(|s| s.parse().ok())
                                .unwrap();
                            if event_time > table.offset {
                                table.offset = event_time;
                            }
                            table.sequence += 1;
                            let payload = Payload {
                                stream: format!("{}_system_{}", self.config.client_name, table.name),
                                sequence: table.sequence,
                                timestamp: event_time,
                                payload: json,
                            };
                            let _ = self.data_tx.send_async(payload).await;
                        }
                    }
                    Err(e) => {
                        log::error!("couldn’t fetch system.{}:\nquery: {query:?}\nerror: {e:?}", table.name);
                    }
                }
            }
            sleep(Duration::from_secs(30)).await;
        }
    }

    async fn monitor_snapshot_tables(self) {
        struct TableState {
            name: &'static str,
            columns: &'static str,
            interval: u64,
            sequence: u32,
            last_push_at: SystemTime,
        }
        let now = SystemTime::now() - Duration::from_secs(99999999);
        let mut tables = vec![
            TableState { name: "tables", columns: "database, table, create_table_query, total_rows", interval: 60 * 15, sequence: 0, last_push_at: now },
            TableState { name: "parts", columns: "modification_time, table, database, active", interval: 60, sequence: 0, last_push_at: now },
            TableState { name: "disks", columns: "total_space, free_space, keep_free_space", interval: 3600, sequence: 0, last_push_at: now },
            TableState { name: "merge_tree_settings", columns: "value, name", interval: 60 * 10, sequence: 0, last_push_at: now },
            TableState { name: "asynchronous_metrics", columns: "value, metric", interval: 60, sequence: 0, last_push_at: now },
            TableState { name: "macros", columns: "macro, substitution", interval: 60 * 5, sequence: 0, last_push_at: now },
            TableState { name: "dashboards", columns: "*", interval: 3600, sequence: 0, last_push_at: now },
            TableState { name: "mutations", columns: "*", interval: 3, sequence: 0, last_push_at: now },
            TableState { name: "processes", columns: "*", interval: 3, sequence: 0, last_push_at: now },
        ];

        loop {
            for table in tables.iter_mut() {
                let now = SystemTime::now();
                if now.duration_since(table.last_push_at)
                    .map(|d| d > Duration::from_secs(table.interval))
                    .unwrap_or(true) {
                    table.last_push_at = now;
                    let query = format!("SELECT {} FROM system.{}", table.columns, table.name);
                    match self.execute_query(&query).await
                    {
                        Ok(rows) => {
                            let snapshot_time = clock() as u64 / 1000;
                            for row in rows {
                                let mut json: Value = serde_json::from_str(&row).unwrap();
                                json["snapshot_time"] = json!(snapshot_time);
                                table.sequence += 1;
                                let payload = Payload {
                                    stream: format!("{}_system_{}", self.config.client_name, table.name),
                                    sequence: table.sequence,
                                    timestamp: snapshot_time,
                                    payload: json,
                                };
                                let _ = self.data_tx.send_async(payload).await;
                            }
                        }
                        Err(e) => {
                            log::error!("couldn’t fetch system.{}: {:?}", table.name, e);
                        }
                    }
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn execute_query(&self, query: &str) -> Result<Vec<String>, String> {
        let res = self.http_client
            .post(&self.config.url)
            .basic_auth(&self.config.username, Some(&self.password))
            .body(format!("{query} FORMAT JSONEachRow settings log_queries=0,log_profile_events=0"))
            .send()
            .await
            .map_err(|e| format!("Request error: {}", e))?;

        let status = res.status();
        let body = res.text().await.map_err(|e| format!("Failed to read response body: {}", e))?;

        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status.as_u16(), body));
        }

        Ok(body.lines().map(|s| s.to_owned()).collect())
    }
}

// language=clickhouse
const FETCH_QUERY_LOG: &'static str = "
SELECT query_id,
       user AS db_user,
       arrayElement(databases, 1) AS database,
       arrayElement(tables, 1) AS table,
       toUnixTimestamp64Micro(event_time_microseconds) AS event_time_us,
       query_duration_ms,
       read_bytes, read_rows, result_bytes, memory_usage,
       toString(type) AS event_type,
       log_comment,
       query_kind,
       exception AS error,
       query,
       multiIf(interface = 1, 'TCP', interface = 2, 'HTTP', 'UNKNOWN') AS interface
FROM system.query_log
WHERE database != 'system'
  AND type != 'QueryStart'
  AND (query_duration_ms > 100 OR read_bytes > 10240000)
  AND (event_date = today() OR event_date = yesterday())
  AND toUnixTimestamp64Micro(query_log.event_time_microseconds) > ?
ORDER BY event_time_microseconds
";

#[serde_as]
#[derive(Deserialize, Debug)]
struct QueryLogEntry {
    pub query_id: String,
    pub db_user: String,
    pub database: String,
    pub table: String,

    #[serde_as(as = "DisplayFromStr")]
    pub event_time_us: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub query_duration_ms: u64,

    #[serde_as(as = "DisplayFromStr")]
    pub read_bytes: usize,
    #[serde_as(as = "DisplayFromStr")]
    pub read_rows: usize,
    #[serde_as(as = "DisplayFromStr")]
    pub result_bytes: usize,
    #[serde_as(as = "DisplayFromStr")]
    pub memory_usage: usize,

    pub event_type: String,
    pub log_comment: String,
    pub query_kind: String,
    pub error: String,
    pub query: String,
    pub interface: String,
}

// language=clickhouse
const FETCH_PROCESSES_STATS: &'static str = "
SELECT count(*) as processes_count,
       max(elapsed) as max_elapsed_secs,
       argMax(query_id, elapsed) as max_elapsed_query_id,
       sum(memory_usage) AS total_memory_usage,
       max(memory_usage) AS max_memory_usage,
       argMax(query_id, memory_usage) AS max_memory_query_id
FROM system.processes
WHERE query NOT LIKE '%FROM system.processes%'
";

#[serde_as]
#[derive(Deserialize, Debug)]
struct ProcessesSnapshot {
    #[serde_as(as = "DisplayFromStr")]
    pub processes_count: u64,
    pub max_elapsed_secs: f64,
    pub max_elapsed_query_id: String,
    #[serde_as(as = "DisplayFromStr")]
    pub total_memory_usage: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub max_memory_usage: u64,
    pub max_memory_query_id: String,
}

// language=clickhouse
const FETCH_LARGE_TABLES: &'static str = "
SELECT
    database, table,
    sum(bytes_on_disk) AS size_on_disk,
    count(*) AS parts_count,
    (SELECT IF(toInt32OrZero(value) = 0, 3000, toInt32OrZero(value))
                   FROM system.merge_tree_settings
                   WHERE name = 'parts_to_throw_insert') AS max_parts_count,
    parts_count / max_parts_count AS parts_fraction
FROM system.parts
WHERE active AND database != 'system'
GROUP BY database, table
ORDER BY size_on_disk DESC
LIMIT 5
";

#[serde_as]
#[derive(Deserialize, Debug)]
struct PartsTableInfo {
    pub database: String,
    pub table: String,
    #[serde_as(as = "DisplayFromStr")]
    pub size_on_disk: i64,
    #[serde_as(as = "DisplayFromStr")]
    pub parts_count: i64,
    pub max_parts_count: i64,
    pub parts_fraction: f64,
}

// language=clickhouse
const FETCH_FRAGMENTED_TABLES: &str = "
SELECT
    database, table,
    sum(bytes_on_disk) AS size_on_disk,
    count(*) AS parts_count,
    (SELECT IF(toInt32OrZero(value) = 0, 3000, toInt32OrZero(value))
                   FROM system.merge_tree_settings
                   WHERE name = 'parts_to_throw_insert') AS max_parts_count,
    parts_count / max_parts_count AS parts_fraction
FROM system.parts
WHERE active AND database != 'system'
GROUP BY database, table
ORDER BY parts_fraction DESC
LIMIT 5
";

// language=clickhouse
const FETCH_MERGE_INFO: &'static str = "
SELECT query_id, event_type, toUnixTimestamp64Milli(event_time_microseconds) as event_time_ms, duration_ms, size_in_bytes, read_bytes, peak_memory_usage
FROM system.part_log
WHERE toUnixTimestamp64Milli(event_time_microseconds) > ?
";

#[derive(Deserialize, Debug)]
struct PartLogRow {
    pub query_id: String,
    pub event_type: u8,
    pub event_time_ms: u64,
    pub duration_ms: u64,
    pub size_in_bytes: u64,
    pub read_bytes: u64,
    pub peak_memory_usage: u64,
}

#[derive(Deserialize, Clone, Debug)]
struct PanelInfo {
    dashboard_id: Option<String>,
    panel_id: Option<String>,
    user_email: Option<String>,
}


// language=clickhouse
const FETCH_QUERY_COUNTS: &'static str = "
SELECT
    countIf(type = 'QueryStart') AS started_count,
    countIf(type != 'QueryStart') AS finished_count
FROM system.query_log
WHERE (event_date = today() OR event_date = yesterday())
  AND toUnixTimestamp64Milli(event_time_microseconds) >= ?
  AND query NOT ILIKE '%system.query_log%'
";

#[serde_as]
#[derive(Deserialize, Debug)]
struct QueryCounts {
    #[serde_as(as = "DisplayFromStr")]
    pub started_count: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub finished_count: u64,
}

#[test]
fn t1() {
    let row = "{\"query_id\":\"50177688-4c54-4467-a6b1-7b0dca253bdf\",\"db_user\":\"freshbus_user\",\"database\":\"system\",\"table\":\"system.one\",\"event_time_us\":\"1745926178139679\",\"query_duration_ms\":\"42\",\"read_bytes\":\"1567994\",\"read_rows\":\"7099\",\"result_bytes\":\"2005\",\"memory_usage\":\"4200855\",\"event_type\":\"QueryFinish\",\"log_comment\":\"eyJkYXNoYm9hcmRfaWQiOiIxMDA4IiwicGFuZWxfaWQiOiIwZmE5NTg5Ml9jMDI0XzQyMmRfOTAxOF8wOWYwOTUwNjlmYWQiLCJ1c2VyX2VtYWlsIjoic3VuZWV0aGFAbWFydmVsZXZtLmluIn0=\",\"query_kind\":\"Select\",\"error\":\"\",\"query\":\"select currentUser() user, timezone() timezone, version() version, toUInt8(ifnull((select value from system.settings where name = 'readonly'), '0')) as readonly, toInt8(ifnull((select value from system.settings where name = 'throw_on_unsupported_query_inside_transaction'), '-1')) as throw_on_unsupported_query_inside_transaction, (ifnull((select value from system.settings where name = 'wait_changes_become_visible_after_commit_mode'), '')) as wait_changes_become_visible_after_commit_mode,toInt8(ifnull((select value from system.settings where name = 'implicit_transaction'), '-1')) as implicit_transaction, toUInt64(ifnull((select value from system.settings where name = 'max_insert_block_size'), '0')) as max_insert_block_size, toInt8(ifnull((select value from system.settings where name = 'allow_experimental_lightweight_delete'), '-1')) as allow_experimental_lightweight_delete, (ifnull((select value from system.settings where name = 'custom_jdbc_config'), '')) as custom_jdbc_config FORMAT RowBinaryWithNamesAndTypes\",\"interface\":\"HTTP\"}";
    dbg!(serde_json::from_str::<QueryLogEntry>(row));
}