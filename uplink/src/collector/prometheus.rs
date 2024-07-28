use std::{collections::HashMap, time::Duration};

use log::error;
use reqwest::{Client, Method};
use serde_json::{json, Map, Value};

use crate::{
    base::{
        bridge::{BridgeTx, Payload},
        clock,
    },
    config::PrometheusConfig,
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
        loop {
            if let Err(e) = self.query().await {
                error!("{e}")
            }

            tokio::time::sleep(Duration::from_secs(self.config.interval)).await;
        }
    }

    async fn query(&self) -> Result<(), Error> {
        let resp = self.client.request(Method::GET, &self.config.endpoint).send().await?;
        let text = resp.text().await?;

        for payload in read_prom(&text) {
            self.tx.send_payload(payload).await;
        }

        Ok(())
    }
}

fn read_prom(text: &str) -> Vec<Payload> {
    let mut groups = HashMap::new();
    let mut ungrouped = HashMap::new();

    let lines = text.lines();
    for line in lines {
        let line = line.trim();
        // skip the line that starts with #
        if line.is_empty() || line.trim_start().starts_with('#') {
            continue;
        }

        // If metrics have same label, e.g. {node=1,io=0},
        // then we will group them together.
        // if there are no labels, each line will be treated as new Payload
        if let Some((label, name, value)) = frame_metric(line) {
            if let Some(label) = label {
                groups.entry(label).or_insert_with(Map::new).insert(name, json!(value));
            } else {
                let mut m = serde_json::Map::new();
                m.insert(name.clone(), json!(value));
                ungrouped.insert(name, m);
            }
        }
    }

    groups
        .into_iter()
        .map(|(label, mut payload)| {
            let mut node_and_id = create_map(&label);

            // check if node_and_id contains io or compute
            // and determine stream name based on it!
            let stream = if node_and_id.contains_key("io_id") {
                "io_metrics"
            } else if node_and_id.contains_key("compute_id") {
                "compute_metrics"
            } else {
                // TODO: add list of other modules
                // - what shall be the behaviour if none of the modules match?
                // - can we get this from config?
                "metrics"
            };

            payload.append(&mut node_and_id);

            (stream.to_owned(), payload)
        })
        .chain(ungrouped)
        .map(|(stream, payload)| Payload {
            stream,
            sequence: 0,
            timestamp: clock() as u64,
            payload: payload.into(),
        })
        .collect()
}

fn frame_metric(line: &str) -> Option<(Option<String>, String, f64)> {
    let mut tokens = line.split_whitespace();
    let metric_and_labels = tokens.next()?;
    let value = tokens.next()?.parse().ok()?;

    let mut metric_iter = metric_and_labels.split('{');
    let metric_name = metric_iter.next()?.to_owned();

    let labels = metric_iter.next().map(|m| m.trim_end_matches('}').to_owned());

    // NOTE: as we are grouping metrics, timestamps will be discarded!
    // we are discarding even if label isn't there ( no grouping )
    // to be consistent
    // if let Some(timestamp) = tokens.next() {
    //     let timestamp = timestamp.parse::<u64>().ok()?;
    //     payload.timestamp = timestamp;
    // }

    Some((labels, metric_name, value))
}

// Create a function with impl Iterator over &str
fn _frame_payload<'a>(mut line: impl Iterator<Item = &'a str>) -> Option<Payload> {
    // split at the first { to get stream and payload
    let stream_and_payload = line.next()?;

    // split without removing the delimiter
    let mut tokens = stream_and_payload.split('{');
    let stream = tokens.next()?.to_owned();

    let mut payload = if tokens.next().is_some() {
        let payload = &stream_and_payload[stream.len()..];
        create_map(payload)
    } else {
        serde_json::Map::new()
    };

    let value = line.next()?;
    let value = value.parse::<f64>().ok()?;
    payload.insert(stream.to_owned(), json!(value));

    let mut payload =
        Payload { stream, sequence: 0, timestamp: clock() as u64, payload: payload.into() };

    if let Some(timestamp) = line.next() {
        let timestamp = timestamp.parse::<u64>().ok()?;
        payload.timestamp = timestamp;
    }

    Some(payload)
}

fn create_map(line: &str) -> Map<String, Value> {
    let v: Vec<&str> = line.trim_matches(|c| c == '{' || c == '}').split(',').collect();

    let mut map = serde_json::Map::new();
    for item in v {
        let parts: Vec<&str> = item.split('=').collect();
        if parts.len() == 2 {
            // NOTE: shall we strip quotes (") from parts[1]?
            map.insert(parts[0].to_string(), json!(parts[1]));
        }
    }

    map
}

#[cfg(test)]
mod test {
    use super::{_frame_payload, read_prom};

    #[test]
    fn ghz_metrics() {
        let scrape = r#"
            # HELP metrics_ghz_active_connections Number of current active connections.
            # TYPE metrics_ghz_active_connections gauge
            metrics_ghz_active_connections{io_id="0"} 0
            # HELP metrics_ghz_incoming_data Total incoming data processed.
            # TYPE metrics_ghz_incoming_data counter
            metrics_ghz_incoming_data_total{io_id="0"} 143900
            # HELP metrics_ghz_incoming_publish Total incoming publish packets.
            # TYPE metrics_ghz_incoming_publish counter
            metrics_ghz_incoming_publish_total{io_id="0"} 90
            # HELP metrics_ghz_incoming_data_throughput Throughput of incoming data processed by IO.
            # TYPE metrics_ghz_incoming_data_throughput gauge
            metrics_ghz_incoming_data_throughput{io_id="0"} 0.0
            # HELP metrics_ghz_outgoing_data Total outgoing data processed.
            # TYPE metrics_ghz_outgoing_data counter
            metrics_ghz_outgoing_data_total{io_id="0"} 1080
            # HELP metrics_ghz_outgoing_data_throughput Throughput of outgoing data processed by IO.
            # TYPE metrics_ghz_outgoing_data_throughput gauge
            metrics_ghz_outgoing_data_throughput{io_id="0"} 0.0
            # HELP metrics_ghz_compute_data_processed Total data processed by compute.
            # TYPE metrics_ghz_compute_data_processed counter
            metrics_ghz_compute_data_processed_total{compute_id="0"} 0
            metrics_ghz_compute_data_processed_total{compute_id="1"} 0
            # HELP metrics_ghz_compute_data_throughput Throughput of data processed by compute.
            # TYPE metrics_ghz_compute_data_throughput gauge
            metrics_ghz_compute_data_throughput{compute_id="1"} 0.0
            metrics_ghz_compute_data_throughput{compute_id="0"} 0.0
            # HELP metrics_ghz_storage_data_processed Total data processed by storage.
            # TYPE metrics_ghz_storage_data_processed counter
            metrics_ghz_storage_data_processed_total 156463
            # EOF
        "#;
        dbg!(read_prom(scrape));
    }

    #[test]
    fn parse_prom_to_payload() {
        let scrape = r#"
            # HELP http_requests_total The total number of HTTP requests.
            # TYPE http_requests_total counter
            http_requests_total{method="post",code="200"} 1027 1395066363000
            http_requests_total{method="post",code="400"}    3 1395066363000

            # Escaping in label values:
            # msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9

            # Minimalistic line:
            metric_without_timestamp_and_labels 12.47

            # A weird metric from before the epoch:
            # something_weird{problem="division by zero"} +Inf -3982045

            # A histogram, which has a pretty complex representation in the text format:
            # HELP http_request_duration_seconds A histogram of the request duration.
            # TYPE http_request_duration_seconds histogram
            http_request_duration_seconds_bucket{le="0.05"} 24054
            http_request_duration_seconds_bucket{le="0.1"} 33444
            http_request_duration_seconds_bucket{le="0.2"} 100392
            http_request_duration_seconds_bucket{le="0.5"} 129389
            http_request_duration_seconds_bucket{le="1"} 133988
            http_request_duration_seconds_bucket{le="+Inf"} 144320
            http_request_duration_seconds_sum 53423
            http_request_duration_seconds_count 144320

            # Finally a summary, which has a complex representation, too:
            # HELP rpc_duration_seconds A summary of the RPC duration in seconds.
            # TYPE rpc_duration_seconds summary
            rpc_duration_seconds{quantile="0.01"} 3102
            rpc_duration_seconds{quantile="0.05"} 3272
            rpc_duration_seconds{quantile="0.5"} 4773
            rpc_duration_seconds{quantile="0.9"} 9001
            rpc_duration_seconds{quantile="0.99"} 76656
            rpc_duration_seconds_sum 1.7560473e+07
            rpc_duration_seconds_count 2693

    # A histogram, which has a pretty complex representation in the text format:
    # HELP http_request_duration_seconds A histogram of the request duration.
    # TYPE http_request_duration_seconds histogram
    http_request_duration_seconds_bucket{service="main",code="200",le="0.05"} 24054 1395066363000
    http_request_duration_seconds_bucket{code="200",le="0.1",service="main"} 33444 1395066363000
    http_request_duration_seconds_bucket{code="200",service="main",le="0.2"} 100392 1395066363000
    http_request_duration_seconds_bucket{le="0.5",code="200",service="main"} 129389 1395066363000
    http_request_duration_seconds_bucket{service="main",le="1",code="200"} 133988 1395066363000
    http_request_duration_seconds_bucket{le="+Inf",service="main",code="200"} 144320 1395066363000
    http_request_duration_seconds_sum{service="main",code="200"} 53423 1395066363000
    http_request_duration_seconds_count{service="main",code="200"} 144320 1395066363000

    # Finally a summary, which has a complex representation, too:
    # HELP rpc_duration_seconds A summary of the RPC duration in seconds.
    # TYPE rpc_duration_seconds summary
    rpc_duration_seconds{service="backup",code="400",quantile="0.01"} 3102 1395066363000
    rpc_duration_seconds{code="400",service="backup",quantile="0.05"} 3272 1395066363000
    rpc_duration_seconds{code="400",quantile="0.5",service="backup"} 4773 1395066363000
    rpc_duration_seconds{service="backup",quantile="0.9",code="400"} 9001 1395066363000
    rpc_duration_seconds{quantile="0.99",service="backup",code="400"} 76656 1395066363000
    rpc_duration_seconds_sum{service="backup",code="400"} 1.7560473e+07 1395066363000
    rpc_duration_seconds_count{service="backup",code="400"} 2693 1395066363000
    "#;

        // Read line by line
        let lines: Vec<_> = scrape.lines().map(|s| s.to_owned()).collect();
        for line in lines.into_iter() {
            let line = line.trim();
            // skip the line that starts with #
            if line.is_empty() || line.trim_start().starts_with("#") {
                continue;
            }

            println!("{:?}", line);

            // split the line by space
            let tokens = &mut line.split_whitespace();
            let payload = _frame_payload(tokens).unwrap();

            dbg!(&payload);
        }
    }
}
