use std::time::Duration;

use log::error;
use reqwest::{Client, Method};
use serde_json::{json, Map, Value};

use crate::base::bridge::{BridgeTx, Payload};
use crate::base::{clock, PrometheusConfig};

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
        let lines: Vec<_> = resp.text().await?.lines().map(|s| s.to_owned()).collect();
        let mut sequence = 0;

        for mut payload in read_prom(lines) {
            sequence += 1;
            payload.sequence = sequence;
            self.tx.send_payload(payload).await;
        }

        Ok(())
    }
}

fn read_prom(lines: Vec<String>) -> Vec<Payload> {
    let mut payloads = vec![];
    for line in lines {
        let line = line.trim();
        // skip the line that starts with #
        if line.is_empty() || line.trim_start().starts_with('#') {
            continue;
        }

        // split the line by space
        let tokens = &mut line.split_whitespace();
        if let Some(p) = frame_payload(tokens) {
            payloads.push(p);
        }
    }

    payloads
}

// Create a function with impl Iterator over &str
fn frame_payload<'a>(mut line: impl Iterator<Item = &'a str>) -> Option<Payload> {
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
    payload.insert("value".to_owned(), json!(value));

    let mut payload = Payload {
        stream,
        device_id: None,
        sequence: 0,
        timestamp: clock() as u64,
        payload: payload.into(),
    };

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
            map.insert(parts[0].to_string(), json!(parts[1]));
        }
    }

    map
}

#[cfg(test)]
mod test {
    use super::frame_payload;

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
            let payload = frame_payload(tokens).unwrap();

            dbg!(&payload);
        }
    }
}
