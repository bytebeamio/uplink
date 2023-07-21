use regex::{Match, Regex};
use tokio::io::{stdin, AsyncBufReadExt, BufReader, Lines};

use serde::Serialize;
use serde_json::json;

use crate::base::bridge::{BridgeTx, Payload};
use crate::base::StdoutConfig;

#[derive(Debug, Serialize)]
struct LogEntry {
    pub line: String,
    pub tag: Option<String>,
    pub level: Option<String>,
    #[serde(skip)]
    pub timestamp: u64,
    pub message: Option<String>,
}

/// Parse timestamp from log line, use current time as default if unable to ascertain partially
pub fn parse_timestamp(s: &str, template: &Regex) -> Option<u64> {
    let matches = template.captures(s)?;
    let mut date = time::OffsetDateTime::now_utc();
    if let Some(year) = matches.name("year") {
        let year = year.as_str().parse().ok()?;
        date = date.replace_year(year).ok()?;
    }
    if let Some(month) = matches.name("month") {
        let month = month.as_str().parse().ok()?;
        date = date.replace_month(month).ok()?;
    }
    if let Some(day) = matches.name("day") {
        let day = day.as_str().parse().ok()?;
        date = date.replace_day(day).ok()?;
    }
    if let Some(hour) = matches.name("hour") {
        let hour = hour.as_str().parse().ok()?;
        date = date.replace_hour(hour).ok()?;
    }
    if let Some(minute) = matches.name("minute") {
        let minute = minute.as_str().parse().ok()?;
        date = date.replace_minute(minute).ok()?;
    }
    if let Some(second) = matches.name("second") {
        let second = second.as_str().parse().ok()?;
        date = date.replace_second(second).ok()?;
    }
    if let Some(microsecond) = matches.name("microsecond") {
        let microsecond = microsecond.as_str().parse().ok()?;
        date = date.replace_microsecond(microsecond).ok()?;
    }

    Some((date.unix_timestamp_nanos() / 1_000_000) as u64)
}

impl LogEntry {
    // NOTE: expects log lines to contain information as defined in log_template e.g. "{log_timestamp} {level} {tag}: {message}" else treats them as message lines
    fn parse(
        current_line: &mut Option<Self>,
        line: &str,
        log_template: &Regex,
        timestamp_template: &Regex,
    ) -> Option<Self> {
        let to_string = |x: Match| x.as_str().to_string();
        let to_timestamp = |t: Match| parse_timestamp(t.as_str(), timestamp_template);
        let line = line.trim().to_string();
        if let Some(captures) = log_template.captures(&line) {
            // Use current time if not able to parse properly
            let timestamp = match captures.name("timestamp").map(to_timestamp).flatten() {
                Some(t) => t,
                _ => (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as u64,
            };
            let level = captures.name("level").map(to_string);
            let tag = captures.name("tag").map(to_string);
            let message = captures.name("message").map(to_string);

            return current_line.replace(LogEntry { line, tag, level, timestamp, message });
        } else if let Some(log_entry) = current_line {
            log_entry.line += &format!("\n{line}");
            match &mut log_entry.message {
                Some(msg) => *msg += &format!("\n{line}"),
                _ => log_entry.message = Some(line),
            };
        }

        None
    }

    fn payload(self, stream: String, sequence: u32) -> Payload {
        Payload {
            stream,
            device_id: None,
            sequence,
            timestamp: self.timestamp,
            payload: json!(self),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

pub struct Stdout {
    config: StdoutConfig,
    tx: BridgeTx,
    sequence: u32,
    log_template: Regex,
    timestamp_template: Regex,
    log_entry: Option<LogEntry>,
}

impl Stdout {
    pub fn new(config: StdoutConfig, tx: BridgeTx) -> Self {
        let log_template = Regex::new(&config.log_template).unwrap();
        let timestamp_template = Regex::new(&config.timestamp_template).unwrap();
        Self { config, tx, log_template, timestamp_template, sequence: 0, log_entry: None }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        let stdin = stdin();
        let mut lines = BufReader::new(stdin).lines();

        loop {
            match self.parse_lines(&mut lines).await {
                Some(payload) => self.tx.send_payload(payload).await,
                None => return Ok(()),
            }
        }
    }

    async fn parse_lines<T: AsyncBufReadExt + Unpin>(
        &mut self,
        lines: &mut Lines<T>,
    ) -> Option<Payload> {
        while let Some(line) = lines.next_line().await.ok()? {
            if let Some(entry) = LogEntry::parse(
                &mut self.log_entry,
                &line,
                &self.log_template,
                &self.timestamp_template,
            ) {
                self.sequence += 1;
                return Some(entry.payload(self.config.stream_name.to_owned(), self.sequence));
            }
        }

        self.log_entry
            .take()
            .map(|e| e.payload(self.config.stream_name.to_owned(), self.sequence + 1))
    }
}

#[cfg(test)]
mod test {
    use flume::bounded;

    use super::*;

    #[tokio::test]
    async fn parse_single_log_line() {
        let raw = r#"2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)"#;
        let mut lines = BufReader::new(raw.as_bytes()).lines();

        let config = StdoutConfig {
            stream_name: "".to_string(),
            log_template:
                r#"^(?P<timestamp>.*)Z\s(?P<level>\S+)\s(?P<tag>\S+):\s(?P<message>.*)"#
                    .to_string(),
            timestamp_template: r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+).(?P<millisecond>\S\S\S)"#.to_string(),
        };
        let tx = BridgeTx {
            events_tx: {
                let (tx, _) = bounded(1);
                tx
            },
            shutdown_handle: {
                let (tx, _) = bounded(1);
                tx
            },
        };
        let mut handle = Stdout::new(config, tx);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({"level": "DEBUG", "line": "2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)", "message": "Outgoing = Publish(9)", "tag": "uplink::base::mqtt"})
        );
        assert_eq!(sequence, 1);

        assert!(handle.parse_lines(&mut lines).await.is_none());
    }

    #[tokio::test]
    async fn parse_timestamp() {
        let raw = r#"2023-07-03T17:59:22.979012"#;
        let mut lines = BufReader::new(raw.as_bytes()).lines();

        let config = StdoutConfig {
            stream_name: "".to_string(),
            log_template: r#"^(?P<timestamp>.*)"#.to_string(),
            timestamp_template: r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+).(?P<millisecond>\S\S\S).*"#.to_string(),
        };
        let tx = BridgeTx {
            events_tx: {
                let (tx, _) = bounded(1);
                tx
            },
            shutdown_handle: {
                let (tx, _) = bounded(1);
                tx
            },
        };
        let mut handle = Stdout::new(config, tx);

        let Payload { timestamp, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(sequence, 1);
        assert_eq!(timestamp, 1689138886255);

        let raw = r#"23-07-11 18:03:32"#;
        let mut lines = BufReader::new(raw.as_bytes()).lines();

        let config = StdoutConfig {
            stream_name: "".to_string(),
            log_template: r#"^(?P<timestamp>.*)"#.to_string(),
            timestamp_template: r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)\s(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)"#.to_string(),
        };
        let tx = BridgeTx {
            events_tx: {
                let (tx, _) = bounded(1);
                tx
            },
            shutdown_handle: {
                let (tx, _) = bounded(1);
                tx
            },
        };
        let mut handle = Stdout::new(config, tx);

        let Payload { timestamp, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(sequence, 1);
        assert_eq!(timestamp, 1689138886255);
    }

    #[tokio::test]
    async fn parse_multiple_log_lines() {
        let raw = r#"2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)
2023-07-03T17:59:23.012000Z DEBUG uplink::base::mqtt: Incoming = PubAck(9)"#;
        let mut lines = BufReader::new(raw.as_bytes()).lines();

        let config = StdoutConfig {
            stream_name: "".to_string(),
            log_template:
                r#"^(?P<timestamp>.*)Z\s(?P<level>\S+)\s(?P<tag>\S+):\s(?P<message>.*)"#
                    .to_string(),
            timestamp_template: r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+).(?P<millisecond>\S\S\S)"#.to_string(),
        };
        let tx = BridgeTx {
            events_tx: {
                let (tx, _) = bounded(1);
                tx
            },
            shutdown_handle: {
                let (tx, _) = bounded(1);
                tx
            },
        };
        let mut handle = Stdout::new(config, tx);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({"level": "DEBUG", "line": "2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)", "message": "Outgoing = Publish(9)", "tag": "uplink::base::mqtt"})
        );
        assert_eq!(sequence, 1);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({"level": "DEBUG", "line": "2023-07-03T17:59:23.012000Z DEBUG uplink::base::mqtt: Incoming = PubAck(9)", "message": "Incoming = PubAck(9)", "tag": "uplink::base::mqtt"})
        );
        assert_eq!(sequence, 2);

        assert!(handle.parse_lines(&mut lines).await.is_none());
    }

    #[tokio::test]
    async fn parse_beamd_log_lines() {
        let raw = r#"2023-07-11T13:56:44.101585Z  INFO beamd::http::endpoint: Method = "POST", Uri = "/tenants/naveentest/devices/8/actions", Payload = "{\"name\":\"update_firmware\",\"id\":\"830\",\"payload\":\"{\\\"content-length\\\":35393,\\\"status\\\":false,\\\"url\\\":\\\"https://firmware.stage.bytebeam.io/api/v1/firmwares/one/artifact\\\",\\\"version\\\":\\\"one\\\"}\",\"kind\":\"process\"}"
2023-07-11T13:56:44.113343Z  INFO beamd::http::endpoint: Method = "POST", Uri = "/tenants/rpi/devices/6/actions", Payload = "{\"name\":\"tunshell\",\"id\":\"226\",\"payload\":\"{}\",\"kind\":\"process\"}"
2023-07-11T13:56:44.221249Z ERROR beamd::clickhouse: Flush-error: [Status - 500] Ok("Code: 243. DB::Exception: Cannot reserve 11.58 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 22.6.2.12 (official build))\n"), back_up_enabled: true
    in beamd::clickhouse::clickhouse_flush with stream: "demo.uplink_process_stats""#;
        let mut lines = BufReader::new(raw.as_bytes()).lines();

        let config = StdoutConfig {
      stream_name: "".to_string(),
      log_template:
          r#"^(?P<timestamp>.*)Z\s+(?P<level>\S+)\s+(?P<tag>\S+):\s+(?P<message>.*)"#
              .to_string(),
      timestamp_template: r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+).(?P<millisecond>\S\S\S)"#.to_string(),
  };
        let tx = BridgeTx {
            events_tx: {
                let (tx, _) = bounded(1);
                tx
            },
            shutdown_handle: {
                let (tx, _) = bounded(1);
                tx
            },
        };
        let mut handle = Stdout::new(config, tx);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({
                "level": "INFO",
                "line": "2023-07-11T13:56:44.101585Z  INFO beamd::http::endpoint: Method = \"POST\", Uri = \"/tenants/naveentest/devices/8/actions\", Payload = \"{\\\"name\\\":\\\"update_firmware\\\",\\\"id\\\":\\\"830\\\",\\\"payload\\\":\\\"{\\\\\\\"content-length\\\\\\\":35393,\\\\\\\"status\\\\\\\":false,\\\\\\\"url\\\\\\\":\\\\\\\"https://firmware.stage.bytebeam.io/api/v1/firmwares/one/artifact\\\\\\\",\\\\\\\"version\\\\\\\":\\\\\\\"one\\\\\\\"}\\\",\\\"kind\\\":\\\"process\\\"}\"",
                "message": "Method = \"POST\", Uri = \"/tenants/naveentest/devices/8/actions\", Payload = \"{\\\"name\\\":\\\"update_firmware\\\",\\\"id\\\":\\\"830\\\",\\\"payload\\\":\\\"{\\\\\\\"content-length\\\\\\\":35393,\\\\\\\"status\\\\\\\":false,\\\\\\\"url\\\\\\\":\\\\\\\"https://firmware.stage.bytebeam.io/api/v1/firmwares/one/artifact\\\\\\\",\\\\\\\"version\\\\\\\":\\\\\\\"one\\\\\\\"}\\\",\\\"kind\\\":\\\"process\\\"}\"",
                "tag": "beamd::http::endpoint"
            })
        );
        assert_eq!(sequence, 1);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({
                "level": "INFO",
                "line": "2023-07-11T13:56:44.113343Z  INFO beamd::http::endpoint: Method = \"POST\", Uri = \"/tenants/rpi/devices/6/actions\", Payload = \"{\\\"name\\\":\\\"tunshell\\\",\\\"id\\\":\\\"226\\\",\\\"payload\\\":\\\"{}\\\",\\\"kind\\\":\\\"process\\\"}\"",
                "message": "Method = \"POST\", Uri = \"/tenants/rpi/devices/6/actions\", Payload = \"{\\\"name\\\":\\\"tunshell\\\",\\\"id\\\":\\\"226\\\",\\\"payload\\\":\\\"{}\\\",\\\"kind\\\":\\\"process\\\"}\"",
                "tag": "beamd::http::endpoint"
            })
        );
        assert_eq!(sequence, 2);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({
                "level": "ERROR",
                "line": "2023-07-11T13:56:44.221249Z ERROR beamd::clickhouse: Flush-error: [Status - 500] Ok(\"Code: 243. DB::Exception: Cannot reserve 11.58 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 22.6.2.12 (official build))\\n\"), back_up_enabled: true\n    in beamd::clickhouse::clickhouse_flush with stream: \"demo.uplink_process_stats\"",
                "message": "Flush-error: [Status - 500] Ok(\"Code: 243. DB::Exception: Cannot reserve 11.58 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 22.6.2.12 (official build))\\n\"), back_up_enabled: true\n    in beamd::clickhouse::clickhouse_flush with stream: \"demo.uplink_process_stats\"",
                "tag": "beamd::clickhouse"
            })
        );
        assert_eq!(sequence, 3);

        assert!(handle.parse_lines(&mut lines).await.is_none());
    }

    #[tokio::test]
    async fn parse_consoled_log_lines() {
        let raw = r#"23-07-11 18:03:32 consoled-6cd8795566-76km9 INFO [ring.logger:0] - {:request-method :get, :uri "/api/v1/devices/count", :server-name "cloud.bytebeam.io", :ring.logger/type :finish, :status 200, :ring.logger/ms 11}
10.13.2.69 - - [11/Jul/2023:18:03:32 +0000] "GET /api/v1/devices/count?status=active HTTP/1.1" 200 1 "https://cloud.bytebeam.io/projects/kptl/device-management/devices" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"rt=0.016 uct=0.000 cn= o=
"Notifying broker for tenant reactlabs device 305 action 683022""#;
        let mut lines = BufReader::new(raw.as_bytes()).lines();

        let config = StdoutConfig {
      stream_name: "".to_string(),
      log_template:
          r#"^(?P<timestamp>\S+-\S+-\S+\s\S+:\S+:\S+)\s+(?P<tag>\S+)\s+(?P<level>\S+)\s+(?P<message>.*)"#
              .to_string(),
      timestamp_template: r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)\s(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)"#.to_string(),
  };
        let tx = BridgeTx {
            events_tx: {
                let (tx, _) = bounded(1);
                tx
            },
            shutdown_handle: {
                let (tx, _) = bounded(1);
                tx
            },
        };
        let mut handle = Stdout::new(config, tx);

        let Payload { payload, sequence, .. } = handle.parse_lines(&mut lines).await.unwrap();
        assert_eq!(
            payload,
            json!({
                "level": "INFO",
                "line": "23-07-11 18:03:32 consoled-6cd8795566-76km9 INFO [ring.logger:0] - {:request-method :get, :uri \"/api/v1/devices/count\", :server-name \"cloud.bytebeam.io\", :ring.logger/type :finish, :status 200, :ring.logger/ms 11}\n10.13.2.69 - - [11/Jul/2023:18:03:32 +0000] \"GET /api/v1/devices/count?status=active HTTP/1.1\" 200 1 \"https://cloud.bytebeam.io/projects/kptl/device-management/devices\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36\"rt=0.016 uct=0.000 cn= o=\n\"Notifying broker for tenant reactlabs device 305 action 683022\"",
                "message": "[ring.logger:0] - {:request-method :get, :uri \"/api/v1/devices/count\", :server-name \"cloud.bytebeam.io\", :ring.logger/type :finish, :status 200, :ring.logger/ms 11}\n10.13.2.69 - - [11/Jul/2023:18:03:32 +0000] \"GET /api/v1/devices/count?status=active HTTP/1.1\" 200 1 \"https://cloud.bytebeam.io/projects/kptl/device-management/devices\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36\"rt=0.016 uct=0.000 cn= o=\n\"Notifying broker for tenant reactlabs device 305 action 683022\"",
                "tag": "consoled-6cd8795566-76km9"
            })
        );
        assert_eq!(sequence, 1);

        assert!(handle.parse_lines(&mut lines).await.is_none());
    }
}
