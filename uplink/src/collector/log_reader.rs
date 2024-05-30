use std::process::Stdio;

use log::error;
use regex::{Match, Regex};
use time::{Month, OffsetDateTime};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use serde::Serialize;
use serde_json::json;
use tokio::process::Command;

use crate::base::bridge::{BridgeTx, Payload};
use crate::base::LogReaderConfig;

#[derive(Debug, Serialize, Clone, PartialEq)]
struct LogEntry {
    pub line: String,
    pub tag: String,
    pub level: Option<String>,
    #[serde(skip)]
    pub timestamp: u64,
    pub message: Option<String>,
    pub pid: Option<usize>,
    pub query_id: Option<String>,
}

/// Parse timestamp from log line, use current time as default if unable to ascertain partially
pub fn parse_timestamp(date: &mut OffsetDateTime, s: &str, template: &Regex) -> Option<()> {
    let matches = template.captures(s)?;
    let to_int = |m: Match<'_>| m.as_str().parse().ok();

    let mut year = matches.name("year").and_then(to_int).unwrap_or(2000);
    if year < 2000 {
        year += 2000
    }
    *date = date.replace_year(year).ok()?;

    let month = matches.name("month").unwrap().as_str();
    let month = match month {
        "January" | "Jan" => Month::January,
        "February" | "Feb" => Month::February,
        "March" | "Mar" => Month::March,
        "April" | "Apr" => Month::April,
        "May" => Month::May,
        "June" | "Jun" => Month::June,
        "July" | "Jul" => Month::July,
        "August" | "Aug" => Month::August,
        "September" | "Sep" => Month::September,
        "October" | "Oct" => Month::October,
        "November" | "Nov" => Month::November,
        "December" | "Dec" => Month::December,
        m => {
            let month: u8 = m.parse().ok()?;
            Month::try_from(month).ok()?
        }
    };
    *date = date.replace_month(month).ok()?;

    let day = matches.name("day").and_then(to_int).unwrap_or(0);
    *date = date.replace_day(day as u8).ok()?;

    let hour = matches.name("hour").and_then(to_int).unwrap_or(0);
    *date = date.replace_hour(hour as u8).ok()?;

    let minute = matches.name("minute").and_then(to_int).unwrap_or(0);
    *date = date.replace_minute(minute as u8).ok()?;

    let second = matches.name("second").and_then(to_int).unwrap_or(0);
    *date = date.replace_second(second as u8).ok()?;

    let millisecond =
        matches.name("subsecond").and_then(|m| m.as_str()[0..3].parse().ok()).unwrap_or(0);
    *date = date.replace_millisecond(millisecond).ok()?;

    Some(())
}

impl LogEntry {
    // NOTE: expects log lines to contain information as defined in log_template e.g. "{log_timestamp} {level} {tag}: {message}" else treats them as message lines
    fn parse(
        current_line: &mut Option<Self>,
        line: &str,
        log_template: &Regex,
        timestamp_template: &Regex,
        multi_line: bool,
    ) -> Option<Self> {
        let to_string = |x: Match| x.as_str().to_string();
        fn to_usize(x: Match) -> Option<usize> {
            x.as_str().parse().ok()
        }
        let line = line.trim().to_string();
        if let Some(captures) = log_template.captures(&line) {
            // Use current time if not able to parse properly
            let mut date = time::OffsetDateTime::now_utc();
            if let Some(t) = captures.name("timestamp") {
                parse_timestamp(&mut date, t.as_str(), timestamp_template);
            }

            let timestamp = (date.unix_timestamp_nanos() / 1_000_000) as u64;
            let level = captures.name("level").map(to_string);
            // set tag from env
            let tag = captures
                .name("tag")
                .map(to_string)
                .unwrap_or_else(|| std::env::var("LOG_TAG").unwrap_or("".to_owned()));
            let message = captures.name("message").map(to_string);
            let pid = captures.name("pid").map(to_usize).flatten();
            let query_id = captures.name("query_id").map(to_string);

            return current_line.replace(LogEntry {
                line,
                tag,
                level,
                timestamp,
                message,
                pid,
                query_id,
            });
        } else if multi_line {
            if let Some(log_entry) = current_line {
                log_entry.line += &format!("\n{line}");
                match &mut log_entry.message {
                    Some(msg) => *msg += &format!("\n{line}"),
                    _ => log_entry.message = Some(line),
                };
            }
        }

        None
    }

    fn payload(self, stream: String, sequence: u32) -> Payload {
        Payload { stream, sequence, timestamp: self.timestamp, payload: json!(self) }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

pub struct LogParser<T> {
    lines: Lines<T>,
    log_entry: Option<LogEntry>,
    log_template: Regex,
    timestamp_template: Regex,
}

impl<T: AsyncBufReadExt + Unpin> LogParser<T> {
    fn new(lines: Lines<T>, log_template: Regex, timestamp_template: Regex) -> Self {
        Self { lines, log_entry: None, log_template, timestamp_template }
    }

    async fn next(&mut self, multi_line: bool) -> Option<LogEntry> {
        while let Some(line) = self.lines.next_line().await.ok()? {
            if let Some(entry) = LogEntry::parse(
                &mut self.log_entry,
                &line,
                &self.log_template,
                &self.timestamp_template,
                multi_line,
            ) {
                return Some(entry);
            }
        }

        self.log_entry.take()
    }
}

#[derive(Debug, Clone)]
pub struct LogFileReader {
    config: LogReaderConfig,
    tx: BridgeTx,
    log_template: Regex,
    timestamp_template: Regex,
}

impl LogFileReader {
    pub fn new(config: LogReaderConfig, tx: BridgeTx) -> Self {
        let log_template = Regex::new(&config.log_template).unwrap();
        let timestamp_template = Regex::new(&config.timestamp_template).unwrap();
        Self { config, tx, log_template, timestamp_template }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) -> Result<(), Error> {
        let mut cmd =
            Command::new("tail").args(["-F", &self.config.path]).stdout(Stdio::piped()).spawn()?;
        let file = cmd.stdout.take().expect("Expected stdout");
        let lines = BufReader::new(file).lines();
        let mut parser =
            LogParser::new(lines, self.log_template.clone(), self.timestamp_template.clone());
        let mut sequence = 0;
        let stream_name = self.config.stream_name.to_owned();
        let tx = self.tx.clone();

        while let Some(entry) = parser.next(self.config.multi_line).await {
            sequence += 1;
            let payload = entry.payload(stream_name.clone(), sequence);
            tx.send_payload(payload).await
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn parse_single_log_line() {
        let raw = r#"2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)"#;
        let lines = BufReader::new(raw.as_bytes()).lines();

        let log_template =
            Regex::new(r#"^(?P<timestamp>.*)Z\s(?P<level>\S+)\s(?P<tag>\S+):\s(?P<message>.*)"#)
                .unwrap();
        let timestamp_template = Regex::new(r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)\.(?P<millisecond>\S\S\S)"#).unwrap();
        let mut parser = LogParser::new(lines, log_template, timestamp_template);

        let entry = parser.next(false).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                level: Some("DEBUG".to_string()),
                line: "2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)"
                    .to_string(),
                timestamp: 1688407162000,
                message: Some("Outgoing = Publish(9)".to_string()),
                tag: Some("uplink::base::mqtt".to_string()),
                pid: None,
                query_id: None
            }
        );

        assert!(parser.next(false).await.is_none());
    }

    #[tokio::test]
    async fn parse_timestamp() {
        let raw = r#"2023-07-03T17:59:22.979012"#;
        let lines = BufReader::new(raw.as_bytes()).lines();

        let log_template = Regex::new(r#"^(?P<timestamp>.*)"#).unwrap();
        let timestamp_template = Regex::new(r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)\.(?P<subsecond>\S\S\S)"#).unwrap();
        let mut parser = LogParser::new(lines, log_template.clone(), timestamp_template);

        let entry = parser.next(false).await.unwrap();
        assert_eq!(entry.timestamp, 1688407162979);

        let raw = r#"23-07-11 18:03:32"#;
        let lines = BufReader::new(raw.as_bytes()).lines();

        let timestamp_template= Regex::new(r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)\s(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)"#).unwrap();
        let mut parser = LogParser::new(lines, log_template.clone(), timestamp_template);

        let entry = parser.next(false).await.unwrap();

        assert_eq!(entry.timestamp, 1689098612000);
    }

    #[tokio::test]
    async fn parse_multiple_log_lines() {
        let raw = r#"2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)
2023-07-03T17:59:23.012000Z DEBUG uplink::base::mqtt: Incoming = PubAck(9)"#;
        let lines = BufReader::new(raw.as_bytes()).lines();

        let log_template =
            Regex::new(r#"^(?P<timestamp>.*)Z\s(?P<level>\S+)\s(?P<tag>\S+):\s(?P<message>.*)"#)
                .unwrap();
        let timestamp_template= Regex::new(r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)\.(?P<subsecond>\S\S\S)"#).unwrap();
        let mut parser = LogParser::new(lines, log_template, timestamp_template);

        let entry = parser.next(true).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                level: Some("DEBUG".to_string()),
                line: "2023-07-03T17:59:22.979012Z DEBUG uplink::base::mqtt: Outgoing = Publish(9)"
                    .to_string(),
                timestamp: 1688407162979,
                message: Some("Outgoing = Publish(9)".to_string()),
                tag: Some("uplink::base::mqtt".to_string()),
                pid: None,
                query_id: None
            }
        );

        let entry = parser.next(true).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                level: Some("DEBUG".to_string()),
                line: "2023-07-03T17:59:23.012000Z DEBUG uplink::base::mqtt: Incoming = PubAck(9)"
                    .to_string(),
                timestamp: 1688407163012,
                message: Some("Incoming = PubAck(9)".to_string()),
                tag: Some("uplink::base::mqtt".to_string()),
                pid: None,
                query_id: None
            }
        );

        assert!(parser.next(true).await.is_none());
    }

    #[tokio::test]
    async fn parse_beamd_log_lines() {
        let raw = r#"2023-07-11T13:56:44.101585Z  INFO beamd::http::endpoint: Method = "POST", Uri = "/tenants/naveentest/devices/8/actions", Payload = "{\"name\":\"update_firmware\",\"id\":\"830\",\"payload\":\"{\\\"content-length\\\":35393,\\\"status\\\":false,\\\"url\\\":\\\"https://firmware.stage.bytebeam.io/api/v1/firmwares/one/artifact\\\",\\\"version\\\":\\\"one\\\"}\",\"kind\":\"process\"}"
2023-07-11T13:56:44.113343Z  INFO beamd::http::endpoint: Method = "POST", Uri = "/tenants/rpi/devices/6/actions", Payload = "{\"name\":\"tunshell\",\"id\":\"226\",\"payload\":\"{}\",\"kind\":\"process\"}"
2023-07-11T13:56:44.221249Z ERROR beamd::clickhouse: Flush-error: [Status - 500] Ok("Code: 243. DB::Exception: Cannot reserve 11.58 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 22.6.2.12 (official build))\n"), back_up_enabled: true
    in beamd::clickhouse::clickhouse_flush with stream: "demo.uplink_process_stats""#;
        let lines = BufReader::new(raw.as_bytes()).lines();

        let log_template =
            Regex::new(r#"^(?P<timestamp>.*)Z\s+(?P<level>\S+)\s+(?P<tag>\S+):\s+(?P<message>.*)"#)
                .unwrap();
        let timestamp_template= Regex::new(r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)T(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)\.(?P<subsecond>\S\S\S)"#).unwrap();
        let mut parser = LogParser::new(lines, log_template.clone(), timestamp_template);

        let entry = parser.next(true).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                level: Some("INFO".to_string()),
                line: "2023-07-11T13:56:44.101585Z  INFO beamd::http::endpoint: Method = \"POST\", Uri = \"/tenants/naveentest/devices/8/actions\", Payload = \"{\\\"name\\\":\\\"update_firmware\\\",\\\"id\\\":\\\"830\\\",\\\"payload\\\":\\\"{\\\\\\\"content-length\\\\\\\":35393,\\\\\\\"status\\\\\\\":false,\\\\\\\"url\\\\\\\":\\\\\\\"https://firmware.stage.bytebeam.io/api/v1/firmwares/one/artifact\\\\\\\",\\\\\\\"version\\\\\\\":\\\\\\\"one\\\\\\\"}\\\",\\\"kind\\\":\\\"process\\\"}\"".to_string(),
                timestamp: 1689083804101,
                message: Some("Method = \"POST\", Uri = \"/tenants/naveentest/devices/8/actions\", Payload = \"{\\\"name\\\":\\\"update_firmware\\\",\\\"id\\\":\\\"830\\\",\\\"payload\\\":\\\"{\\\\\\\"content-length\\\\\\\":35393,\\\\\\\"status\\\\\\\":false,\\\\\\\"url\\\\\\\":\\\\\\\"https://firmware.stage.bytebeam.io/api/v1/firmwares/one/artifact\\\\\\\",\\\\\\\"version\\\\\\\":\\\\\\\"one\\\\\\\"}\\\",\\\"kind\\\":\\\"process\\\"}\"".to_string()),
                tag: Some("beamd::http::endpoint".to_string()),
                pid: None,
                query_id: None
            }
        );

        let entry = parser.next(true).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                level: Some("INFO".to_string()),
                line: "2023-07-11T13:56:44.113343Z  INFO beamd::http::endpoint: Method = \"POST\", Uri = \"/tenants/rpi/devices/6/actions\", Payload = \"{\\\"name\\\":\\\"tunshell\\\",\\\"id\\\":\\\"226\\\",\\\"payload\\\":\\\"{}\\\",\\\"kind\\\":\\\"process\\\"}\"".to_string(),
                timestamp: 1689083804113,
                message: Some("Method = \"POST\", Uri = \"/tenants/rpi/devices/6/actions\", Payload = \"{\\\"name\\\":\\\"tunshell\\\",\\\"id\\\":\\\"226\\\",\\\"payload\\\":\\\"{}\\\",\\\"kind\\\":\\\"process\\\"}\"".to_string()),
                tag: Some("beamd::http::endpoint".to_string()),
                pid: None,
                query_id: None
            }
        );

        let entry = parser.next(true).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                line: "2023-07-11T13:56:44.221249Z ERROR beamd::clickhouse: Flush-error: [Status - 500] Ok(\"Code: 243. DB::Exception: Cannot reserve 11.58 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 22.6.2.12 (official build))\\n\"), back_up_enabled: true\nin beamd::clickhouse::clickhouse_flush with stream: \"demo.uplink_process_stats\"".to_string(),
                tag: Some("beamd::clickhouse".to_string()),
                level: Some("ERROR".to_string()),
                timestamp: 1689083804221,
                message: Some("Flush-error: [Status - 500] Ok(\"Code: 243. DB::Exception: Cannot reserve 11.58 MiB, not enough space. (NOT_ENOUGH_SPACE) (version 22.6.2.12 (official build))\\n\"), back_up_enabled: true\nin beamd::clickhouse::clickhouse_flush with stream: \"demo.uplink_process_stats\"".to_string()),
                pid: None,
                query_id: None
            }
        );

        assert!(parser.next(true).await.is_none());
    }

    #[tokio::test]
    async fn parse_consoled_log_lines() {
        let raw = r#"23-07-11 18:03:32 consoled-6cd8795566-76km9 INFO [ring.logger:0] - {:request-method :get, :uri "/api/v1/devices/count", :server-name "cloud.bytebeam.io", :ring.logger/type :finish, :status 200, :ring.logger/ms 11}
10.13.2.69 - - [11/Jul/2023:18:03:32 +0000] "GET /api/v1/devices/count?status=active HTTP/1.1" 200 1 "https://cloud.bytebeam.io/projects/kptl/device-management/devices" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"rt=0.016 uct=0.000 cn= o=
"Notifying broker for tenant reactlabs device 305 action 683022""#;
        let lines = BufReader::new(raw.as_bytes()).lines();

        let log_template = Regex::new(r#"^(?P<timestamp>\S+-\S+-\S+\s\S+:\S+:\S+)\s+(?P<tag>\S+)\s+(?P<level>\S+)\s+(?P<message>.*)"#).unwrap();
        let timestamp_template = Regex::new(r#"^(?P<year>\S+)-(?P<month>\S+)-(?P<day>\S+)\s(?P<hour>\S+):(?P<minute>\S+):(?P<second>\S+)"#).unwrap();
        let mut parser = LogParser::new(lines, log_template.clone(), timestamp_template);

        let entry = parser.next(true).await.unwrap();
        assert_eq!(
            entry,
            LogEntry {
                level: Some("INFO".to_string()),
                line: "23-07-11 18:03:32 consoled-6cd8795566-76km9 INFO [ring.logger:0] - {:request-method :get, :uri \"/api/v1/devices/count\", :server-name \"cloud.bytebeam.io\", :ring.logger/type :finish, :status 200, :ring.logger/ms 11}\n10.13.2.69 - - [11/Jul/2023:18:03:32 +0000] \"GET /api/v1/devices/count?status=active HTTP/1.1\" 200 1 \"https://cloud.bytebeam.io/projects/kptl/device-management/devices\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36\"rt=0.016 uct=0.000 cn= o=\n\"Notifying broker for tenant reactlabs device 305 action 683022\"".to_string(),
                timestamp: 1689098612000,
                message: Some("[ring.logger:0] - {:request-method :get, :uri \"/api/v1/devices/count\", :server-name \"cloud.bytebeam.io\", :ring.logger/type :finish, :status 200, :ring.logger/ms 11}\n10.13.2.69 - - [11/Jul/2023:18:03:32 +0000] \"GET /api/v1/devices/count?status=active HTTP/1.1\" 200 1 \"https://cloud.bytebeam.io/projects/kptl/device-management/devices\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36\"rt=0.016 uct=0.000 cn= o=\n\"Notifying broker for tenant reactlabs device 305 action 683022\"".to_string()),
                tag: Some("consoled-6cd8795566-76km9".to_string()),
                pid: None,
                query_id: None
            }
        );

        assert!(parser.next(true).await.is_none());
    }
}
