#![allow(
    clippy::enum_variant_names,
    clippy::unused_unit,
    clippy::let_and_return,
    clippy::not_unsafe_ptr_arg_deref,
    clippy::cast_lossless,
    clippy::blacklisted_name,
    clippy::too_many_arguments,
    clippy::trivially_copy_pass_by_ref,
    clippy::let_unit_value,
    clippy::clone_on_copy
)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use figment::providers::{Data, Json, Toml};
use figment::Figment;
use flume::Sender;
use log::{debug, error, info, LevelFilter};

use simplelog::{ColorChoice, CombinedLogger, LevelPadding, TermLogger, TerminalMode};
use uplink::config::{Config, Ota, Persistence, Stats};
use uplink::{Action, ActionResponse, Package, Payload, Stream, StreamStatus};

const DEFAULT_CONFIG: &str = r#"
    bridge_port = 5555
    max_packet_size = 102400
    max_inflight = 100

    # Whitelist of binaries which uplink can spawn as a process
    # This makes sure that user is protected against random actions
    # triggered from cloud.
    actions = ["tunshell"]

    [streams.metrics]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/metrics/jsonarray"
    buf_size = 10

    # Action status stream from status messages from bridge
    [streams.action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    buf_size = 1

    [ota]
    enabled = false
    path = "/var/tmp/ota-file"

    [stats]
    enabled = false
    process_names = ["uplink"]
    update_period = 5
"#;

pub trait ActionCallback {
    fn recvd_action(&self, action: UplinkAction);
}

struct CB(pub Box<dyn ActionCallback>);

unsafe impl Send for CB {}

pub struct UplinkConfig {
    inner: Config,
    enable_log_collector: bool,
}

impl UplinkConfig {
    pub fn new(config: String) -> Result<UplinkConfig, String> {
        Ok(UplinkConfig {
            inner: Figment::new()
                .merge(Data::<Toml>::string(&DEFAULT_CONFIG))
                .merge(Data::<Json>::string(&config))
                .extract()
                .map_err(|e| e.to_string())?,
            enable_log_collector: false,
        })
    }

    pub fn set_ota(&mut self, enabled: bool, path: String) {
        self.inner.ota = Ota { enabled, path };
    }

    pub fn set_stats(&mut self, enabled: bool, update_period: u64) {
        self.inner.stats =
            Stats { enabled, process_names: vec![], update_period, stream_size: None };
    }

    pub fn enable_log_collector(&mut self) {
        self.enable_log_collector = true;
    }

    // TODO: Add a method to insert `StreamConfig`s into inner.streams

    pub fn add_to_stats(&mut self, app: String) {
        self.inner.stats.process_names.push(app);
    }

    pub fn set_persistence(&mut self, path: String, max_file_size: usize, max_file_count: usize) {
        let persistence = Persistence { path, max_file_size, max_file_count };
        self.inner.persistence = Some(persistence);
    }
}

pub struct UplinkPayload {
    inner: Payload,
}

impl UplinkPayload {
    pub fn new(
        stream: String,
        timestamp: u64,
        sequence: u32,
        data: String,
    ) -> Result<UplinkPayload, String> {
        Ok(UplinkPayload {
            inner: Payload {
                stream,
                timestamp,
                sequence,
                payload: serde_json::from_str(&data).map_err(|e| e.to_string())?,
            },
        })
    }
}

pub struct UplinkAction {
    inner: Action,
}

impl UplinkAction {
    pub fn get_id(&self) -> &str {
        &self.inner.action_id
    }

    pub fn get_payload(&self) -> &str {
        &self.inner.payload
    }

    pub fn get_name(&self) -> &str {
        &self.inner.name
    }
}

pub struct Uplink {
    config: Arc<Config>,
    action_stream: Stream<ActionResponse>,
    streams: HashMap<String, Stream<Payload>>,
    data_tx: Sender<Box<dyn Package>>,
}

impl Uplink {
    pub fn new(config: UplinkConfig) -> Result<Uplink, String> {
        info!("init log system - done");

        let start_relay = config.enable_log_collector;
        let mut config = config.inner;

        if let Some(persistence) = &config.persistence {
            std::fs::create_dir_all(&persistence.path).map_err(|e| e.to_string())?;
        }
        let tenant_id = config.project_id.trim();
        let device_id = config.device_id.trim();
        for config in config.streams.values_mut() {
            let topic = str::replace(&config.topic, "{tenant_id}", tenant_id);
            config.topic = topic;

            let topic = str::replace(&config.topic, "{device_id}", device_id);
            config.topic = topic;
        }

        info!("Config: {:#?}", config);
        let config = Arc::new(config);

        let mut uplink = uplink::Uplink::new(config.clone()).map_err(|e| e.to_string())?;
        uplink.spawn().map_err(|e| e.to_string())?;

        let mut streams = HashMap::new();

        for (stream, cfg) in config.streams.iter() {
            streams.insert(
                stream.to_owned(),
                Stream::new(
                    stream.to_owned(),
                    cfg.topic.to_owned(),
                    cfg.buf_size,
                    uplink.bridge_data_tx(),
                ),
            );
        }

        let uplink = Uplink {
            config,
            action_stream: uplink.action_status(),
            streams,
            data_tx: uplink.bridge_data_tx(),
        };

        if start_relay {
            uplink.spawn_log_relay().map_err(|e| e.to_string())?;
        }

        Ok(uplink)
    }

    pub fn send(&mut self, payload: UplinkPayload) -> Result<(), String> {
        let data = payload.inner;
        match self.streams.get_mut(&data.stream) {
            Some(x) => {
                x.push(data).map_err(|e| e.to_string())?;
            }
            _ => {
                self.streams.insert(
                    data.stream.to_owned(),
                    Stream::dynamic(
                        &data.stream,
                        &self.config.project_id,
                        &self.config.device_id,
                        self.data_tx.clone(),
                    ),
                );

                self.streams
                    .get_mut(&data.stream)
                    .unwrap()
                    .push(data)
                    .map_err(|e| e.to_string())?;
            }
        }

        Ok(())
    }

    pub fn respond(&mut self, response: ActionResponse) -> Result<(), String> {
        self.action_stream.push(response).map_err(|e| e.to_string())?;

        Ok(())
    }

    pub fn spawn_log_relay(&self) -> Result<(), String> {
        let log_stream = Stream::dynamic(
            "logs",
            &self.config.project_id,
            &self.config.device_id,
            self.data_tx.clone(),
        );

        std::thread::spawn(move || {
            if let Err(e) = relay_logs(log_stream) {
                error!("Error while relaying logs: {}", e);
            }
        });

        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Assert,
    Verbose,
    Fatal,
}

#[derive(Debug, serde::Serialize)]
struct Log {
    level: LogLevel,
    tag: String,
    msg: String,
}

impl Log {
    fn from_string(log: String) -> Option<Self> {
        let tokens: Vec<&str> = log.split(' ').collect();

        // let level = match tokens.get(4)?.to_string() {
        //     &"I" => LogLevel::Info,
        //     &"D" => LogLevel::Debug,
        //     &"W" => LogLevel::Warning,
        //     &"E" => LogLevel::Error,
        //     &"F" => LogLevel::Fatal,
        //     &"V" => LogLevel::Verbose,
        //     &"A" => LogLevel::Assert,
        //     _ => return None,
        // };
        let tag = tokens.get(4)?.to_string();

        Some(Self { level: LogLevel::Verbose, tag, msg: log })
    }

    fn to_payload(self, sequence: u32) -> Result<Payload, String> {
        let payload = serde_json::to_value(self).map_err(|e| e.to_string())?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

        Ok(Payload { stream: "logs".to_string(), sequence, timestamp, payload })
    }
}

pub fn relay_logs(mut log_stream: Stream<Payload>) -> Result<ExitStatus, String> {
    let mut logcat = Command::new("tail")
        .args(["-f", "-n", "5", "/var/log/syslog"])
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    let stdout = logcat.stdout.as_mut().ok_or("stdout missing".to_string())?;
    let stdout_reader = BufReader::new(stdout);

    debug!("Collector setup to relay logs");

    for (sequence, line) in stdout_reader.lines().enumerate() {
        let log = line.map_err(|e| e.to_string())?;
        if let Some(log) = Log::from_string(log) {
            let data = log.to_payload(sequence as u32)?;
            debug!("{}", serde_json::to_string(&data).unwrap());
            if let StreamStatus::Partial(l) = log_stream.push(data).map_err(|e| e.to_string())? {
                info!("stream len: {}", l)
            };
        }
    }

    logcat.wait().map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() {
    let mut config = simplelog::ConfigBuilder::new();
    config
        .set_location_level(LevelFilter::Off)
        .set_target_level(LevelFilter::Error)
        .set_thread_level(LevelFilter::Error)
        .set_level_padding(LevelPadding::Right);

    config
        .add_filter_allow_str("uplink")
        .add_filter_allow_str("disk")
        .add_filter_allow_str("log_collector");

    let loggers =
        TermLogger::new(LevelFilter::Debug, config.build(), TerminalMode::Mixed, ColorChoice::Auto);
    CombinedLogger::init(vec![loggers]).unwrap();

    let mut config = UplinkConfig::new(r#"
      {
        "project_id": "test",
        "broker": "demo.bytebeam.io",
        "port": 8883,
        "device_id": "1117",
        "authentication": {
          "ca_certificate": "-----BEGIN CERTIFICATE-----\nMIIFrDCCA5SgAwIBAgICB+MwDQYJKoZIhvcNAQELBQAwdzEOMAwGA1UEBhMFSW5k\naWExETAPBgNVBAgTCEthcm5hdGFrMRIwEAYDVQQHEwlCYW5nYWxvcmUxFzAVBgNV\nBAkTDlN1YmJpYWggR2FyZGVuMQ8wDQYDVQQREwY1NjAwMTExFDASBgNVBAoTC0J5\ndGViZWFtLmlvMB4XDTIxMDkwMjExMDYyM1oXDTMxMDkwMjExMDYyM1owdzEOMAwG\nA1UEBhMFSW5kaWExETAPBgNVBAgTCEthcm5hdGFrMRIwEAYDVQQHEwlCYW5nYWxv\ncmUxFzAVBgNVBAkTDlN1YmJpYWggR2FyZGVuMQ8wDQYDVQQREwY1NjAwMTExFDAS\nBgNVBAoTC0J5dGViZWFtLmlvMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKC\nAgEAr/bnOa/8AUGZmd/s+7rejuROgeLqqU9X15KKfKOBqcoMyXsSO65UEwpzadpw\nMl7GDCdHqFTymqdnAnbhgaT1PoIFhOG64y7UiNgiWmbh0XJj8G6oLrW9rQ1gug1Q\n/D7x2fUnza71aixiwEL+KsIFYIdDuzmoRD3rSer/bKOcGGs0WfB54KqIVVZ1DwsU\nk1wx5ExsKo7gAdXMAbdHRI2Szmn5MsZwGL6V0LfsKLE8ms2qlZe50oo2woLNN6XP\nRfRL4bwwkdsCqXWkkt4eUSNDq9hJsuINHdhO3GUieLsKLJGWJ0lq6si74t75rIKb\nvvsFEQ9mnAVS+iuUUsSjHPJIMnn/J64Nmgl/R/8FP5TUgUrHvHXKQkJ9h/a7+3tS\nlV2KMsFksXaFrGEByGIJ7yR4qu9hx5MXf8pf8EGEwOW/H3CdWcC2MvJ11PVpceUJ\neDVwE7B4gPM9Kx02RNwvUMH2FmYqkXX2DrrHQGQuq+6VRoN3rEdmGPqnONJEPeOw\nZzcGDVXKWZtd7UCbcZKdn0RYmVtI/OB5OW8IRoXFYgGB3IWP796dsXIwbJSqRb9m\nylICGOceQy3VR+8+BHkQLj5/ZKTe+AA3Ktk9UADvxRiWKGcejSA/LvyT8qzz0dqn\nGtcHYJuhJ/XpkHtB0PykB5WtxFjx3G/osbZfrNflcQZ9h1MCAwEAAaNCMEAwDgYD\nVR0PAQH/BAQDAgKEMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFKl/MTbLrZ0g\nurneOmAfBHO+LHz+MA0GCSqGSIb3DQEBCwUAA4ICAQAlus/uKic5sgo1d2hBJ0Ak\ns1XJsA2jz+OEdshQHmCCmzFir3IRSuVRmDBaBGlJDHCELqYxKn6dl/sKGwoqoAQ5\nOeR2sey3Nmdyw2k2JTDx58HnApZKAVir7BDxbIbbHmfhJk4ljeUBbertNXWbRHVr\ncs4XBNwXvX+noZjQzmXXK89YBsV2DCrGRAUeZ4hQEqV7XC0VKmlzEmfkr1nibDr5\nqwbI+7QWIAnkHggYi27lL2UTHpbsy9AnlrRMe73upiuLO7TvkwYC4TyDaoQ2ZRpG\nHY+mxXLdftoMv/ZvmyjOPYeTRQbfPqoRqcM6XOPXwSw9B6YddwmnkI7ohNOvAVfD\nwGptUc5OodgFQc3waRljX1q2lawZCTh58IUf32CRtOEL2RIz4VpUrNF/0E2vts1f\npO7V1vY2Qin998Nwqkxdsll0GLtEEE9hUyvk1F8U+fgjJ3Rjn4BxnCN4oCrdJOMa\nJCaysaHV7EEIMqrYP4jH6RzQzOXLd0m9NaL8A/Y9z2a96fwpZZU/fEEOH71t3Eo3\nV/CKlysiALMtsHfZDwHNpa6g0NQNGN5IRl/w1TS1izzjzgWhR6r8wX8OPLRzhNRz\n2HDbTXGYsem0ihC0B8uzujOhTHcBwsfxZUMpGjg8iycJlfpPDWBdw8qrGu8LeNux\na0cIevjvYAtVysoXInV0kg==\n-----END CERTIFICATE-----\n",
          "device_certificate": "-----BEGIN CERTIFICATE-----\nMIIEajCCAlKgAwIBAgICB+MwDQYJKoZIhvcNAQELBQAwdzEOMAwGA1UEBhMFSW5k\naWExETAPBgNVBAgTCEthcm5hdGFrMRIwEAYDVQQHEwlCYW5nYWxvcmUxFzAVBgNV\nBAkTDlN1YmJpYWggR2FyZGVuMQ8wDQYDVQQREwY1NjAwMTExFDASBgNVBAoTC0J5\ndGViZWFtLmlvMB4XDTIyMDQxMzA5MDkwM1oXDTMyMDQxMzA5MDkwM1owHjENMAsG\nA1UEChMEdGVzdDENMAsGA1UEAxMEMTExNzCCASIwDQYJKoZIhvcNAQEBBQADggEP\nADCCAQoCggEBAMAwdwaGdrGP9W4jy3Jw/GFxJ4fwlZDR6R1I/j9M6DrlWU422mLd\njf9F8kP86zUlj1tfgM2OpO3XtNoqnUovCVZUtCcLUtFir9Bb4Mz37G02G6tlvtQE\nKyGolcp5Aiwr0JcTM9buMJleFdbnUdVvc+/dCOTpCNSmpplHChA/SmYfxSHQYwcO\nIem+zQHX+P+OqSFPrEGNT8jtMm6v60KYNZduwXN7htk5FOI+Hhb/NSGe0x5hJKQ9\nmwb6tz9zrKmJoYtn1RzskgrS0jZ3CVZ+Y8cPxGDgCMMFlUMl4+UVRs/H7Ef6vyyg\nm4wjGcust6SsCL6P+/3Kk8U4mMInjBkemUkCAwEAAaNZMFcwDgYDVR0PAQH/BAQD\nAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMCMB8GA1UdIwQYMBaAFKl/MTbLrZ0gurne\nOmAfBHO+LHz+MA8GA1UdEQQIMAaCBDExMTcwDQYJKoZIhvcNAQELBQADggIBAAEl\nWjuHQ9aSnhBDyeR11dfKODyXryCTpGxfHnZbXYWq0kw0VrrvZoOyYHfOZgBGp9qW\n8LGf3apzU0a7PbzLLAlsSYmCn4GqDOIbnmpV+l/O0tmzdFBrRllyK2fkwIHTl19M\n4nhXERrlFBwUkP8piHg6aIXcMaig9QECx77HJJ6bdA4tLD+w1mDC4bMnKRhb7dId\nP0OEB9Vcd+mHLk6srM5+SNXfnp8xv3FdmuZ1/yr04hJA1LB025FbQw8WebLhyUPw\nt9uWOU1RnvV0auNa7z4mX8ZcA+C+D9vyVmV6x3qDZ+l2cuidvEy6Iq3EDypmsW+b\nOIJNWWt1W1S3vZ6XUB1vW1YHQqqjyxNLBFRU7E52006XdKNnFdtyTQTLT6OEkltf\nG62oeXS+MSSwiMW5vyhKup0Q2lUYOjI+bfiSYm7b93qtBqEoexfQimxYmVL7FhDJ\naaQ6vR8GuSrLxdviHUjld+s/veZAIPHYInTe8mbc4l5EbbYftwbIgkrQ6zwnTQ1f\n3pvg3ajM19pddDePenxnOL7cl73LSn98PGNCgV8RDP1pLrb/TZrMPHmpatVrgSw8\nKcdLIa0eRtZNiatxZRlRaRfS8Wcay6UC2Tg8t5EJ+9+zsKZpzMnT6+cbG0rJPCq2\nCQyfirJVLrU7FTr4uDWUGC4NkpvoPxkD/qfSIsSX\n-----END CERTIFICATE-----\n",
          "device_private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAwDB3BoZ2sY/1biPLcnD8YXEnh/CVkNHpHUj+P0zoOuVZTjba\nYt2N/0XyQ/zrNSWPW1+AzY6k7de02iqdSi8JVlS0JwtS0WKv0FvgzPfsbTYbq2W+\n1AQrIaiVynkCLCvQlxMz1u4wmV4V1udR1W9z790I5OkI1KammUcKED9KZh/FIdBj\nBw4h6b7NAdf4/46pIU+sQY1PyO0ybq/rQpg1l27Bc3uG2TkU4j4eFv81IZ7THmEk\npD2bBvq3P3OsqYmhi2fVHOySCtLSNncJVn5jxw/EYOAIwwWVQyXj5RVGz8fsR/q/\nLKCbjCMZy6y3pKwIvo/7/cqTxTiYwieMGR6ZSQIDAQABAoIBAQCjQ0iJgYapFkr/\nEmdMw5CSUmarg4P672bhmtVo/rM0/QodeFmSrPVoiongmaVRk6OxknK/rFKNPbYD\nsznFAColbXVQybzD5NrH3JUeaeotaE6fDqLKRvRA9o6w3pUq4tmizQw3pEYisxtI\nYV9SOgi6FgHtO81loGBcS03QOYPBNff4zLelU8Zy18eK7iGKtP6dQVavx5rSJo0F\n5msIWPu8UBnJWdiuC/sqCNVPF4+V6EMVONqbS1KOBxLDCd/Iby7Q6hjFcmILzFpB\nh8jyBhy/KP88ijwUYuGx2IwL986jJffHTABZzgLCoFHl/EaiGRkUko1N3JiLy9uS\nvxUelSl1AoGBAM2upIiKyaa5SqkdM1PqzQFZmTYiWgSn58SKwKusEMV9WEfs9lm4\nwkGEDREN+pVvRTanr+g7bYizmja8rv96cZe7brEpUMPtUnZdlugYWuZI2EFqANC5\ndyueXBSt5YpHVeFiwax/LlCLeux3HSeHVzIbNjHIM5BqZkTqvClcnhGnAoGBAO80\nzA0FMp4wQ5uysrj83tuKs/b+If8gCkG0FAfEFrcdykfgMpKY992/Ba2ppCKPwgUO\n+B+zobePA4Vqqm9YSkAZZ02IBZ+BmXltriPIJ90941of4u1GkQKkvJN6Ge7oR7Aq\nYKIx/9gD22K86KNR4ukf63kfEpSufETd4IXa3PuPAoGAITkmho+0huPDNZHr6pAw\n0RkB8IaX98yPWWX4PUKr6tqWWffiyxdW+XI1Eh4p7d1tVqi7d02yIbSxIkpUEhxb\nIOE7vg4oZ518BnaTm6XjePMnS1muDAkJQNhxkz2LqExhiOiE1DIu7v+4uV18Lhhk\nc0mF9YCbI6asIlGwVxYIyl0CgYBZ2NLr8Q/aKva5/Jz0rsZmX+rI0xuh4D75/tGn\nORfvH9litetI9Pvk5mMTn6xu7uBJVh4MikQr1iPUcQQjXl5FRUVv8a9rAhrLaU8H\nUZ7nkt9acq+hv+envoi2PB6Rhd2nZcN2KKGYWZqFs78N2SwJtFuV2v33qrIyi8RR\ngquOMQKBgBMYJP5s7XwHVVTJ28AqavYZgMGX5PhHuJKOWa05ZmG1UcXe0oMMBvmy\nTCqthApi8YHE+e87+R2se6HkiQBPg+7LW1qavGkkkHTZPUtU7TkqoY+G/YhSgN2F\nVP65xKL0KoNBzbbSkVxXBJ4jIyTfKOq4a4g7TGk/iDo7sUpZ1V1I\n-----END RSA PRIVATE KEY-----\n"
        }
      }
    "#.to_string()).unwrap();
    config.enable_log_collector();
    let _uplink = Uplink::new(config).unwrap();

    loop {}
}
