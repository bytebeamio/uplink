//! Uplink is a utility/library to interact with the Bytebeam platform. Its internal architecture is described in the diagram below.
//!
//! We use [`rumqttc`], which implements the MQTT protocol, to communicate with the platform. Communication is handled separately as ingress and egress
//! by [`Mqtt`] and [`Serializer`] respectively. [`Action`]s are received and forwarded by [`Mqtt`] to the [`Bridge`] module, where it is handled
//! depending on the [`name`], with [`Bridge`] forwarding it to one of many **Action Handlers**, configured with an [`ActionRoute`].
//!
//! Some of the action handlers are [`TcpJson`], [`ProcessHandler`], [`FileDownloader`] and [`TunshellSession`]. [`TcpJson`] forwards Actions received
//! from the platform to the application connected to it through the [`port`] and collects response data from these devices, to forward to the platform.
//! Response data can be of multiple types, of interest to us are [`ActionResponse`]s and data [`Payload`]s, which are forwarded to [`Bridge`] and from
//! there to the [`Serializer`], where depending on the network, it may be persisted in-memory or on-disk with [`Storage`].
//!
//!```text
//!                                      ┌───────────┐
//!                                      │MQTT broker│
//!                                      └────┐▲─────┘
//!                                           ││
//!                                    Action ││ ActionResponse
//!                                           ││ / Data
//!                                         ┌─▼└─┐
//!                              ┌──────────┤Mqtt◄─────────┐
//!                       Action │          └────┘         │ ActionResponse
//!                              │                         │ / Data
//!                              │                         │
//!                           ┌──▼───┐ ActionResponse ┌────┴─────┐   Publish   ┌───────┐
//!   ┌───────────────────────►Bridge├────────────────►Serializer◄─────────────►Storage|
//!   │                       └┬─┬┬─┬┘    / Data      └──────────┘   Packets   └───────┘
//!   │                        │ ││ │
//!   │                        │ ││ | Action (BridgeTx)
//!   │        ┌───────────────┘ ││ └────────────────────┐
//!   │        │           ┌─────┘└───────┐              │
//!   │  ------│-----------│--------------│--------------│------
//!   │  '     │           │ Applications │              │     '
//!   │  '┌────▼───┐   ┌───▼───┐   ┌──────▼───────┐  ┌───▼───┐ '    Action       ┌───────────┐
//!   │  '│Tunshell│   │Process│   │FileDownloader│  │TcpJson◄───────────────────►Application│
//!   │  '└────┬───┘   └───┬───┘   └──────┬───────┘  └───┬───┘ '  ActionResponse │ / Device  │
//!   │  '     │           │              │              │     '    / Data       └───────────┘
//!   │  ------│-----------│--------------│--------------│------         
//!   │        │           │              │              │
//!   └────────┴───────────┴──────────────┴──────────────┘
//!                   ActionResponse / Data
//!```
//! [`port`]: base::AppConfig#structfield.port
//! [`name`]: Action#structfield.name
use std::sync::Arc;
use std::thread;

use anyhow::Error;

use base::bridge::stream::Stream;
use base::monitor::Monitor;
use base::Compression;
use collector::device_shadow::DeviceShadow;
use collector::downloader::FileDownloader;
use collector::installer::OTAInstaller;
use collector::process::ProcessHandler;
use collector::systemstats::StatCollector;
use collector::tunshell::TunshellSession;
use flume::{bounded, Receiver, RecvError, Sender};
use log::error;

pub mod base;
pub mod collector;

pub mod config {
    use crate::base::{bridge::stream::MAX_BUFFER_SIZE, StreamConfig};
    pub use crate::base::{Config, Persistence, Stats};
    use config::{Environment, File, FileFormat};
    use std::fs;
    use structopt::StructOpt;

    #[derive(StructOpt, Debug)]
    #[structopt(name = "uplink", about = "collect, batch, compress, publish")]
    pub struct CommandLine {
        /// Binary version
        #[structopt(skip = env ! ("VERGEN_BUILD_SEMVER"))]
        pub version: String,
        /// Build profile
        #[structopt(skip = env ! ("VERGEN_CARGO_PROFILE"))]
        pub profile: String,
        /// Commit SHA
        #[structopt(skip = env ! ("VERGEN_GIT_SHA"))]
        pub commit_sha: String,
        /// Commit SHA
        #[structopt(skip = env ! ("VERGEN_GIT_COMMIT_TIMESTAMP"))]
        pub commit_date: String,
        /// config file
        #[structopt(short = "c", help = "Config file")]
        pub config: Option<String>,
        /// config file
        #[structopt(short = "a", help = "Authentication file")]
        pub auth: String,
        /// log level (v: info, vv: debug, vvv: trace)
        #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
        pub verbose: u8,
        /// list of modules to log
        #[structopt(short = "m", long = "modules")]
        pub modules: Vec<String>,
    }

    const DEFAULT_CONFIG: &str = r#"    
    [mqtt]
    max_packet_size = 256000
    max_inflight = 100
    keep_alive = 30
    network_timeout = 30

    # Create empty streams map
    [streams]

    # Downloader config
    [downloader]
    actions = []
    path = "/var/tmp/ota-file"

    [stream_metrics]
    enabled = false
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_stream_metrics/jsonarray"
    blacklist = []
    timeout = 10

    [serializer_metrics]
    enabled = false
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_serializer_metrics/jsonarray"
    timeout = 10

    [mqtt_metrics]
    enabled = true
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_mqtt_metrics/jsonarray"

    [action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    flush_period = 2

    [streams.device_shadow]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/device_shadow/jsonarray"
    flush_period = 5

    [streams.logs]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/logs/jsonarray"
    buf_size = 32

    [system_stats]
    enabled = true
    process_names = ["uplink"]
    update_period = 30
"#;

    /// Reads config file to generate config struct and replaces places holders
    /// like bike id and data version
    pub fn initialize(auth_config: &str, uplink_config: &str) -> Result<Config, anyhow::Error> {
        let config = config::Config::builder()
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml))
            .add_source(File::from_str(uplink_config, FileFormat::Toml))
            .add_source(File::from_str(auth_config, FileFormat::Json))
            .add_source(Environment::default())
            .build()?;

        let mut config: Config = config.try_deserialize()?;

        // Create directory at persistence_path if it doesn't already exist
        fs::create_dir_all(&config.persistence_path).map_err(|_| {
            anyhow::Error::msg(format!(
                "Permission denied for creating persistence directory at \"{}\"",
                config.persistence_path.display()
            ))
        })?;

        // replace placeholders with device/tenant ID
        let tenant_id = config.project_id.trim();
        let device_id = config.device_id.trim();
        for config in config.streams.values_mut() {
            replace_topic_placeholders(&mut config.topic, tenant_id, device_id);
        }

        replace_topic_placeholders(&mut config.action_status.topic, tenant_id, device_id);
        replace_topic_placeholders(&mut config.stream_metrics.topic, tenant_id, device_id);
        replace_topic_placeholders(&mut config.serializer_metrics.topic, tenant_id, device_id);
        replace_topic_placeholders(&mut config.mqtt_metrics.topic, tenant_id, device_id);

        // for config in [&mut config.serializer_metrics, &mut config.stream_metrics] {
        //     if let Some(topic) = &config.topic {
        //         let topic = topic.replace("{tenant_id}", tenant_id);
        //         let topic = topic.replace("{device_id}", device_id);
        //         config.topic = Some(topic);
        //     }
        // }

        if config.system_stats.enabled {
            for stream_name in [
                "uplink_disk_stats",
                "uplink_network_stats",
                "uplink_processor_stats",
                "uplink_process_stats",
                "uplink_component_stats",
                "uplink_system_stats",
            ] {
                config.stream_metrics.blacklist.push(stream_name.to_owned());
                let stream_config = StreamConfig {
                    topic: format!(
                        "/tenants/{tenant_id}/devices/{device_id}/events/{stream_name}/jsonarray"
                    ),
                    buf_size: config.system_stats.stream_size.unwrap_or(MAX_BUFFER_SIZE),
                    ..Default::default()
                };
                config.streams.insert(stream_name.to_owned(), stream_config);
            }
        }

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(buf_size) = config.logging.as_ref().and_then(|c| c.stream_size) {
            let stream_config =
                config.streams.entry("logs".to_string()).or_insert_with(|| StreamConfig {
                    topic: format!(
                        "/tenants/{tenant_id}/devices/{device_id}/events/logs/jsonarray"
                    ),
                    buf_size: 32,
                    ..Default::default()
                });
            stream_config.buf_size = buf_size;
        }

        let action_topic_template = "/tenants/{tenant_id}/devices/{device_id}/actions";
        let mut device_action_topic = action_topic_template.to_string();
        replace_topic_placeholders(&mut device_action_topic, tenant_id, device_id);
        config.actions_subscription = device_action_topic;

        // Add topics to be subscribed to for simulation purposes, if in simulator mode
        if let Some(sim_cfg) = &mut config.simulator {
            for n in 1..=sim_cfg.num_devices {
                let mut topic = action_topic_template.to_string();
                replace_topic_placeholders(&mut topic, tenant_id, &n.to_string());
                sim_cfg.actions_subscriptions.push(topic);
            }
        }

        Ok(config)
    }

    // Replace placeholders in topic strings with configured values for tenant_id and device_id
    fn replace_topic_placeholders(topic: &mut String, tenant_id: &str, device_id: &str) {
        *topic = topic.replace("{tenant_id}", tenant_id);
        *topic = topic.replace("{device_id}", device_id);
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ReadFileError {
        #[error("Auth file not found at {0}")]
        Auth(String),
        #[error("Config file not found at {0}")]
        Config(String),
    }

    fn read_file_contents(path: &str) -> Option<String> {
        fs::read_to_string(path).ok()
    }

    pub fn get_configs(
        commandline: &CommandLine,
    ) -> Result<(String, Option<String>), ReadFileError> {
        let auth = read_file_contents(&commandline.auth)
            .ok_or_else(|| ReadFileError::Auth(commandline.auth.to_string()))?;
        let config = match &commandline.config {
            Some(path) => Some(
                read_file_contents(path).ok_or_else(|| ReadFileError::Config(path.to_string()))?,
            ),
            None => None,
        };

        Ok((auth, config))
    }
}

pub use base::actions::{Action, ActionResponse};
use base::bridge::{Bridge, BridgeTx, Package, Payload, Point, StreamMetrics};
use base::mqtt::Mqtt;
use base::serializer::{Serializer, SerializerMetrics};
pub use base::{ActionRoute, Config};
pub use collector::{simulator, tcpjson::TcpJson};
pub use storage::Storage;

pub struct Uplink {
    config: Arc<Config>,
    action_rx: Receiver<Action>,
    action_tx: Sender<Action>,
    data_rx: Receiver<Box<dyn Package>>,
    data_tx: Sender<Box<dyn Package>>,
    action_status: Stream<ActionResponse>,
    stream_metrics_tx: Sender<StreamMetrics>,
    stream_metrics_rx: Receiver<StreamMetrics>,
    serializer_metrics_tx: Sender<SerializerMetrics>,
    serializer_metrics_rx: Receiver<SerializerMetrics>,
    shutdown_tx: Sender<()>,
    shutdown_rx: Receiver<()>,
}

impl Uplink {
    pub fn new(config: Arc<Config>) -> Result<Uplink, Error> {
        let (action_tx, action_rx) = bounded(10);
        let (data_tx, data_rx) = bounded(10);
        let (stream_metrics_tx, stream_metrics_rx) = bounded(10);
        let (serializer_metrics_tx, serializer_metrics_rx) = bounded(10);
        let (shutdown_tx, shutdown_rx) = bounded(1);

        let action_status_topic = &config.action_status.topic;
        let action_status = Stream::new(
            "action_status",
            action_status_topic,
            1,
            data_tx.clone(),
            Compression::Disabled,
        );
        Ok(Uplink {
            config,
            action_rx,
            action_tx,
            data_rx,
            data_tx,
            action_status,
            stream_metrics_tx,
            stream_metrics_rx,
            serializer_metrics_tx,
            serializer_metrics_rx,
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn spawn(&mut self) -> Result<BridgeTx, Error> {
        let config = self.config.clone();
        let mut bridge = Bridge::new(
            self.config.clone(),
            self.data_tx.clone(),
            self.stream_metrics_tx(),
            self.action_rx.clone(),
            self.action_status(),
            self.shutdown_tx.clone(),
        );

        let bridge_tx = bridge.tx();
        let (mqtt_metrics_tx, mqtt_metrics_rx) = bounded(10);

        // Bridge thread to batch data and redicet actions
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("bridge")
                .enable_time()
                .build()
                .unwrap();

            rt.block_on(async move {
                if let Err(e) = bridge.start().await {
                    error!("Bridge stopped!! Error = {:?}", e);
                }
            })
        });

        let mut mqtt = Mqtt::new(self.config.clone(), self.action_tx.clone(), mqtt_metrics_tx);
        let mqtt_client = mqtt.client();

        let serializer = Serializer::new(
            self.config.clone(),
            self.data_rx.clone(),
            mqtt_client.clone(),
            self.serializer_metrics_tx(),
        )?;

        // Serializer thread to handle network conditions state machine
        // and send data to mqtt thread
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("serializer")
                .enable_time()
                .build()
                .unwrap();

            rt.block_on(async {
                if let Err(e) = serializer.start().await {
                    error!("Serializer stopped!! Error = {:?}", e);
                }
            })
        });

        // Mqtt thread to receive actions and send data
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("mqttio")
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(async {
                mqtt.start().await;
            })
        });

        let tunshell_session = TunshellSession::new(config.clone(), false, bridge_tx.clone());
        thread::spawn(move || tunshell_session.start());

        let file_downloader = FileDownloader::new(config.clone(), bridge_tx.clone())?;
        thread::spawn(move || file_downloader.start());

        let device_shadow = DeviceShadow::new(config.device_shadow.clone(), bridge_tx.clone());
        thread::spawn(move || device_shadow.start());

        if let Some(config) = &config.ota_installer {
            let ota_installer = OTAInstaller::new(config.clone(), bridge_tx.clone());
            thread::spawn(move || ota_installer.start());
        }

        #[cfg(target_os = "linux")]
        if let Some(config) = &config.logging {
            let logger = collector::journalctl::JournalCtl::new(bridge_tx.clone());
            let config = config.clone();
            thread::spawn(move || logger.start(config));
        }

        #[cfg(target_os = "android")]
        if let Some(config) = &config.logging {
            let logger = collector::logcat::Logcat::new(bridge_tx.clone());
            let config = config.clone();
            thread::spawn(move || logger.start(config));
        }

        if config.system_stats.enabled {
            let stat_collector = StatCollector::new(config.clone(), bridge_tx.clone());
            thread::spawn(move || stat_collector.start());
        }

        let process_handler = ProcessHandler::new(bridge_tx.clone());
        let processes = config.processes.clone();
        thread::spawn(move || process_handler.start(processes));

        let monitor = Monitor::new(
            self.config.clone(),
            mqtt_client,
            self.stream_metrics_rx.clone(),
            self.serializer_metrics_rx.clone(),
            mqtt_metrics_rx,
        );

        // Metrics monitor thread
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("monitor")
                .enable_time()
                .build()
                .unwrap();

            rt.block_on(async move {
                if let Err(e) = monitor.start().await {
                    error!("Monitor stopped!! Error = {:?}", e);
                }
            })
        });

        Ok(bridge_tx)
    }

    pub fn bridge_action_rx(&self) -> Receiver<Action> {
        self.action_rx.clone()
    }

    pub fn bridge_data_tx(&self) -> Sender<Box<dyn Package>> {
        self.data_tx.clone()
    }

    pub fn stream_metrics_tx(&self) -> Sender<StreamMetrics> {
        self.stream_metrics_tx.clone()
    }

    pub fn serializer_metrics_tx(&self) -> Sender<SerializerMetrics> {
        self.serializer_metrics_tx.clone()
    }

    pub fn action_status(&self) -> Stream<ActionResponse> {
        self.action_status.clone()
    }

    pub async fn resolve_on_shutdown(&self) -> Result<(), RecvError> {
        self.shutdown_rx.recv_async().await
    }
}
