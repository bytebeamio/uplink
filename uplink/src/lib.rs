#[doc = include_str ! ("../../README.md")]
use std::sync::Arc;
use std::thread;

use anyhow::Error;

use base::monitor::Monitor;
use flume::{bounded, Receiver, Sender};
use log::error;
use tokio::task;

pub mod base;
pub mod collector;

pub mod config {
    use crate::base::StreamConfig;
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
    bridge_port = 5555
    run_logcat = true
    max_packet_size = 102400
    max_inflight = 100
    keep_alive = 60

    # Whitelist of binaries which uplink can spawn as a process
    # This makes sure that user is protected against random actions
    # triggered from cloud.
    actions = ["tunshell"]

    # Create empty streams map
    [streams]

    [stream_metrics]
    enabled = false
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_stream_metrics/jsonarray"
    timeout = 10

    [serializer_metrics]
    enabled = false
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_serializer_metrics/jsonarray"
    blacklist = []

    [action_status]
    topic = "/tenants/{tenant_id}/devices/{device_id}/action/status"
    buf_size = 1

    [stats]
    enabled = false
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

        if config.simulator.is_some() {
            config.device_id = "+".to_string();
        }

        if let Some(persistence) = &config.persistence {
            fs::create_dir_all(&persistence.path)?;
        }

        // replace placeholders with device/tenant ID
        let tenant_id = config.project_id.trim();
        let device_id = config.device_id.trim();
        for config in config.streams.values_mut() {
            replace_topic_placeholders(config, tenant_id, device_id);
        }

        replace_topic_placeholders(&mut config.action_status, tenant_id, device_id);

        // for config in [&mut config.serializer_metrics, &mut config.stream_metrics] {
        //     if let Some(topic) = &config.topic {
        //         let topic = topic.replace("{tenant_id}", tenant_id);
        //         let topic = topic.replace("{device_id}", device_id);
        //         config.topic = Some(topic);
        //     }
        // }

        for stat in [
            "uplink_disk_stats",
            "uplink_network_stats",
            "uplink_processor_stats",
            "uplink_process_stats",
            "uplink_component_stats",
            "uplink_system_stats",
        ] {
            config.stream_metrics.blacklist.push(stat.to_owned());
        }

        Ok(config)
    }

    // Replace placeholders in topic strings with configured values for tenant_id and device_id
    fn replace_topic_placeholders(config: &mut StreamConfig, tenant_id: &str, device_id: &str) {
        if let Some(topic) = &config.topic {
            let topic = topic.replace("{tenant_id}", tenant_id);
            let topic = topic.replace("{device_id}", device_id);
            config.topic = Some(topic);
        }
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
use base::bridge::{self, Bridge, BridgeTx, Package, Payload, Point, Stream, StreamMetrics};
pub use base::middleware;
use base::mqtt::Mqtt;
use base::serializer::{Serializer, SerializerMetrics};
pub use base::Config;
pub use collector::{simulator, tcpjson::TcpJson};
pub use disk::Storage;

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
}

impl Uplink {
    pub fn new(config: Arc<Config>) -> Result<Uplink, Error> {
        let (action_tx, action_rx) = bounded(10);
        let (data_tx, data_rx) = bounded(10);
        let (stream_metrics_tx, stream_metrics_rx) = bounded(10);
        let (serializer_metrics_tx, serializer_metrics_rx) = bounded(10);

        let action_status_topic = &config
            .action_status
            .topic
            .as_ref()
            .ok_or_else(|| Error::msg("Action status topic missing from config"))?;

        let action_status = Stream::new("action_status", action_status_topic, 1, data_tx.clone());
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
        })
    }

    pub fn spawn(&mut self) -> Result<BridgeTx, Error> {
        let mut bridge = Bridge::new(
            self.config.clone(),
            self.data_tx.clone(),
            self.stream_metrics_tx().clone(),
            self.action_rx.clone(),
            self.action_status(),
        );

        let bridge_tx = bridge.tx();

        // Bridge thread to batch data and redicet actions
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("bridge")
                .build()
                .unwrap();

            rt.block_on(async {
                task::spawn(async move {
                    if let Err(e) = bridge.start().await {
                        error!("Bridge stopped!! Error = {:?}", e);
                    }
                })
            })
        });

        let mut mqtt = Mqtt::new(self.config.clone(), self.action_tx.clone());
        let mqtt_client = mqtt.client();

        let serializer =
            Serializer::new(self.config.clone(), self.data_rx.clone(), mqtt_client.clone())?;
        // Serializer thread to handle network conditions state machine
        // and send data to mqtt thread
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("serializer")
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
                .build()
                .unwrap();

            rt.block_on(async {
                mqtt.start().await;
            })
        });

        let monitor = Monitor::new(
            self.config.clone(),
            mqtt_client.clone(),
            self.stream_metrics_rx.clone(),
            self.serializer_metrics_rx.clone(),
        );

        // Metrics monitor thread
        thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .thread_name("monitor")
                .build()
                .unwrap();

            rt.block_on(async {
                task::spawn(async move {
                    if let Err(e) = monitor.start().await {
                        error!("Monitor stopped!! Error = {:?}", e);
                    }
                })
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
}
