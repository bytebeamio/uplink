#[doc = include_str ! ("../../README.md")]
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use anyhow::Error;

use flume::{bounded, Receiver, Sender};
use log::error;
use tokio::task;

pub mod base;
pub mod collector;

pub mod config {
    use std::fs;
    use anyhow::Context;
    use figment::Figment;
    use figment::providers::{Data, Json, Toml};
    use structopt::StructOpt;
    pub use crate::base::{Config, Ota, Persistence, Stats};

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
        /// list of modules to log
        #[structopt(short = "s", long = "simulator")]
        pub simulator: bool,
        /// log level (v: info, vv: debug, vvv: trace)
        #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
        pub verbose: u8,
        /// list of modules to log
        #[structopt(short = "m", long = "modules")]
        pub modules: Vec<String>,
    }

    const DEFAULT_CONFIG: &str = r#"
    bridge_port = 5555
    max_packet_size = 102400
    max_inflight = 100

    # Whitelist of binaries which uplink can spawn as a process
    # This makes sure that user is protected against random actions
    # triggered from cloud.
    actions = ["tunshell"]

    [persistence]
    path = "/tmp/uplink"
    max_file_size = 104857600 # 100MB
    max_file_count = 3

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
    update_period = 30
"#;

    /// Reads config file to generate config struct and replaces places holders
    /// like bike id and data version
    pub fn initialize(auth_config: &str, uplink_config: &str) -> Result<Config, anyhow::Error> {
        let mut config = Figment::new().merge(Data::<Toml>::string(DEFAULT_CONFIG));

        config = config.merge(Data::<Toml>::string(uplink_config));

        let mut config: Config = config
            .join(Data::<Json>::string(auth_config))
            .extract()
            .with_context(|| "Config error".to_string())?;

        if let Some(persistence) = &config.persistence {
            fs::create_dir_all(&persistence.path)?;
        }
        let tenant_id = config.project_id.trim();
        let device_id = config.device_id.trim();
        for config in config.streams.values_mut() {
            let topic = str::replace(&config.topic, "{tenant_id}", tenant_id);
            config.topic = topic;

            let topic = str::replace(&config.topic, "{device_id}", device_id);
            config.topic = topic;
        }

        Ok(config)
    }
}

pub use base::actions;
use base::actions::ota::OtaDownloader;
use base::actions::tunshell::{Relay, TunshellSession};
use base::actions::Actions;
pub use base::actions::{Action, ActionResponse};
use base::mqtt::Mqtt;
use base::serializer::Serializer;
pub use base::{Config, Package, Point, Stream};
pub use collector::simulator::Simulator;
use collector::systemstats::StatCollector;
pub use collector::tcpjson::{Bridge, Payload};
pub use disk::Storage;

struct RxTx<T> {
    rx: Receiver<T>,
    tx: Sender<T>,
}

impl<T> RxTx<T> {
    fn bounded(cap: usize) -> RxTx<T> {
        let (tx, rx) = bounded(cap);

        RxTx { rx, tx }
    }
}

pub struct Uplink {
    config: Arc<Config>,
    action_channel: RxTx<Action>,
    data_channel: RxTx<Box<dyn Package>>,
    action_status: Stream<ActionResponse>,
}

impl Uplink {
    pub fn new(config: Arc<Config>) -> Result<Uplink, Error> {
        let action_channel = RxTx::bounded(10);
        let data_channel = RxTx::bounded(10);

        let action_status_topic = &config
            .streams
            .get("action_status")
            .ok_or_else(|| Error::msg("Action status topic missing from config"))?
            .topic;
        let action_status =
            Stream::new("action_status", action_status_topic, 1, data_channel.tx.clone());

        Ok(Uplink { config, action_channel, data_channel, action_status })
    }

    pub fn spawn(&mut self) -> Result<(), Error> {
        // Launch a thread to handle tunshell access
        let tunshell_keys = RxTx::bounded(10);
        let tunshell_config = self.config.clone();
        let tunshell_session = TunshellSession::new(
            tunshell_config,
            Relay::default(),
            false,
            tunshell_keys.rx,
            self.action_status.clone(),
        );
        thread::spawn(move || tunshell_session.start());

        // Launch a thread to handle downloads for OTA updates
        let (ota_tx, ota_downloader) = OtaDownloader::new(
            self.config.clone(),
            self.action_status.clone(),
            self.action_channel.tx.clone(),
        )?;
        if self.config.ota.enabled {
            thread::spawn(move || ota_downloader.start());
        }

        // Launch a thread to collect system statistics
        let stat_collector = StatCollector::new(self.config.clone(), self.data_channel.tx.clone());
        if self.config.stats.enabled {
            thread::spawn(move || stat_collector.start());
        }

        let raw_action_channel = RxTx::bounded(10);
        let mut mqtt = Mqtt::new(self.config.clone(), raw_action_channel.tx);
        let serializer =
            Serializer::new(self.config.clone(), self.data_channel.rx.clone(), mqtt.client())?;

        let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
        let actions = Actions::new(
            self.config.clone(),
            controllers,
            raw_action_channel.rx,
            tunshell_keys.tx,
            ota_tx,
            self.action_status.clone(),
            self.action_channel.tx.clone(),
        );

        // Launch a thread to handle incoming and outgoing MQTT packets
        let rt = tokio::runtime::Runtime::new()?;
        thread::spawn(move || {
            rt.block_on(async {
                // Collect and forward data from connected applications as MQTT packets
                task::spawn(async move {
                    if let Err(e) = serializer.start().await {
                        error!("Serializer stopped!! Error = {:?}", e);
                    }
                });

                // Receive [Action]s
                task::spawn(async move {
                    mqtt.start().await;
                });

                // Process and forward received [Action]s to connected applications
                actions.start().await;
            })
        });

        Ok(())
    }

    pub fn bridge_action_rx(&self) -> Receiver<Action> {
        self.action_channel.rx.clone()
    }

    pub fn bridge_data_tx(&self) -> Sender<Box<dyn Package>> {
        self.data_channel.tx.clone()
    }

    pub fn action_status(&self) -> Stream<ActionResponse> {
        self.action_status.clone()
    }
}
