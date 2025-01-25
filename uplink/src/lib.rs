//! Uplink is a utility/library to interact with the Bytebeam platform. Its internal architecture is described in the diagram below.
//!
//! We use [`rumqttc`], which implements the MQTT protocol, to communicate with the platform. Communication is handled separately as ingress and egress
//! by [`Mqtt`] and [`Serializer`] respectively. [`Action`]s are received and forwarded by [`Mqtt`] to the [`Bridge`] module, where it is handled
//! depending on the [`name`], with [`Bridge`] forwarding it to one of many **Action Handlers**, configured with an [`ActionRoute`].
//!
//! Some of the action handlers are [`TcpJson`], [`ProcessHandler`], [`FileDownloader`] and [`TunshellClient`]. [`TcpJson`] forwards Actions received
//! from the platform to the application connected to it through the [`port`] and collects response data from these devices, to forward to the platform.
//! Response data can be of multiple types, of interest to us are [`ActionResponse`]s and data [`Payload`]s, which are forwarded to [`Bridge`] and from
//! there to the [`Serializer`], where depending on the network, it may be persisted in-memory or on-disk with [`Storage`].
//!
//!```text
//!                              ┌───────────┐
//!                              │MQTT broker│
//!                              └────┐▲─────┘
//!                            Action ││ ActionResponse
//!                                   ││ / Data
//!                        Action   ┌─▼└─┐
//!                    ┌────────────┤Mqtt◄──────────┐ ActionResponse
//!                    │            └────┘          │ / Data
//!                    │        ActionResponse ┌────┴─────┐   Publish   ┌───────┐
//!                    │       ┌───────┬───────►Serializer◄─────────────►Storage│
//!                    │       │       │ Data  └──────────┘   Packets   └───────┘
//!             ┌------│-------│-------│-----┐
//!             '┌─────▼─────┐ │  ┌────┴────┐'
//! ┌────────────►Action Lane├─┘  │Data Lane◄──────┐
//! │           '└──┬─┬─┬─┬──┘    └─────────┘'     │
//! │           '   │ │ │ │  Bridge          '     │
//! │           └---│-│-│-│------------------┘     │
//! │               │ │ │ └─────────────────────┐  │
//! │      ┌────────┘ │ └──────────┐            │  │
//! │┌-----│----------│------------│------------│--│--┐
//! │'     │          │Applications│            │  │  '
//! │'┌────▼───┐ ┌────▼──┐  ┌──────▼───────┐ ┌──▼──┴─┐'   Action       ┌───────────┐
//! │'│Tunshell│ │Process│  │FileDownloader│ │TcpJson◄─────────────────►Application│
//! │'└────┬───┘ └───┬───┘  └──────┬───────┘ └───┬───┘' ActionResponse │ / Device  │
//! │└-----│---------│-------------│-------------│----┘   / Data       └───────────┘
//! └──────┴─────────┴─────────────┴─────────────┘
//!                    ActionResponse
//!```
//! [`port`]: base::AppConfig#structfield.port
//! [`name`]: Action#structfield.name

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use ::config::{Environment, File, FileFormat};
use anyhow::Error;
use flume::{bounded, Receiver, Sender};
use log::{error, info};
use structopt::StructOpt;
use tracing_subscriber::{EnvFilter, Registry};
use tracing_subscriber::fmt::format::{Format, Pretty};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::reload::Handle;

pub mod console;
pub mod base;
pub mod collector;
pub mod uplink_config;
pub mod mock;
pub mod utils;

use self::uplink_config::{ActionRoute, Config, DeviceConfig};
pub use base::actions::{Action, ActionResponse};
use base::bridge::{stream::Stream, Bridge, Package, Payload, Point, StreamMetrics};
use base::monitor::Monitor;
use base::mqtt::Mqtt;
use base::serializer::{Serializer, SerializerMetrics};
use base::CtrlTx;
use collector::device_shadow::DeviceShadow;
use collector::downloader::{CtrlTx as DownloaderCtrlTx, FileDownloader};
use collector::installer::OTAInstaller;
#[cfg(target_os = "linux")]
use collector::journalctl::JournalCtl;
#[cfg(target_os = "android")]
use collector::logcat::Logcat;
use collector::preconditions::PreconditionChecker;
use collector::process::ProcessHandler;
use collector::script_runner::ScriptRunner;
use collector::systemstats::StatCollector;
use collector::tunshell::TunshellClient;
pub use collector::{simulator, tcpjson::TcpJson};
use crate::uplink_config::{AppConfig, StreamConfig, MAX_BATCH_SIZE};

pub type ReloadHandle =
Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

/// Spawn a named thread to run the function f on
pub fn spawn_named_thread<F>(name: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new().name(name.to_string()).spawn(f).unwrap();
}

pub struct Uplink {
    config: Arc<Config>,
    device_config: Arc<DeviceConfig>,
    action_rx: Receiver<Action>,
    action_tx: Sender<Action>,
    data_rx: Receiver<Box<dyn Package>>,
    data_tx: Sender<Box<dyn Package>>,
    stream_metrics_tx: Sender<StreamMetrics>,
    stream_metrics_rx: Receiver<StreamMetrics>,
    serializer_metrics_tx: Sender<SerializerMetrics>,
    serializer_metrics_rx: Receiver<SerializerMetrics>,
}

impl Uplink {
    pub fn new(config: Arc<Config>, device_config: Arc<DeviceConfig>) -> Result<Uplink, Error> {
        let (action_tx, action_rx) = bounded(10);
        let (data_tx, data_rx) = bounded(10);
        let (stream_metrics_tx, stream_metrics_rx) = bounded(10);
        let (serializer_metrics_tx, serializer_metrics_rx) = bounded(10);

        Ok(Uplink {
            config,
            device_config,
            action_rx,
            action_tx,
            data_rx,
            data_tx,
            stream_metrics_tx,
            stream_metrics_rx,
            serializer_metrics_tx,
            serializer_metrics_rx,
        })
    }

    pub fn configure_bridge(&mut self) -> Bridge {
        Bridge::new(
            self.config.clone(),
            self.device_config.clone(),
            self.data_tx.clone(),
            self.stream_metrics_tx(),
            self.action_rx.clone(),
        )
    }

    pub fn spawn(
        &mut self,
        device_config: &DeviceConfig,
        mut bridge: Bridge,
        downloader_disable: Arc<Mutex<bool>>,
        network_up: Arc<Mutex<bool>>,
    ) -> Result<CtrlTx, Error> {
        let (mqtt_metrics_tx, mqtt_metrics_rx) = bounded(10);
        let ctrl_data_lane = bridge.ctrl_tx();

        let mut mqtt = Mqtt::new(
            self.config.clone(),
            device_config,
            self.action_tx.clone(),
            mqtt_metrics_tx,
            network_up,
        );
        let mqtt_client = mqtt.client();
        let ctrl_mqtt = mqtt.ctrl_tx();

        let tenant_filter = format!("/tenants/{}/devices/{}", device_config.project_id, device_config.device_id);
        let (serializer_shutdown_tx, serializer_shutdown_rx) = flume::bounded(1);

        let (ctrl_tx, ctrl_rx) = bounded(1);
        let ctrl_downloader = DownloaderCtrlTx { inner: ctrl_tx };

        // Downloader thread if configured
        if !self.config.downloader.actions.is_empty() {
            let actions_rx = bridge.register_action_routes(&self.config.downloader.actions)?;
            let file_downloader = FileDownloader::new(
                self.config.clone(),
                &device_config.authentication,
                actions_rx,
                bridge.bridge_tx(),
                ctrl_rx,
                downloader_disable,
            )?;
            spawn_named_thread("File Downloader", || file_downloader.start());
        }

        // Serializer thread to handle network conditions state machine
        // and send data to mqtt thread
        {
            let mqtt_client = mqtt_client.clone();
            let config = self.config.clone();
            let data_rx = self.data_rx.clone();
            let serializer_metrics_tx = self.serializer_metrics_tx.clone();
            let serializer_shutdown_rx = serializer_shutdown_rx;
            spawn_named_thread("Serializer", move || {
                let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

                rt.block_on(async move {
                    let serializer = Serializer::new(
                        config,
                        tenant_filter,
                        data_rx,
                        mqtt_client,
                        serializer_metrics_tx,
                        serializer_shutdown_rx,
                    );
                    serializer.start().await;
                })
            });
        }

        // Mqtt thread to receive actions and send data
        spawn_named_thread("Mqttio", || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();

            rt.block_on(async {
                mqtt.start().await;
            })
        });

        let monitor = Monitor::new(
            self.config.clone(),
            mqtt_client,
            self.stream_metrics_rx.clone(),
            self.serializer_metrics_rx.clone(),
            mqtt_metrics_rx,
        );

        // Metrics monitor thread
        spawn_named_thread("Monitor", || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async move {
                if let Err(e) = monitor.start().await {
                    error!("Monitor stopped!! Error = {e}");
                }
            })
        });

        let Bridge { data: mut data_lane, actions: mut actions_lane, .. } = bridge;

        // Bridge thread to direct actions
        spawn_named_thread("Bridge actions_lane", || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async move {
                if let Err(e) = actions_lane.start().await {
                    error!("Actions lane stopped!! Error = {e}");
                }
            })
        });

        // Bridge thread to batch and forward data
        spawn_named_thread("Bridge data_lane", || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async move {
                if let Err(e) = data_lane.start().await {
                    error!("Data lane stopped!! Error = {e}");
                }
            })
        });

        Ok(CtrlTx {
            data_lane: ctrl_data_lane,
            mqtt: ctrl_mqtt,
            serializer: serializer_shutdown_tx,
            downloader: ctrl_downloader,
        })
    }

    pub fn spawn_builtins(&mut self, bridge: &mut Bridge) -> Result<(), Error> {
        let bridge_tx = bridge.bridge_tx();

        let route = ActionRoute {
            name: "launch_shell".to_owned(),
            cancellable: false,
        };
        let actions_rx = bridge.register_action_route(route)?;
        let tunshell_client = TunshellClient::new(actions_rx, bridge_tx.clone());
        spawn_named_thread("Tunshell Client", move || tunshell_client.start());

        let device_shadow = DeviceShadow::new(self.config.device_shadow.clone(), bridge_tx.clone());
        spawn_named_thread("Device Shadow Generator", move || device_shadow.start());

        if !self.config.ota_installer.actions.is_empty() {
            let actions_rx = bridge.register_action_routes(&self.config.ota_installer.actions)?;
            let ota_installer =
                OTAInstaller::new(self.config.ota_installer.clone(), actions_rx, bridge_tx.clone());
            spawn_named_thread("OTA Installer", move || ota_installer.start());
        }

        #[cfg(target_os = "linux")]
        if let Some(config) = self.config.logging.clone() {
            let route = ActionRoute {
                name: "journalctl_config".to_string(),
                cancellable: false,
            };
            let actions_rx = bridge.register_action_route(route)?;
            let logger = JournalCtl::new(config, actions_rx, bridge_tx.clone());
            spawn_named_thread("Logger", || {
                if let Err(e) = logger.start() {
                    error!("Logger stopped!! Error = {e}");
                }
            });
        }

        #[cfg(target_os = "android")]
        if let Some(config) = self.config.logging.clone() {
            let route = ActionRoute {
                name: "journalctl_config".to_string(),
                cancellable: false,
            };
            let actions_rx = bridge.register_action_route(route)?;
            let logger = Logcat::new(config, actions_rx, bridge_tx.clone());
            spawn_named_thread("Logger", || {
                if let Err(e) = logger.start() {
                    error!("Logger stopped!! Error = {e}");
                }
            });
        }

        if self.config.system_stats.enabled {
            let stat_collector = StatCollector::new(self.config.clone(), bridge_tx.clone());
            spawn_named_thread("Stat Collector", || stat_collector.start());
        };

        if !self.config.processes.is_empty() {
            let actions_rx = bridge.register_action_routes(&self.config.processes)?;
            let process_handler = ProcessHandler::new(actions_rx, bridge_tx.clone());
            spawn_named_thread("Process Handler", || {
                if let Err(e) = process_handler.start() {
                    error!("Process handler stopped!! Error = {e}");
                }
            });
        }

        if !self.config.script_runner.is_empty() {
            let actions_rx = bridge.register_action_routes(&self.config.script_runner)?;
            let script_runner = ScriptRunner::new(actions_rx, bridge_tx.clone());
            spawn_named_thread("Script Runner", || {
                if let Err(e) = script_runner.start() {
                    error!("Script runner stopped!! Error = {e}");
                }
            });
        }

        if let Some(checker_config) = &self.config.precondition_checks {
            let actions_rx = bridge.register_action_routes(&checker_config.actions)?;
            let checker = PreconditionChecker::new(self.config.clone(), actions_rx, bridge_tx);
            spawn_named_thread("Logger", || checker.start());
        }

        Ok(())
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
}



const DEFAULT_CONFIG: &str = r#"
    [mqtt]
    max_packet_size = 256000
    max_inflight = 100
    keep_alive = 30
    network_timeout = 30

    [stream_metrics]
    enabled = false
    bridge_topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_stream_metrics/jsonarray"
    serializer_topic = "/tenants/{tenant_id}/devices/{device_id}/events/uplink_serializer_stream_metrics/jsonarray"
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
    batch_size = 1
    flush_period = 2
    persistence = { max_file_count = 1 } # Ensures action responses are not lost on restarts
    priority = 255 # highest priority for quick delivery of action status info to platform

    [streams.device_shadow]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/device_shadow/jsonarray"
    flush_period = 5

    [streams.logs]
    topic = "/tenants/{tenant_id}/devices/{device_id}/events/logs/jsonarray"
    batch_size = 32

    [system_stats]
    enabled = true
    process_names = ["uplink"]
    update_period = 30
"#;

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
    /// config file
    #[structopt(short = "c", help = "Config file")]
    pub config: Option<PathBuf>,
    /// config file
    #[structopt(short = "a", help = "Authentication file")]
    pub auth: PathBuf,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbose: u8,
    /// list of modules to log
    #[structopt(short = "m", long = "modules")]
    pub modules: Vec<String>,
}

fn initialize_logging(log_level: u8, log_modules: Vec<String>) -> ReloadHandle {
    let level = match log_level {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    let levels =
        match log_modules.into_iter().reduce(|e, acc| format!("{e}={level},{acc}")) {
            Some(f) => format!("{f}={level}"),
            _ => format!("{level}"),
        };

    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(levels)
        .with_filter_reloading();

    let reload_handle = builder.reload_handle();

    builder.try_init().expect("initialized subscriber succesfully");

    reload_handle
}

fn parse_config(device_json: &str, config_toml: &str) -> Result<(Config, DeviceConfig), Error> {
    let mut config =
        config::Config::builder().add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml));

    config = config.add_source(File::from_str(config_toml, FileFormat::Toml));

    let mut config: Config =
        config.add_source(Environment::default()).build()?.try_deserialize()?;

    // Create directory at persistence_path if it doesn't already exist
    std::fs::create_dir_all(&config.persistence_path).map_err(|_| {
        Error::msg(format!(
            "Permission denied for creating persistence directory at {:?}",
            config.persistence_path.display()
        ))
    })?;

    let device_config: DeviceConfig = config::Config::builder()
        .add_source(File::from_str(device_json, FileFormat::Json))
        .add_source(Environment::default())
        .build()?
        .try_deserialize()?;

    // replace placeholders with device/tenant ID
    let tenant_id = device_config.project_id.trim();
    let device_id = device_config.device_id.trim();

    // Replace placeholders in topic strings with configured values for tenant_id and device_id
    // e.g. for tenant_id: "demo"; device_id: "123"
    // "/tenants/{tenant_id}/devices/{device_id}/events/stream/jsonarry" ~> "/tenants/demo/devices/123/events/stream/jsonarry"
    let replace_topic_placeholders = |topic: &mut String| {
        *topic = topic.replace("{tenant_id}", tenant_id).replace("{device_id}", device_id);
    };

    for (stream_name, stream_config) in config.streams.iter_mut() {
        stream_name.clone_into(&mut stream_config.name);
        replace_topic_placeholders(&mut stream_config.topic);
    }

    "action_status".clone_into(&mut config.action_status.name);
    replace_topic_placeholders(&mut config.action_status.topic);
    replace_topic_placeholders(&mut config.stream_metrics.bridge_topic);
    replace_topic_placeholders(&mut config.stream_metrics.serializer_topic);
    replace_topic_placeholders(&mut config.serializer_metrics.topic);
    replace_topic_placeholders(&mut config.mqtt_metrics.topic);

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
                batch_size: config.system_stats.stream_size.unwrap_or(MAX_BATCH_SIZE),
                ..Default::default()
            };
            config.streams.insert(stream_name.to_owned(), stream_config);
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Some(batch_size) = config.logging.as_ref().and_then(|c| c.stream_size) {
        let stream_config =
            config.streams.entry("logs".to_string()).or_insert_with(|| StreamConfig {
                topic: format!(
                    "/tenants/{tenant_id}/devices/{device_id}/events/logs/jsonarray"
                ),
                batch_size: 32,
                ..Default::default()
            });
        stream_config.batch_size = batch_size;
    }

    config.actions_subscription = format!("/tenants/{tenant_id}/devices/{device_id}/actions");

    // downloader actions are cancellable by default
    for route in config.downloader.actions.iter_mut() {
        route.cancellable = true;
    }

    // process actions are cancellable by default
    for route in config.processes.iter_mut() {
        route.cancellable = true;
    }

    // script runner actions are cancellable by default
    for route in config.script_runner.iter_mut() {
        route.cancellable = true;
    }

    Ok((config, device_config))
}

fn banner(config: &Config, device_config: &DeviceConfig) {
    const B: &str = r#"
        ░█░▒█░▄▀▀▄░█░░░▀░░█▀▀▄░█░▄
        ░█░▒█░█▄▄█░█░░░█▀░█░▒█░█▀▄
        ░░▀▀▀░█░░░░▀▀░▀▀▀░▀░░▀░▀░▀
        "#;

    println!("{B}");
    println!("    version: {}", env!("VERGEN_BUILD_SEMVER"));
    println!("    profile: {}", env!("VERGEN_CARGO_PROFILE"));
    println!("    commit_sha: {}", env!("VERGEN_GIT_SHA"));
    println!("    commit_date: {}", env!("VERGEN_GIT_COMMIT_TIMESTAMP"));
    println!("    project_id: {}", device_config.project_id);
    println!("    device_id: {}", device_config.device_id);
    println!("    remote: {}:{}", device_config.broker, device_config.port);
    println!("    persistence_path: {}", config.persistence_path.display());
    if !config.action_redirections.is_empty() {
        println!("    action redirections:");
        for (action, redirection) in config.action_redirections.iter() {
            println!("\t{action} -> {redirection}");
        }
    }
    if !config.tcpapps.is_empty() {
        println!("    tcp applications:");
        for (app, AppConfig { port, actions }) in config.tcpapps.iter() {
            println!("\tname: {app:?}\n\tport: {port}\n\tactions: {actions:?}\n\t@");
        }
    }
    println!("    secure_transport: {}", device_config.authentication.is_some());
    println!("    max_packet_size: {}", config.mqtt.max_packet_size);
    println!("    max_inflight_messages: {}", config.mqtt.max_inflight);
    println!("    keep_alive_timeout: {}", config.mqtt.keep_alive);

    if !config.downloader.actions.is_empty() {
        println!(
            "    downloader:\n\tpath: {:?}\n\tactions: {:?}",
            config.downloader.path.display(),
            config.downloader.actions
        );
    }
    if !config.ota_installer.actions.is_empty() {
        println!(
            "    installer:\n\tpath: {}\n\tactions: {:?}",
            config.ota_installer.path, config.ota_installer.actions
        );
    }
    if config.system_stats.enabled {
        println!("    processes: {:?}", config.system_stats.process_names);
    }
    if config.console.enabled {
        println!("    console: http://localhost:{}", config.console.port);
    }
    println!("\n");
}

pub fn entrypoint(device_json: String, config_toml: String, log_level: u8, log_modules: Vec<String>, print_banner: bool) -> Result<(CtrlTx, Receiver<()>), Error> {
    let (config, device_config) = parse_config(device_json.as_str(), config_toml.as_str())?;
    if print_banner {
        banner(&config, &device_config);
    }
    let reload_handle = initialize_logging(log_level, log_modules);
    let config = Arc::new(config);
    let device_config = Arc::new(device_config);
    let mut uplink = Uplink::new(config.clone(), device_config.clone())?;
    let mut bridge = uplink.configure_bridge();
    uplink.spawn_builtins(&mut bridge)?;

    let bridge_tx = bridge.bridge_tx();

    let mut tcpapps = vec![];
    for (app, cfg) in config.tcpapps.clone() {
        let route_rx = if !cfg.actions.is_empty() {
            let actions_rx = bridge.register_action_routes(&cfg.actions)?;
            Some(actions_rx)
        } else {
            None
        };
        tcpapps.push(TcpJson::new(app, cfg, route_rx, bridge.bridge_tx()));
    }

    let simulator_actions = match &config.simulator {
        Some(cfg) if !cfg.actions.is_empty() => {
            let actions_rx = bridge.register_action_routes(&cfg.actions)?;
            Some(actions_rx)
        }
        _ => None,
    };

    let downloader_disable = Arc::new(Mutex::new(false));
    let network_up = Arc::new(Mutex::new(false));
    let ctrl_tx =
        uplink.spawn(&device_config, bridge, downloader_disable.clone(), network_up.clone())?;

    if let Some(config) = config.simulator.clone() {
        spawn_named_thread("Simulator", || {
            simulator::start(config, bridge_tx, simulator_actions).unwrap();
        });
    }

    if config.console.enabled {
        let port = config.console.port;
        let ctrl_tx = ctrl_tx.clone();
        spawn_named_thread("Uplink Console", move || {
            console::start(
                port, reload_handle, ctrl_tx, downloader_disable, network_up,
                config.console.enable_events.then(|| config.persistence_path.join("events.db"))
            )
        });
    }

    let (end_tx, end_rx) = bounded::<()>(1);
    {
        let ctrl_tx = ctrl_tx.clone();
        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .thread_name("tcpjson")
                .build()
                .unwrap();

            rt.block_on(async {
                for app in tcpapps {
                    tokio::task::spawn(async move {
                        app.start().await;
                    });
                }

                #[cfg(unix)]
                {
                    use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
                    use signal_hook_tokio::Signals;
                    use tokio_stream::StreamExt;

                    let mut signals = Signals::new([SIGTERM, SIGINT, SIGQUIT]).unwrap();
                    while let Some(signal) = signals.next().await {
                        match signal {
                            SIGTERM | SIGINT | SIGQUIT => {
                                ctrl_tx.trigger_shutdown();
                                break;
                            },
                            s => tracing::error!("Couldn't handle signal: {s}"),
                        }
                    }
                };
            });
            info!("Uplink shutting down...");
            thread::sleep(Duration::from_secs(5));

            let _ = end_tx.send(());
        });
    }
    Ok((ctrl_tx, end_rx))
}

pub struct UplinkController {
    pub shutdown: CtrlTx,
    pub end_rx: Receiver<()>,
    pub data_lane: Sender<Payload>,
}
