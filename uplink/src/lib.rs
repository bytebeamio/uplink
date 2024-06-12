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
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Error;
use collector::bus::Bus;
use config::DEFAULT_TIMEOUT;
use flume::{bounded, Receiver, RecvError, Sender};
use log::error;

pub mod base;
pub mod collector;
pub mod config;

use self::config::{ActionRoute, Config};
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
pub use storage::Storage;

/// Spawn a named thread to run the function f on
pub fn spawn_named_thread<F>(name: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new().name(name.to_string()).spawn(f).unwrap();
}

pub struct Uplink {
    config: Arc<Config>,
    action_rx: Receiver<Action>,
    action_tx: Sender<Action>,
    data_rx: Receiver<Box<dyn Package>>,
    data_tx: Sender<Box<dyn Package>>,
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

        Ok(Uplink {
            config,
            action_rx,
            action_tx,
            data_rx,
            data_tx,
            stream_metrics_tx,
            stream_metrics_rx,
            serializer_metrics_tx,
            serializer_metrics_rx,
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn configure_bridge(&mut self) -> Bridge {
        Bridge::new(
            self.config.clone(),
            self.data_tx.clone(),
            self.stream_metrics_tx(),
            self.action_rx.clone(),
            self.shutdown_tx.clone(),
        )
    }

    pub fn spawn(
        &mut self,
        mut bridge: Bridge,
        downloader_disable: Arc<Mutex<bool>>,
    ) -> Result<CtrlTx, Error> {
        let (mqtt_metrics_tx, mqtt_metrics_rx) = bounded(10);
        let (ctrl_actions_lane, ctrl_data_lane) = bridge.ctrl_tx();

        let mut mqtt = Mqtt::new(self.config.clone(), self.action_tx.clone(), mqtt_metrics_tx);
        let mqtt_client = mqtt.client();
        let ctrl_mqtt = mqtt.ctrl_tx();

        let serializer = Serializer::new(
            self.config.clone(),
            self.data_rx.clone(),
            mqtt_client.clone(),
            self.serializer_metrics_tx(),
        )?;
        let ctrl_serializer = serializer.ctrl_tx();

        let (ctrl_tx, ctrl_rx) = bounded(1);
        let ctrl_downloader = DownloaderCtrlTx { inner: ctrl_tx };

        // Downloader thread if configured
        if !self.config.downloader.actions.is_empty() {
            let actions_rx = bridge.register_action_routes(&self.config.downloader.actions)?;
            let file_downloader = FileDownloader::new(
                self.config.clone(),
                actions_rx,
                bridge.bridge_tx(),
                ctrl_rx,
                downloader_disable,
            )?;
            spawn_named_thread("File Downloader", || file_downloader.start());
        }

        if let Some(cfg) = &self.config.bus {
            let bridge_tx = bridge.bridge_tx();
            let actions_rx = bridge.register_action_routes([ActionRoute {
                name: "*".to_string(),
                timeout: DEFAULT_TIMEOUT,
                cancellable: false,
            }])?;
            let bus = Bus::new(cfg.clone(), bridge_tx, actions_rx)?;
            spawn_named_thread("BusRx", move || bus.start());
        }

        // Serializer thread to handle network conditions state machine
        // and send data to mqtt thread
        spawn_named_thread("Serializer", || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();

            rt.block_on(async {
                if let Err(e) = serializer.start().await {
                    error!("Serializer stopped!! Error = {e}");
                }
            })
        });

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
            actions_lane: ctrl_actions_lane,
            data_lane: ctrl_data_lane,
            mqtt: ctrl_mqtt,
            serializer: ctrl_serializer,
            downloader: ctrl_downloader,
        })
    }

    pub fn spawn_builtins(&mut self, bridge: &mut Bridge) -> Result<(), Error> {
        let bridge_tx = bridge.bridge_tx();

        let route = ActionRoute {
            name: "launch_shell".to_owned(),
            timeout: Duration::from_secs(10),
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
                timeout: Duration::from_secs(10),
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
                timeout: Duration::from_secs(10),
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

    pub async fn resolve_on_shutdown(&self) -> Result<(), RecvError> {
        self.shutdown_rx.recv_async().await
    }
}
