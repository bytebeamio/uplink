#[doc = include_str ! ("../../README.md")]
use std::sync::Arc;
use std::thread;

use anyhow::Error;
use flume::{bounded, Receiver, Sender};
use log::error;

use base::monitor::Monitor;
use bridge::{Bridge, BridgeTx, Stream, StreamMetrics};
use collector::downloader::FileDownloader;
use collector::installer::OTAInstaller;
use collector::process::ProcessHandler;
use collector::systemstats::StatCollector;
use collector::tunshell::TunshellSession;

pub mod collector;

use base::mqtt::Mqtt;
pub use base::Config;
pub use collector::{simulator, tcpjson::TcpJson};
use protocol::{Action, ActionResponse, Package};
use serializer::{Serializer, SerializerMetrics};

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

        let action_status_topic = &config.bridge.action_status.topic;
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
        let config = self.config.clone();
        let mut bridge = Bridge::new(
            self.config.bridge.clone(),
            self.data_tx.clone(),
            self.stream_metrics_tx(),
            self.action_rx.clone(),
            self.action_status(),
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
            self.config.serializer.clone(),
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

        let ota_installer = OTAInstaller::new(config.clone(), bridge_tx.clone());
        thread::spawn(move || ota_installer.start());

        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let logger = collector::logging::LoggerInstance::new(
                config.clone(),
                self.data_tx.clone(),
                bridge_tx.clone(),
            );
            thread::spawn(move || logger.start());
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
}
