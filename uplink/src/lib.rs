#[doc = include_str ! ("../../README.md")]
use std::sync::Arc;
use std::thread;

use anyhow::Error;

use flume::{bounded, Receiver, Sender};
use tokio::task;
use tracing::error;

pub mod base;
pub mod collector;
pub mod config;

use crate::config::Config;
pub use base::actions::{Action, ActionResponse};
use base::middleware::tunshell::TunshellSession;
use base::middleware::Middleware;
use base::mqtt::Mqtt;
use base::serializer::Serializer;
pub use base::{middleware, Package, Payload, Point, Stream};
use collector::downloader::FileDownloader;
use collector::systemstats::StatCollector;
pub use collector::{simulator, tcpjson::Bridge};
pub use disk::Storage;

pub struct Uplink {
    config: Arc<Config>,
    action_rx: Receiver<Action>,
    action_tx: Sender<Action>,
    data_rx: Receiver<Box<dyn Package>>,
    data_tx: Sender<Box<dyn Package>>,
    action_status: Stream<ActionResponse>,
}

impl Uplink {
    pub fn new(config: Arc<Config>) -> Result<Uplink, Error> {
        let (action_tx, action_rx) = bounded(10);
        let (data_tx, data_rx) = bounded(10);

        let action_status_topic = &config
            .action_status
            .topic
            .as_ref()
            .ok_or_else(|| Error::msg("Action status topic missing from config"))?;
        let action_status = Stream::new("action_status", action_status_topic, 1, data_tx.clone());

        Ok(Uplink { config, action_rx, action_tx, data_rx, data_tx, action_status })
    }

    pub fn spawn(&mut self) -> Result<(), Error> {
        // Launch a thread to handle tunshell access
        let (tunshell_keys_tx, tunshell_keys_rx) = bounded(10);
        let tunshell_config = self.config.clone();
        let tunshell_session = TunshellSession::new(
            tunshell_config,
            false,
            tunshell_keys_rx,
            self.action_status.clone(),
        );
        thread::spawn(move || tunshell_session.start());

        // Launch a thread to handle file downloads
        let download_tx = if let Some(downloader_cfg) = self.config.downloader.clone() {
            let (download_tx, file_downloader) = FileDownloader::new(
                downloader_cfg,
                self.config.authentication.clone(),
                self.action_status.clone(),
                self.action_tx.clone(),
            )?;
            thread::spawn(move || file_downloader.start());

            download_tx
        } else {
            let (tx, _) = bounded(10);

            tx
        };

        // Launch a thread to collect system statistics
        let stat_collector = StatCollector::new(self.config.clone(), self.data_tx.clone());
        if self.config.stats.enabled {
            thread::spawn(move || stat_collector.start());
        }

        let (log_tx, log_rx) = bounded(10);
        // Launch log collector thread
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let logger = collector::logging::LoggerInstance::new(
                self.config.clone(),
                self.data_tx.clone(),
                log_rx,
            );
            thread::spawn(|| {
                if let Err(e) = logger.start() {
                    error!("Error running logger: {}", e);
                }
            });
        }

        let (raw_action_tx, raw_action_rx) = bounded(10);
        let mut mqtt = Mqtt::new(self.config.clone(), raw_action_tx);

        let serializer = Serializer::new(self.config.clone(), self.data_rx.clone(), mqtt.client())?;

        let actions = Middleware::new(
            self.config.clone(),
            raw_action_rx,
            tunshell_keys_tx,
            download_tx,
            log_tx,
            self.action_status.clone(),
            self.action_tx.clone(),
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
        self.action_rx.clone()
    }

    pub fn bridge_data_tx(&self) -> Sender<Box<dyn Package>> {
        self.data_tx.clone()
    }

    pub fn action_status(&self) -> Stream<ActionResponse> {
        self.action_status.clone()
    }
}
