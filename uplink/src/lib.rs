#[doc = include_str!("../../README.md")]
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use anyhow::Error;

use flume::{bounded, Receiver, Sender};
use log::error;
use tokio::task;

mod base;
mod collector;

pub mod config {
    pub use crate::base::{Config, Ota, Persistence, Stats};
}

use base::actions::tunshell::{Relay, TunshellSession};
use base::actions::Actions;
pub use base::actions::{Action, ActionResponse};
use base::mqtt::Mqtt;
use base::serializer::Serializer;
pub use base::{Config, Stream};
pub use base::{Package, Point};
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
        let action_status = Stream::new(
            "action_status",
            action_status_topic,
            1,
            data_channel.tx.clone(),
        );

        Ok(Uplink { config, action_channel, data_channel, action_status })
    }

    pub fn spawn(&mut self) -> Result<(), Error> {
        let raw_action_channel = RxTx::bounded(10);
        let mut mqtt = Mqtt::new(self.config.clone(), raw_action_channel.tx);
        let mut serializer =
            Serializer::new(self.config.clone(), self.data_channel.rx.clone(), mqtt.client())?;

        let tunshell_keys = RxTx::bounded(10);
        let tunshell_config = self.config.clone();
        let tunshell_session = TunshellSession::new(
            tunshell_config,
            Relay::default(),
            false,
            tunshell_keys.rx,
            self.action_status.clone(),
        );

        let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
        let mut actions = Actions::new(
            self.config.clone(),
            controllers,
            raw_action_channel.rx,
            tunshell_keys.tx,
            self.action_status.clone(),
            self.action_channel.tx.clone(),
        );

        let enable_stats = self.config.stats.enabled;
        let stat_collector = StatCollector::new(self.config.clone(), self.data_channel.tx.clone());

        let rt = tokio::runtime::Runtime::new()?;
        thread::spawn(move || {
            rt.block_on(async {
                task::spawn(async move {
                    if let Err(e) = serializer.start().await {
                        error!("Serializer stopped!! Error = {:?}", e);
                    }
                });

                task::spawn(async move {
                    mqtt.start().await;
                });

                if enable_stats {
                    thread::spawn(move || stat_collector.start());
                }

                thread::spawn(move || tunshell_session.start());
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
