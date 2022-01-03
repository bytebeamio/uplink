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

use crate::base::actions::tunshell::{Relay, TunshellSession};
use crate::base::actions::Actions;
use crate::base::mqtt::Mqtt;
use crate::base::serializer::Serializer;
pub use crate::base::Stream;
pub use crate::collector::simulator::Simulator;
use crate::collector::systemstats::StatCollector;
pub use crate::collector::tcpjson::{Bridge, Payload};
pub use base::actions::{Action, ActionResponse};
pub use base::{Config, Package, Point};
pub use disk::Storage;

pub fn spawn_uplink(
    config: Arc<Config>,
) -> Result<(Receiver<Action>, Sender<Box<dyn Package>>, Stream<ActionResponse>), Error> {
    let enable_stats = config.stats.enabled;

    let (native_actions_tx, native_actions_rx) = bounded(10);
    let (tunshell_keys_tx, tunshell_keys_rx) = bounded(10);
    let (collector_tx, collector_rx) = bounded(10);
    let (bridge_actions_tx, bridge_actions_rx) = bounded(10);

    let action_status_topic = &config.streams.get("action_status").unwrap().topic;
    let action_status = Stream::new("action_status", action_status_topic, 1, collector_tx.clone());

    let mut mqtt = Mqtt::new(config.clone(), native_actions_tx);
    let mut serializer = Serializer::new(config.clone(), collector_rx, mqtt.client())?;
    let bridge_rx = bridge_actions_rx.clone();
    let c_tx = collector_tx.clone();
    let status_stream = action_status.clone();

    let rt = tokio::runtime::Runtime::new().unwrap();
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
                let stat_collector = StatCollector::new(config.clone(), collector_tx.clone());
                thread::spawn(move || stat_collector.start());
            }

            let tunshell_config = config.clone();
            let tunshell_session = TunshellSession::new(
                tunshell_config,
                Relay::default(),
                false,
                tunshell_keys_rx,
                action_status.clone(),
            );
            thread::spawn(move || tunshell_session.start());

            let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
            let mut actions = Actions::new(
                config.clone(),
                controllers,
                native_actions_rx,
                tunshell_keys_tx,
                action_status,
                bridge_actions_tx,
            )
            .await;
            actions.start().await;
        })
    });

    Ok((bridge_rx, c_tx, status_stream))
}
