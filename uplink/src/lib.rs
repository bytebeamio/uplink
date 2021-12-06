#[doc = include_str!("../../README.md")]
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Error};
use base::{
    actions::{Action, ActionResponse},
    Package,
};
use flume::{bounded, Receiver, Sender};
use log::error;
use tokio::task;

mod base;
mod collector;

use crate::base::actions::{
    tunshell::{Relay, TunshellSession},
    Actions,
};
use crate::base::mqtt::Mqtt;
use crate::base::serializer::Serializer;
pub use crate::base::Stream;
pub use crate::collector::simulator::Simulator;
use crate::collector::systemstats::StatCollector;
pub use crate::collector::tcpjson::Bridge;
pub use base::Config;
pub use disk::Storage;

pub async fn spawn_uplink(
    config: Arc<Config>,
    bridge_actions_tx: Sender<Action>,
    collector_rx: Receiver<Box<dyn Package>>,
    collector_tx: Sender<Box<dyn Package>>,
    action_status: Stream<ActionResponse>,
) -> Result<(), Error> {
    let enable_stats = config.stats.enabled;

    let (native_actions_tx, native_actions_rx) = bounded(10);
    let (tunshell_keys_tx, tunshell_keys_rx) = bounded(10);

    let storage = Storage::new(
        &config.persistence.path,
        config.persistence.max_file_size,
        config.persistence.max_file_count,
    );
    let storage = storage.with_context(|| format!("Storage = {:?}", config.persistence))?;

    let mut mqtt = Mqtt::new(config.clone(), native_actions_tx);
    let mut serializer = Serializer::new(config.clone(), collector_rx, mqtt.client(), storage)?;

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

    Ok(())
}
