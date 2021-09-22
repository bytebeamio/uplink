use anyhow::{Context, Error};
use async_channel::{bounded, Sender};
use disk::Storage;
use log::error;
use structopt::StructOpt;
use tokio::task;

use std::{collections::HashMap, sync::Arc, thread};

mod base;
mod cli;
mod collector;

use crate::base::actions::tunshell::{Relay, TunshellSession};
use crate::base::{actions::Actions, mqtt::Mqtt, serializer::Serializer, Stream};
use crate::cli::CommandLine;
use crate::collector::{simulator::Simulator, tcpjson::Bridge};

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    // Setup logging and other CLI options, print uplink banner
    let commandline: CommandLine = StructOpt::from_args();
    commandline.initialize_logging();
    let config = Arc::new(commandline.initalize_config()?);
    commandline.banner(&config);

    let (collector_tx, collector_rx) = bounded(10);
    let (native_actions_tx, native_actions_rx) = bounded(10);
    let (bridge_actions_tx, bridge_actions_rx) = bounded(10);
    let (tunshell_keys_tx, tunshell_keys_rx) = bounded(10);

    let storage = Storage::new(
        &config.persistence.path,
        config.persistence.max_file_size,
        config.persistence.max_file_count,
    )
    .with_context(|| format!("Storage = {:?}", config.persistence))?;

    let action_status_topic = &config.streams.get("action_status").unwrap().topic;
    let action_status = Stream::new("action_status", action_status_topic, 1, collector_tx.clone());

    let mut mqtt_processor = Mqtt::new(config.clone(), native_actions_tx);
    let mut serializer = Serializer::new(config.clone(), collector_rx, mqtt_processor.client(), storage)?;

    task::spawn(async move {
        if let Err(e) = serializer.start().await {
            error!("Serializer stopped!! Error = {:?}", e);
        }
    });

    task::spawn(async move {
        mqtt_processor.start().await;
    });

    // Configure and spawn a bridge instance for uplink
    let mut bridge =
        Bridge::new(config.clone(), collector_tx.clone(), bridge_actions_rx, action_status.clone());
    task::spawn(async move {
        if let Err(e) = bridge.start().await {
            error!("Bridge stopped!! Error = {:?}", e);
        }
    });

    // If enabled, configure and spawn a simulator instance for uplink
    if commandline.enable_simulator {
        let simulator_config = config.clone();
        let data_tx = collector_tx.clone();
        task::spawn(async {
            let mut simulator = Simulator::new(simulator_config, data_tx);
            simulator.start().await;
        });
    }

    // Configure a tunshell session for uplink and spawn a thread to run it
    let tunshell_session = TunshellSession::new(
        config.clone(),
        Relay::default(),
        false,
        tunshell_keys_rx,
        action_status.clone(),
    );
    thread::spawn(move || tunshell_session.start());

    let mut actions = Actions::new(
        config.clone(),
        HashMap::<String, Sender<base::Control>>::new(),
        native_actions_rx,
        tunshell_keys_tx,
        action_status,
        bridge_actions_tx,
    )
    .await;
    actions.start().await;
    Ok(())
}
