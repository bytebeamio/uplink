use anyhow::Error;
use async_channel::{bounded, Sender};
use log::error;
use structopt::StructOpt;
use tokio::task;
use uplink::Uplink;

use std::{collections::HashMap, sync::Arc, thread};

use uplink::collector::{simulator::Simulator, tcpjson::Bridge};
use uplink::tunshell::{Relay, TunshellSession};
use uplink::{cli::CommandLine, Actions, Stream};

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    // Setup logging and other CLI options, print uplink banner
    let commandline: CommandLine = StructOpt::from_args();
    commandline.initialize_logging();
    let config = Arc::new(commandline.initalize_config()?);
    commandline.banner(&config);

    let (bridge_actions_tx, bridge_actions_rx) = bounded(10);
    let (tunshell_keys_tx, tunshell_keys_rx) = bounded(10);
    let (collector_tx, native_actions_rx) = Uplink::spawn_intefaces(config.clone())?;

    let action_status_topic = &config.streams.get("action_status").unwrap().topic;
    let action_status = Stream::new("action_status", action_status_topic, 1, collector_tx.clone());

    // If enabled, configure and spawn a simulator instance for uplink.
    // Else, configure and spawn a bridge for uplink.
    if !commandline.enable_simulator {
        let mut bridge = Bridge::new(
            config.clone(),
            collector_tx.clone(),
            bridge_actions_rx,
            action_status.clone(),
        );
        task::spawn(async move {
            if let Err(e) = bridge.start().await {
                error!("Bridge stopped!! Error = {:?}", e);
            }
        });
    } else {
        let mut simulator = Simulator::new(config.clone(), collector_tx.clone());
        task::spawn(async move {
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

    let controllers: HashMap<String, Sender<uplink::Control>> = HashMap::new();
    let mut actions = Actions::new(
        config.clone(),
        controllers,
        native_actions_rx,
        tunshell_keys_tx,
        action_status,
        bridge_actions_tx,
    );
    actions.start().await;
    Ok(())
}
