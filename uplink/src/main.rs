#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::thread;

use anyhow::{Context, Error};
use async_channel::{bounded, Sender};
use figment::{providers::Format, providers::Json, providers::Toml, Figment};
use simplelog::{CombinedLogger, LevelFilter, LevelPadding, TermLogger, TerminalMode};
use structopt::StructOpt;
use tokio::task;

mod base;
mod collector;

use crate::base::actions::{
    self,
    tunshell::{Relay, TunshellSession},
};
use crate::base::mqtt::Mqtt;
use crate::base::serializer::Serializer;

use crate::collector::simulator::Simulator;
use crate::collector::tcpjson::Bridge;

use base::Config;
use disk::Storage;
use std::sync::Arc;

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
    /// config file
    #[structopt(short = "c", help = "Config file")]
    config: String,
    /// config file
    #[structopt(short = "a", help = "Authentication file")]
    auth: String,
    /// list of modules to log
    #[structopt(short = "s", long = "simulator")]
    simulator: bool,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,
    /// list of modules to log
    #[structopt(short = "m", long = "modules")]
    modules: Vec<String>,
}

/// Reads config file to generate config struct and replaces places holders
/// like bike id and data version
fn initalize_config(commandline: CommandLine) -> Result<Config, Error> {
    let mut config: Config = Figment::new()
        .merge(Toml::file(&commandline.config))
        .join(Json::file(&commandline.auth))
        .extract()
        .with_context(|| format!("Config = {}", commandline.config))?;

    let tenant_id = config.project_id.trim();
    let device_id = config.device_id.trim();
    for config in config.streams.values_mut() {
        let topic = str::replace(&config.topic, "{tenant_id}", tenant_id);
        config.topic = topic;

        let topic = str::replace(&config.topic, "{device_id}", device_id);
        config.topic = topic;
    }

    dbg!(&config);
    Ok(config)
}

fn initialize_logging(commandline: &CommandLine) {
    let level = match commandline.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    let mut config = simplelog::ConfigBuilder::new();
    config
        .set_location_level(LevelFilter::Off)
        .set_target_level(LevelFilter::Error)
        .set_thread_level(LevelFilter::Error)
        .set_level_padding(LevelPadding::Right);

    if commandline.modules.is_empty() {
        config.add_filter_allow_str("uplink").add_filter_allow_str("disk");
    } else {
        for module in commandline.modules.iter() {
            config.add_filter_allow(format!("{}", module));
        }
    }

    let loggers = TermLogger::new(level, config.build(), TerminalMode::Mixed);
    CombinedLogger::init(vec![loggers]).unwrap();
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    let commandline: CommandLine = StructOpt::from_args();
    let enable_simulator = commandline.simulator;

    initialize_logging(&commandline);
    let config = Arc::new(initalize_config(commandline)?);

    let (collector_tx, collector_rx) = bounded(10);
    let (native_actions_tx, native_actions_rx) = bounded(10);
    let (bridge_actions_tx, bridge_actions_rx) = bounded(10);
    let (tunshell_keys_tx, tunshell_keys_rx) = bounded(10);

    let storage = Storage::new(&config.persistence.path, config.persistence.max_file_size, config.persistence.max_file_count);
    let storage = storage.with_context(|| format!("Storage = {:?}", config.persistence))?;

    let mut mqtt = Mqtt::new(config.clone(), native_actions_tx, bridge_actions_tx);
    let mut serializer = Serializer::new(config.clone(), collector_rx, mqtt.client(), storage)?;

    task::spawn(async move {
        if let Err(e) = serializer.start().await {
            error!("Serializer stopped!! Error = {:?}", e);
        }
    });

    task::spawn(async move {
        mqtt.start().await;
    });

    let mut bridge = Bridge::new(config.clone(), collector_tx.clone(), bridge_actions_rx);
    task::spawn(async move {
        if let Err(e) = bridge.start().await {
            error!("Bridge stopped!! Error = {:?}", e);
        }
    });

    if enable_simulator {
        let data_tx = collector_tx.clone();
        task::spawn(async {
            let mut simulator = Simulator::new(config, data_tx);
            simulator.start().await;
        });
    }

    let tunshell_collector_tx = collector_tx.clone();
    thread::spawn(move || {
        let tunshell_session = TunshellSession::new(Relay::default(), false, tunshell_keys_rx, tunshell_collector_tx);
        tunshell_session.start()
    });

    let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
    let mut actions = actions::new(collector_tx, controllers, native_actions_rx, tunshell_keys_tx).await;
    actions.start().await;
    Ok(())
}
