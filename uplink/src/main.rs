#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Error};
use async_channel::{bounded, Sender};
use structopt::StructOpt;
use tokio::task;
use simplelog::{CombinedLogger, LevelFilter, LevelPadding, TermLogger, TerminalMode};


mod base;
mod collector;

use crate::base::mqtt::Mqtt;
use crate::base::serializer::Serializer;
use crate::collector::bridge::Bridge;
use base::actions;
use base::Config;
use disk::Storage;
use std::sync::Arc;

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
    /// device id
    #[structopt(short = "i", help = "Device id")]
    device_id: String,
    /// config file
    #[structopt(short = "c", help = "Config file")]
    config: String,
    /// directory with certificates
    #[structopt(short = "a", help = "certs")]
    certs_dir: PathBuf,
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
    let config = fs::read_to_string(&commandline.config);
    let config = config.with_context(|| format!("Config = {}", commandline.config))?;

    let device_id = commandline.device_id.trim();

    let mut config: Config = toml::from_str(&config)?;
    config.ca = Some(commandline.certs_dir.join(device_id).join("roots.pem"));
    config.key = Some(commandline.certs_dir.join(device_id).join("rsa_private.pem"));
    config.device_id = str::replace(&config.device_id, "{device_id}", device_id);

    for config in config.streams.values_mut() {
        let topic = str::replace(&config.topic, "{device_id}", device_id);
        config.topic = topic
    }

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
        config
            .add_filter_allow_str("uplink")
            .add_filter_allow_str("disk");
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
    initialize_logging(&commandline);
    let config = Arc::new(initalize_config(commandline)?);

    let (collector_tx, collector_rx) = bounded(10);
    let (native_actions_tx, native_actions_rx) = bounded(10);
    let (bridge_actions_tx, bridge_actions_rx) = bounded(10);

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

    let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
    let mut actions = actions::new(collector_tx, controllers, native_actions_rx).await;
    actions.start().await;
    Ok(())
}
