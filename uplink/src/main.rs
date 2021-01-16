#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Error};
use async_channel::{bounded, Sender};
use structopt::StructOpt;
use tokio::task;

mod base;
mod collector;

use crate::collector::bridge::Bridge;
use base::actions;
use base::mqtt;
use base::serializer;
use base::Config;
use std::sync::Arc;

#[derive(StructOpt, Debug)]
#[structopt(name = "uplink", about = "collect, batch, compress, publish")]
pub struct CommandLine {
    #[structopt(short = "i", help = "Device id")]
    device_id: String,
    #[structopt(short = "c", help = "Config file path")]
    config_path: String,
    #[structopt(short = "v", help = "version", default_value = "v1")]
    version: String,
    #[structopt(short = "a", help = "certs")]
    certs_dir: PathBuf,
}

/// Reads config file to generate config struct and replaces places holders
/// like bike id and data version
fn init_config(commandline: CommandLine) -> Result<Config, Error> {
    let config = fs::read_to_string(&commandline.config_path);
    let config = config.with_context(|| format!("Config error = {}", commandline.config_path))?;

    let device_id = commandline.device_id.trim();
    let version = commandline.version.trim();

    let mut config: Config = toml::from_str(&config)?;

    config.ca = Some(commandline.certs_dir.join(device_id).join("roots.pem"));
    config.key = Some(commandline.certs_dir.join(device_id).join("rsa_private.pem"));

    config.device_id = str::replace(&config.device_id, "{device_id}", device_id);
    for config in config.streams.values_mut() {
        let topic = str::replace(&config.topic, "{device_id}", device_id);
        let topic = str::replace(&topic, "{version}", version);

        config.topic = topic
    }

    Ok(config)
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    pretty_env_logger::init();
    let commandline: CommandLine = StructOpt::from_args();
    let config = Arc::new(init_config(commandline)?);

    let (collector_tx, collector_rx) = bounded(10);
    let (native_actions_tx, native_actions_rx) = bounded(10);
    let (bridge_actions_tx, bridge_actions_rx) = bounded(10);

    let mut mqtt = mqtt::Mqtt::new(config.clone(), native_actions_tx, bridge_actions_tx);
    let mut serializer = serializer::Serializer::new(config.clone(), collector_rx, mqtt.client())?;

    task::spawn(async move {
        serializer.start().await;
    });

    task::spawn(async move {
        mqtt.start().await;
    });

    let mut bridge = Bridge::new(config.clone(), collector_tx.clone(), bridge_actions_rx);
    task::spawn(async move {
        bridge.start().await;
    });

    let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
    let mut actions = actions::new(collector_tx, controllers, native_actions_rx).await;
    actions.start().await;
    Ok(())
}
