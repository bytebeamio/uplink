#[macro_use]
extern crate log;

use std::{fs, io};
use std::path::PathBuf;
use std::collections::HashMap;

use derive_more::From;
use structopt::StructOpt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;

mod base;
mod collector;

use base::actions;
use base::serializer;
use base::mqtt;
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

#[derive(Debug, From)]
pub enum InitError {
    Toml(toml::de::Error),
    File { name: String, err: io::Error },
    Base(base::Error),
    PersistentStream(persistentstream::Error<rumq_client::Request>),
}

/// Reads config file to generate config struct and replaces places holders
/// like bike id and data version
fn init_config(commandline: CommandLine) -> Result<Config, InitError> {
    let config = fs::read_to_string(&commandline.config_path)
        .map_err(|err| InitError::File { name: commandline.config_path.clone(), err })?;

    let device_id = commandline.device_id.trim();
    let version = commandline.version.trim();

    let mut config: Config = toml::from_str(&config)?;

    config.ca = Some(commandline.certs_dir.join(device_id).join("roots.pem"));
    config.key = Some(commandline.certs_dir.join(device_id).join("rsa_private.pem"));

    config.device_id = str::replace(&config.device_id, "{device_id}", device_id);
    for config in config.channels.values_mut() {
        let topic = str::replace(&config.topic, "{device_id}", device_id);
        let topic = str::replace(&topic, "{version}", version);

        config.topic = topic
    }

    Ok(config)
}

#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), InitError> {
    pretty_env_logger::init();

    let commandline: CommandLine = StructOpt::from_args();
    let config = init_config(commandline)?;

    let (collector_tx, collector_rx) = channel(10);
    let (actions_tx, actions_rx) = channel(10);
    let (bridge_actions_tx, bridge_actions_rx) = channel(10);
    let (mqtt_tx, mqtt_rx) = channel(10);
    // let serializer_mqtt_tx = persistentstream::upgrade("/tmp/persist", mqtt_tx.clone(), 10 * 1024, 30)?;
    
    let mut serializer = serializer::Serializer::new(config.clone(), collector_rx, mqtt_tx.clone());
    task::spawn(async move {
        serializer.start().await;
    });

    let mut mqtt = mqtt::Mqtt::new(config.clone(), actions_tx, bridge_actions_tx, mqtt_tx, mqtt_rx);
    task::spawn(async move {
        mqtt.start().await;
    });
    
    let controllers: HashMap<String, Sender<base::Control>> = HashMap::new();
    #[cfg(feature = "bridge")] {
        let collector = collector_tx.clone();
        let c = Arc::new(config.clone());
        task::spawn(async move {
            if let Err(e) = collector::bridge::start(c, collector, bridge_actions_rx).await {
                error!("Failed to spawn tcpjson collector. Error = {:?}", e);
            }
        });
    }
    
    let mut actions = actions::new(config, collector_tx, controllers, actions_rx).await;
    actions.start().await;
    Ok(())
}
