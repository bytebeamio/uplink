use anyhow::{Context, Error};
use async_channel::{bounded, Receiver, Sender};
use disk::Storage;
use log::error;
use tokio::task;

use std::sync::Arc;

mod base;
pub mod cli;
pub mod collector;

pub use base::actions::tunshell;
use base::{actions::Action, mqtt::Mqtt, serializer::Serializer, Config, Package};
pub use base::{actions::Actions, Control, Stream};

pub fn spawn_intefaces(
    config: Arc<Config>,
) -> Result<(Sender<Box<dyn Package>>, Receiver<Action>), Error> {
    let (collector_tx, collector_rx) = bounded(10);
    let (native_actions_tx, native_actions_rx) = bounded(10);
    let storage = Storage::new(
        &config.persistence.path,
        config.persistence.max_file_size,
        config.persistence.max_file_count,
    )
    .with_context(|| format!("Storage = {:?}", config.persistence))?;

    let mut mqtt_processor = Mqtt::new(config.clone(), native_actions_tx);
    let mut serializer = Serializer::new(config, collector_rx, mqtt_processor.client(), storage)?;

    // Spawn serializer, to handle outflow of `ActionResponse`s
    task::spawn(async move {
        if let Err(e) = serializer.start().await {
            error!("Serializer stopped!! Error = {:?}", e);
        }
    });

    // Spawn mqtt processor, to handle inflow of `Action`s
    task::spawn(async move {
        mqtt_processor.start().await;
    });

    Ok((collector_tx, native_actions_rx))
}
