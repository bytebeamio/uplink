//! uplink is a utility/library to interact with the Bytebeam platform. It's internal architecture is described in the diagram below.
//! We use [`rumqttc`], which implements the MQTT protocol, to communicate with the platform. Communication is handled separately as ingress and egress
//! by [`Mqtt`] and [`Serializer`] respectively. [`Action`]s are received and forwarded by Mqtt to the [`Actions`] module, where it is handled depending
//! on it's type and purpose, forwarding it to either the [`Bridge`](collector::tcpjson::Bridge), [`Process`](base::actions::process::Process),
//! [`Controller`](base::actions::controller::Controller), [`OtaDownloader`](base::actions::ota::OtaDownloader) or [`TunshellSession`](base::actions::tunshell::TunshellSession).
//! Bridge forwards received Actions to devices connected to it through the `bridge_port` and collects response data from these devices, to forward to the platform.
//!
//! Response data can be of multiple types, of interest to us are [`ActionResponse`](base::actions::response::ActionResponse)s, which are forwarded to Actions
//! and then to Serializer where depending on the network, it may be stored onto disk with [`Storage`](disk::Storage) to ensure packets aren't lost.
//!
//!                                                  ┌────────┐
//!                                                  │Platform│
//!                                                  └───┐─▲──┘
//!                                                      │ │
//!                                                      │ │
//!                                                      │ │
//!                                                   ┌──▼─└─┐
//!                                            ┌──────┤rumqtt◄────────┐
//!                                            │      └──────┘        │
//!                                  Eventloop │                      │ AsyncClient
//!                                            │                      │
//!                                            │                      │
//!                                         ┌──▼─┐               ┌────┴─────┐
//!                                         │Mqtt│               │Serializer│
//!                                         └──┬─┘               └────▲─────┘
//!                                            │                      │
//!                             action_channel │                      │ response_channel
//!                                            │      ┌───────┐       │
//!                                            └──────►Actions├───────┘
//!                                                   └┬─┬─┬─▲┘
//!                                                    │ │ │ │                     Collectors
//!                  ----------------------------------│ │ │ │----------------------------------
//!                  +                                 │ │ │ │    collector_channel            +
//!                  +                                 │ │ │ └─────────────────────────┬─────┐ +
//!                  +           tunshell_keys_channel │ │ │                           │     │ +
//!                  +         ┌───────────────────────┘ │ │ bridge_actions_channel ┌──┴───┐ │ +
//!                  +         │                         │ └──────────┬─────────────►Bridge│ │ +
//!                  +         │                         │            │             └──┬▲──┘ │ +
//!                  +         │              ┌──────────┼──────────┐ │                ||    │ +
//!                  +         │              │          │          │ │                ||    │ +
//!                  + ┌───────▼───────┐ ┌────▼─────┐┌───▼───┐┌─────▼─┴─────┐       ┌──▼┴──┐ │ +
//!                  + │TunshellSession│ │Controller││Process││OtaDownloader│       |Device| │ +
//!                  + └───────┬───────┘ └────┬─────┘└───┬───┘└─────────────┘       └──────┘ │ +
//!                  +         │              │          │                                   │ +
//!                  +         └──────────────┼──────────┘                                   │ +
//!                  +          action_status │                                              │ +
//!                  +                        │                                              │ +
//!                  +                 ┌──────▼───────┐                                      │ +
//!                  +                 │Stream, Buffer├──────────────────────────────────────┘ +
//!                  +                 └──────────────┘                                        +
//!                  ---------------------------------------------------------------------------
//!

use anyhow::{Context, Error};
use async_channel::{bounded, Receiver, Sender};
use disk::Storage;
use log::error;
use tokio::task;

use std::sync::Arc;

pub mod base;
pub mod cli;
pub mod collector;
pub mod config;

pub use base::actions::tunshell;
use base::{actions::Action, mqtt::Mqtt, serializer::Serializer, Package};
pub use base::{actions::Actions, Control, Stream};
pub use config::Config;
/// uplink communicates with Bytebeam using the MQTT protocol, which is implemented with the rumqtt crate.
/// All data exchanges happening between broker and client in uplink are handled via the
/// [`AsyncClient`](rumqttc::AsyncClient) and [`EventLoop`](rumqttc::EventLoop) instances.
/// Thus it is necessary to handle data ingress and egress, which is performed by [`Mqtt`] and [`Serializer`] respectively.
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
