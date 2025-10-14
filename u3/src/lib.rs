use crate::collectors::device_shadow::device_shadow_task;
use crate::collectors::remote_shell::remote_shell_task;
use crate::collectors::tcp_client::tcp_client_task;
use crate::config::{AuthConfig, HttpCreds, UplinkConfig};
use crate::core::actions::{Action, send_action_response};
use crate::core::serializer::{SerializerConfig, SerializerStorageHandler};
use crate::core::storage::Publish;
use crate::utils::num_cores;
use flume::{Receiver, Sender};
use futures::task::SpawnExt;
use log::warn;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::Instrument;

pub mod collectors;
pub mod config;
pub mod core;
pub mod utils;

#[derive(Deserialize)]
pub struct DataRow {
    pub stream: String,
    #[serde(flatten)]
    pub data: PublishItem,
}

#[derive(Serialize, Deserialize)]
pub struct PublishItem {
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

#[derive(Clone)]
pub struct AppContext {
    pub cfg: UplinkConfig,
    pub auth: AuthConfig,
}

pub struct Uplink {
    config: UplinkConfig,
    auth: AuthConfig,

    data_rx: Receiver<DataRow>,
    actions_mapping: HashMap<String, Sender<Action>>,

    serializer_task: JoinHandle<()>,
    plugin_tasks: JoinSet<()>,
    cleanup_done: bool,
}

impl Uplink {
    pub fn spawn(
        config: UplinkConfig,
        auth: AuthConfig,
        lib_actions_tx: Sender<Action>,
        lib_data_rx: Receiver<DataRow>,
    ) -> Self {
        let (data_tx, data_rx) = flume::bounded(decide_data_buffer_size(&config));
        let mut plugin_tasks = JoinSet::new();
        let mut actions_mapping = HashMap::new();
        for action in config.lib_actions.iter() {
            actions_mapping.insert(action.name.clone(), lib_actions_tx.clone());
        }
        if config.builtin_collectors.device_shadow.enable {
            plugin_tasks.spawn(Box::pin(device_shadow_task(data_tx.clone())));
        }
        if config.enable_remote_shell {
            let (action_tx, action_rx) = flume::bounded(0);
            actions_mapping.insert("launch_shell".to_owned(), action_tx);
            plugin_tasks.spawn(Box::pin(remote_shell_task(data_tx.clone(), action_rx)));
        }
        for (name, cfg) in config.tcp_clients.iter() {
            let (action_tx, action_rx) = flume::bounded(0);
            for action in cfg.actions.iter() {
                actions_mapping.insert(action.name.clone(), action_tx.clone());
            }
            plugin_tasks.spawn(Box::pin(
                tcp_client_task(cfg.port, data_tx.clone(), action_rx)
                    .instrument(tracing::info_span!("tcp_client", name = name.clone())),
            ));
        }

        let serializer_task = tokio::spawn(Box::pin(
            SerializerStorageHandler::new(
                SerializerConfig {
                    credentials: auth.http_credentials.clone(),
                    streams: config.streams.clone(),
                    mqtt_max_packet_size: config.mqtt.max_packet_size,
                    max_dynamic_streams_count: config.max_dynamic_streams_count,
                    persistence_path: config.persistence_path.clone(),
                },
                data_rx.clone(),
            )
            .run(),
        ));

        Self {
            config,
            auth,
            data_rx,
            actions_mapping,
            serializer_task,
            plugin_tasks,
            cleanup_done: false,
        }
    }

    pub async fn update_credentials(&mut self, new_credentials: AuthConfig) {
        self.serializer_task.abort();
        std::mem::replace(&mut self.serializer_task, tokio::spawn(async {})).await;
        self.auth = new_credentials;
        self.serializer_task = tokio::spawn(Box::pin(
            SerializerStorageHandler::new(
                SerializerConfig {
                    credentials: self.auth.http_credentials.clone(),
                    streams: self.config.streams.clone(),
                    mqtt_max_packet_size: self.config.mqtt.max_packet_size,
                    max_dynamic_streams_count: self.config.max_dynamic_streams_count,
                    persistence_path: self.config.persistence_path.clone(),
                },
                self.data_rx.clone(),
            )
            .run(),
        ));
    }

    pub async fn terminate(&mut self) {
        let plugin_tasks = std::mem::replace(&mut self.plugin_tasks, JoinSet::new());
        let serializer_task = std::mem::replace(&mut self.serializer_task, tokio::spawn(async {}));
        Self::terminate_impl(plugin_tasks, vec![serializer_task]).await;
        self.cleanup_done = true;
    }

    async fn terminate_impl(mut plugin_tasks: JoinSet<()>, core_tasks: Vec<JoinHandle<()>>) {
        plugin_tasks.abort_all();
        while let Some(_) = plugin_tasks.join_next().await {}
        for task in core_tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for Uplink {
    fn drop(&mut self) {
        if !self.cleanup_done {
            warn!("Uplink::terminate() needs to be called manually to perform a clean shutdown");
            let plugin_tasks = std::mem::replace(&mut self.plugin_tasks, JoinSet::new());
            let serializer_task =
                std::mem::replace(&mut self.serializer_task, tokio::spawn(async {}));
            // workaround because async Drop isn't stable yet
            let _ = futures::executor::block_on(tokio::spawn(tokio::time::timeout(
                Duration::from_secs(2),
                Self::terminate_impl(plugin_tasks, vec![serializer_task]),
            )));
        }
    }
}

fn decide_data_buffer_size(ctx: &UplinkConfig) -> usize {
    let max_buffer_size = ctx.streams.iter().map(|(_, s)| s.buffer_size).max().unwrap_or(5);
    max_buffer_size * 500
}

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
