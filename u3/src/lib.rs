use std::cmp::max;
use crate::collectors::device_shadow::device_shadow_task;
use crate::collectors::remote_shell::remote_shell_task;
use crate::config::{AuthConfig, UplinkConfig};
use crate::core::mqtt::MqttConnectionHandler;
use crate::core::serializer::SerializerStorageHandler;
use flume::{Receiver, Sender};
use futures::task::SpawnExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::{JoinError, JoinSet};
use tracing::Instrument;
use crate::collectors::tcp_client::tcp_client_task;
use crate::utils::num_cores;

pub mod collectors;
pub mod config;
pub mod core;
pub mod utils;

pub struct ActionPayload {
    pub name: String,
    pub action_id: String,
    pub payload: serde_json::Value,
}

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

pub struct AppContext {
    pub cfg: UplinkConfig,
    pub auth: AuthConfig,
}
// TODO(3): logs are difficult to read and understand right now. how can that be fixed?
pub async fn uplink_task(
    cfg: UplinkConfig,
    auth: AuthConfig,
    lib_actions_tx: Sender<ActionPayload>,
    lib_data_rx: Receiver<DataRow>,
) {
    let ctx = Arc::new(AppContext { cfg, auth });

    // SerializerStorageHandler reads from both data_rx and metrics_rx, but it'll only save data from data_rx on shutdown
    // all data written by collectors is guaranteed to be saved to disk on clean shutdown
    let (data_tx, data_rx) = flume::bounded(1024);
    let (metrics_tx, metrics_rx) = flume::bounded(8);

    let mut tasks_to_run = Vec::<Task>::new();
    tasks_to_run.push(Box::pin({
        let data_tx = data_tx.clone();
        async move {
            while let Ok(msg) = lib_data_rx.recv_async().await {
                let _ = data_tx.send_async(msg);
            }
        }
    }));

    let mut actions_mapping = HashMap::new();
    if ctx.cfg.builtin_collectors.device_shadow.enable {
        tasks_to_run.push(Box::pin(device_shadow_task(data_tx.clone())));
    }
    if ctx.cfg.enable_remote_shell {
        let (action_tx, action_rx) = flume::bounded(0);
        actions_mapping.insert("launch_shell".to_owned(), action_tx);
        tasks_to_run.push(Box::pin(remote_shell_task(data_tx.clone(), action_rx)));
    }
    for (name, cfg) in ctx.cfg.tcp_clients.iter() {
        let (action_tx, action_rx) = flume::bounded(0);
        for action in cfg.actions.iter() {
            actions_mapping.insert(action.name.clone(), action_tx.clone());
        }
        tasks_to_run.push(Box::pin(
            tcp_client_task(cfg.port, data_tx.clone(), action_rx)
                .instrument(tracing::info_span!("tcp_client", name = name.clone()))
        ));
    }

    let (mqtt_client, mqtt_handler) = MqttConnectionHandler::new(ctx.clone(), metrics_tx, actions_mapping);
    tasks_to_run.push(Box::pin(mqtt_handler.run()));

    tasks_to_run.push(Box::pin(
        SerializerStorageHandler::new(ctx, data_rx, metrics_rx, mqtt_client).run()
    ));

    // create a task for each collector and action handler
    //  * there'll be a mapping from action name to handler
    // create a downloader task
    //  * In case multiple downloads are queued, they'll happen one by one
    //  * All the collectors will call an async function. It'll return a channel that'll return download status.
    //  * Downloader task will maintain a metadata.json file for the active download
    //  * it'll be updated whenever the data is appended to the download file
    //  * both operations will happen synchronously
    // all metrics messages will be sent to serializer
    // all file system operations will be synchronous
    // events will be uploaded using the http api
    // action_status
    //  * users cannot configure this stream
    //  * internally it will use events api

    // await all these tasks
    let mut js = JoinSet::new();
    for task in tasks_to_run {
        js.spawn(task);
    }
    js.join_all().await;
}

type Task = Pin<Box<dyn Future<Output=()> + Send>>;
