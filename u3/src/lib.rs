use std::collections::HashMap;
use std::pin::Pin;
use flume::{Receiver, Sender};
use serde::Serialize;
use tokio::task_local;
use crate::collectors::device_shadow::device_shadow_task;
use crate::collectors::remote_shell::remote_shell_task;
use crate::config::{AuthConfig, UplinkConfig};
use crate::core::mqtt::MqttConnectionHandler;
use crate::core::serializer::data_task;

pub mod config;
pub mod utils;
pub mod core;
pub mod collectors;

pub fn start_uplink(cfg: UplinkConfig, auth: AuthConfig) -> (Receiver<ActionPayload>, Sender<DataRow>, Box<dyn Future<Output=anyhow::Result<()>>>) {
    let (actions_tx, actions_rx) = flume::bounded(8);
    let (data_tx, data_rx) = flume::bounded(128);
    (actions_rx, data_tx, Box::new(uplink_task(cfg, auth, actions_tx, data_rx)))
}

pub struct ActionPayload {
    pub name: String,
    pub action_id: String,
    pub payload: serde_json::Value
}

pub struct DataRow {
    pub stream: String,
    pub data: PublishItem,
}

#[derive(Serialize)]
pub struct PublishItem {
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

task_local! {
    pub static CONFIG: AppConfig;
}
pub struct AppConfig {
    pub cfg: UplinkConfig,
    pub auth: AuthConfig,
}
// TODO(3): logs are difficult to read and understand right now. how can that be fixed?
async fn uplink_task(cfg: UplinkConfig, auth: AuthConfig, lib_actions_tx: Sender<ActionPayload>, lib_data_rx: Receiver<DataRow>) -> anyhow::Result<()> {
    // serialize input, raw messages will be written to it and read by serializer
    let (data_tx, data_rx) = flume::bounded(1024);

    let mut tasks_to_run = Vec::<Pin<Box<dyn Future<Output=()>>>>::new();
    // push data written by lib users
    tasks_to_run.push({
        let data_tx = data_tx.clone();
        Box::pin(async move {
            while let Ok(msg) = lib_data_rx.recv_async().await {
                let _ = data_tx.send_async(msg);
            }
        })
    });

    let mut actions_mapping = HashMap::new();
    // tasks for collectors
    // tasks_to_run.push(system_stats_task(data_tx.clone()));
    if cfg.builtin_collectors.device_shadow.enable {
        tasks_to_run.push(Box::pin(device_shadow_task(data_tx.clone())));
    }
    if cfg.enable_remote_shell {
        let (action_tx, action_rx) = flume::bounded(4);
        actions_mapping.insert("launch_shell".to_owned(), action_tx);
        tasks_to_run.push(Box::pin(remote_shell_task(data_tx.clone(), action_rx)));
    }

    // create mqtt task (receives batches from data task and writes to cloud, receives actions from cloud and dispatches to task for that action)
    //  * It'll save inflight messages to disk on Drop
    let (mqtt_client, mqtt_handler) = MqttConnectionHandler::new(data_tx.clone(), actions_mapping);
    tasks_to_run.push(Box::pin(mqtt_handler.run()));

    // create data task (receives all outgoing data, batches and flushes as per stream config, handles persistence of batches as well)
    //  * It'll save in memory buffers to disk on Drop
    tasks_to_run.push(Box::pin(data_task(data_rx, mqtt_client)));

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
    futures::future::join_all(tasks_to_run).await;
    Ok(())
}
