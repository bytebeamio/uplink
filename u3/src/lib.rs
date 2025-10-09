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
use tokio::task::JoinSet;

pub mod collectors;
pub mod config;
pub mod core;
pub mod utils;

pub fn start_uplink(
    cfg: UplinkConfig,
    auth: AuthConfig,
) -> (Receiver<ActionPayload>, Sender<DataRow>, Box<dyn Future<Output = ()>>) {
    let (actions_tx, actions_rx) = flume::bounded(8);
    let (data_tx, data_rx) = flume::bounded(128);
    (actions_rx, data_tx, Box::new(uplink_task(cfg, auth, actions_tx, data_rx)))
}

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
async fn uplink_task(
    cfg: UplinkConfig,
    auth: AuthConfig,
    lib_actions_tx: Sender<ActionPayload>,
    lib_data_rx: Receiver<DataRow>,
) {
    // SerializerStorageHandler reads from both data_rx and metrics_rx, but it'll only save data from data_rx on shutdown
    // all data written by collectors is guaranteed to be saved to disk on clean shutdown
    let (data_tx, data_rx) = flume::bounded(1024);
    let (metrics_tx, metrics_rx) = flume::bounded(8);
    let ctx = Arc::new(AppContext { cfg, auth });

    let mut tasks_to_run = Vec::<Box<dyn Creator>>::new();
    tasks_to_run.push(Box::new(RetryableTask {
        context: (data_tx.clone(), lib_data_rx.clone()),
        task: |(data_tx, lib_data_rx)| Box::pin(async move {
            while let Ok(msg) = lib_data_rx.recv_async().await {
                let _ = data_tx.send_async(msg);
            }
        }),
    }));

    let mut actions_mapping = HashMap::new();
    if ctx.cfg.builtin_collectors.device_shadow.enable {
        tasks_to_run.push(Box::new(RetryableTask {
            context: data_tx.clone(),
            task: |data_tx| Box::pin(device_shadow_task(data_tx)),
        }));
    }
    if ctx.cfg.enable_remote_shell {
        let (action_tx, action_rx) = flume::bounded(0);
        actions_mapping.insert("launch_shell".to_owned(), action_tx);
        tasks_to_run.push(Box::new(RetryableTask {
            context: (data_tx.clone(), action_rx),
            task: |(data_tx, action_rx)| Box::pin(remote_shell_task(data_tx, action_rx)),
        }));
    }
    for (name, cfg) in ctx.cfg.tcp_clients.iter() {
        let (action_tx, action_rx) = flume::bounded(0);
        for action in cfg.actions.iter() {
            actions_mapping.insert(action.name.clone(), action_tx.clone());
        }
        tasks_to_run.push(Box::new(RetryableTask {
            context: (name.clone(), cfg.port, data_tx.clone(), action_rx),
            task: |(name, port, data_tx, action_rx)| Box::pin(tcp_client_task(port, data_tx, action_rx)
                .instrument(tracing::info_span!("tcp_client", name = name))),
        }));
    }

    let (mqtt_tx, mqtt_rx) = flume::bounded(0);
    tasks_to_run.push(Box::new(RetryableTask {
        context: (ctx.clone(), metrics_tx.clone(), mqtt_rx, actions_mapping),
        task: |(ctx, data_tx, mqtt_rx, actions_mapping)| Box::pin(
            MqttConnectionHandler::new(ctx, data_tx, mqtt_rx, actions_mapping).run()
        ),
    }));

    tasks_to_run.push(Box::new(RetryableTask {
        context: (ctx.clone(), data_rx, metrics_rx, mqtt_tx),
        task: |(ctx, data_rx, metrics_rx, mqtt_tx)| Box::pin(SerializerStorageHandler::new(ctx, data_rx, metrics_rx, mqtt_tx).run()),
    }));

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
        // TODO: restart these tasks on panic
        js.spawn(run_with_retry(task));
    }
    js.join_all().await;
}

use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::time::{Duration, sleep};
use tracing::Instrument;
use crate::collectors::tcp_client::tcp_client_task;

type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
struct RetryableTask<C: Clone> {
    context: C,
    task: fn(C) -> Task
}
trait Creator: Send {
    fn create(&self) -> Task;
}
impl<C: Clone + Send> Creator for RetryableTask<C> {
    fn create(&self) -> Task {
        (self.task)(self.context.clone())
    }
}

async fn run_with_retry(
    task: Box<dyn Creator>
) {
    loop {
        let result = AssertUnwindSafe(task.create()).catch_unwind().await;

        if let Err(_) = result {
            println!("Future panicked, retrying...");
            sleep(Duration::from_millis(1000)).await;
        } else {
            break;
        }
    }
}
