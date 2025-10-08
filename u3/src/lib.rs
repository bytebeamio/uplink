use crate::collectors::device_shadow::device_shadow_task;
use crate::collectors::remote_shell::remote_shell_task;
use crate::config::{AuthConfig, UplinkConfig};
use crate::core::mqtt::MqttConnectionHandler;
use crate::core::serializer::SerializerStorageHandler;
use crate::core::streams_buffer::StreamsBufferHandler;
use flume::{Receiver, Sender};
use futures::task::SpawnExt;
use serde::Serialize;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio::task_local;

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
    pub static CONFIG: Arc<AppConfig>;
}
pub struct AppConfig {
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
    let (data_tx, data_rx) = flume::bounded(1024);

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
    if cfg.builtin_collectors.device_shadow.enable {
        tasks_to_run.push(Box::new(RetryableTask {
            context: data_tx.clone(),
            task: |data_tx| Box::pin(device_shadow_task(data_tx)),
        }));
    }
    if cfg.enable_remote_shell {
        let (action_tx, action_rx) = flume::bounded(4);
        actions_mapping.insert("launch_shell".to_owned(), action_tx);
        tasks_to_run.push(Box::new(RetryableTask {
            context: (data_tx.clone(), action_rx),
            task: |(data_tx, action_rx)| Box::pin(remote_shell_task(data_tx, action_rx)),
        }));
    }

    let (mqtt_tx, mqtt_rx) = flume::bounded(0);
    tasks_to_run.push(Box::new(RetryableTask {
        context: (data_tx.clone(), mqtt_rx, actions_mapping),
        task: |(data_tx, mqtt_rx, actions_mapping)| Box::pin(
            MqttConnectionHandler::new(data_tx, mqtt_rx, actions_mapping).run()
        ),
    }));

    let (buffers_batch_tx, buffers_batch_rx) = flume::bounded(32);
    tasks_to_run.push(Box::new(RetryableTask {
        context: (data_rx.clone(), buffers_batch_tx),
        task: |(data_rx, buffers_batch_tx)| Box::pin(StreamsBufferHandler::new(data_rx, buffers_batch_tx).run()),
    }));
    tasks_to_run.push(Box::new(RetryableTask {
        context: (data_tx, buffers_batch_rx, mqtt_tx),
        task: |(data_tx, buffers_batch_rx, mqtt_tx)| Box::pin(SerializerStorageHandler::new(data_tx, buffers_batch_rx, mqtt_tx).run()),
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
    let ctx = Arc::new(AppConfig { cfg, auth });
    for task in tasks_to_run {
        // TODO: restart these tasks on panic
        js.spawn(run_with_retry(ctx.clone(), task));
    }
    js.join_all().await;
}

use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::time::{Duration, sleep};

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
    ctx: Arc<AppConfig>,
    task: Box<dyn Creator>
) {
    loop {
        let result = AssertUnwindSafe(CONFIG.scope(ctx.clone(), task.create())).catch_unwind().await;

        if let Err(_) = result {
            println!("Future panicked, retrying...");
            sleep(Duration::from_millis(1000)).await;
        } else {
            break;
        }
    }
}
