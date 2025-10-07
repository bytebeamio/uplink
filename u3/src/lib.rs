use flume::{Receiver, Sender};
use tokio::task_local;
use crate::config::{AuthConfig, UplinkConfig};

pub mod config;
pub mod utils;

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
    pub sequence: u32,
    pub timestamp: u64,
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
async fn uplink_task(cfg: UplinkConfig, auth: AuthConfig, actions_tx: Sender<ActionPayload>, data_rx: Receiver<DataRow>) -> anyhow::Result<()> {
    // create mqtt task (receives batches from data task and writes to cloud, receives actions from cloud and dispatches to task for that action)
    //  * It'll save inflight messages to disk on Drop
    // create data task (receives all outgoing data, batches and flushes as per stream config, handles persistence of batches as well)
    //  * It'll save in memory buffers to disk on Drop
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
    Ok(())
}
