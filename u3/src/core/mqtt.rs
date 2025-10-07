use std::collections::HashMap;
use flume::{Receiver, Sender};
use serde_json::json;
use crate::{DataRow, PublishPayload};
use crate::utils::clock;

pub struct Action {
    pub action_id: String,
    pub name: String,
    pub payload: String,
}

pub async fn send_action_response(data_tx: &Sender<DataRow>, action_id: impl ToString, state: impl ToString, progress: u8, errors: &[String]) {
    let _ = data_tx.send_async(DataRow {
        stream: "action_status".to_string(),
        data: PublishPayload {
            sequence: 0,
            timestamp: clock(),
            data: json!({
                "action_id": action_id.to_string(),
                "state": state.to_string(),
                "progress": progress,
                "errors": errors
            }),
        },
    }).await;
}

pub async fn mqtt_task(batch_rx: Receiver<(String, Vec<PublishPayload>)>, actions_mapping: HashMap<String, Sender<Action>>) {
}