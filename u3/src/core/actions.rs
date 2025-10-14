use flume::Sender;
use serde::{Deserialize, Serialize};
use serde_json::json;
use crate::{DataRow, PublishItem};
use crate::utils::clock;

#[derive(Serialize, Deserialize)]
pub struct Action {
    pub id: String,
    pub name: String,
    pub payload: String,
}

pub async fn send_action_response(
    data_tx: &Sender<DataRow>,
    action_id: impl ToString,
    state: impl ToString,
    progress: u8,
    errors: &[String],
) {
    let _ = data_tx
        .send_async(DataRow {
            stream: "action_status".to_string(),
            data: PublishItem {
                sequence: 0,
                timestamp: clock(),
                data: json!({
                    "action_id": action_id.to_string(),
                    "state": state.to_string(),
                    "progress": progress,
                    "errors": errors
                }),
            },
        })
        .await;
}
