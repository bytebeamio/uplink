use std::collections::HashMap;
use std::sync::Arc;
use flume::Sender;
use serde::{Deserialize, Serialize};
use serde_json::json;
use crate::{DataRow, PublishItem};
use crate::core::serializer::ConnectionManager;
use crate::core::storage::Publish;
use crate::utils::ac::AC;
use crate::utils::clock;

#[derive(Debug, Serialize, Deserialize)]
pub struct Action {
    pub action_id: String,
    pub params: serde_json::Value,
}

pub async fn send_action_response(
    cm: &Arc<ConnectionManager>,
    action_id: impl ToString,
    state: impl ToString,
    progress: u8,
    errors: &[String],
) {
    cm.upload_message("action_status", PublishItem {
        sequence: 0,
        timestamp: clock(),
        data: json!({
            "action_id": action_id.to_string(),
            "state": state.to_string(),
            "progress": progress,
            "errors": errors
        }),
    }).await;
}
