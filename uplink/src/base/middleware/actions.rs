use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::Duration;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::{base::Point, Payload};

/// On the Bytebeam platform, an Action is how beamd and through it,
/// the end-user, can communicate the tasks they want to perform on
/// said device, in this case, uplink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    #[serde(skip)]
    pub device_id: String,
    // action id
    #[serde(alias = "id")]
    pub action_id: String,
    // determines if action is a process
    pub kind: String,
    // action name
    pub name: String,
    // action payload. json. can be args/payload. depends on the invoked command
    pub payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    pub id: String,
    // sequence number
    pub sequence: u32,
    // timestamp
    pub timestamp: u64,
    // running, failed
    pub state: String,
    // progress percentage for processes
    pub progress: u8,
    // list of error
    pub errors: Vec<String>,
}

impl ActionResponse {
    fn new(id: &str, state: &str, progress: u8, errors: Vec<String>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp,
            state: state.to_owned(),
            progress,
            errors,
        }
    }

    pub fn progress(id: &str, state: &str, progress: u8) -> Self {
        ActionResponse::new(id, state, progress, vec![])
    }

    pub fn success(id: &str) -> ActionResponse {
        ActionResponse::new(id, "Completed", 100, vec![])
    }

    pub fn add_error<E: Into<String>>(mut self, error: E) -> ActionResponse {
        self.errors.push(error.into());
        self
    }

    pub fn failure<E: Into<String>>(id: &str, error: E) -> ActionResponse {
        ActionResponse::new(id, "Failed", 100, vec![]).add_error(error)
    }

    pub fn set_sequence(mut self, seq: u32) -> ActionResponse {
        self.sequence = seq;
        self
    }

    pub fn as_payload(&self) -> Payload {
        Payload::from(self)
    }
}

impl From<&ActionResponse> for Payload {
    fn from(resp: &ActionResponse) -> Self {
        Self {
            stream: "action_status".to_owned(),
            sequence: resp.sequence,
            timestamp: resp.timestamp,
            payload: json!({
                "id": resp.id,
                "state": resp.state,
                "progress": resp.progress,
                "errors": resp.errors
            }),
        }
    }
}

fn get_payload<'a>(payload: &'a Payload, key: &str) -> &'a Value {
    payload.payload.get(key).unwrap_or_else(|| panic!("{} key missing from payload", key))
}

impl From<Payload> for ActionResponse {
    fn from(payload: Payload) -> Self {
        Self {
            sequence: payload.sequence,
            timestamp: payload.timestamp,
            id: get_payload(&payload, "id")
                .as_str()
                .expect("couldn't convert to string")
                .to_string(),
            state: get_payload(&payload, "state")
                .as_str()
                .expect("couldn't convert to string")
                .to_string(),
            progress: get_payload(&payload, "progress")
                .as_u64()
                .expect("couldn't convert to string") as u8,
            errors: get_payload(&payload, "errors")
                .as_array()
                .expect("couldn't convert to array")
                .iter()
                .map(|v| v.to_string())
                .collect(),
        }
    }
}

impl Point for ActionResponse {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
