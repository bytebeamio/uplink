use serde::{Deserialize, Serialize};
use serde_json::json;
use crate::Payload;

use super::clock;

/// On the Bytebeam platform, an Action is how broker and through it,
/// the end-user, can communicate the tasks they want to perform on
/// said device, in this case, uplink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    #[serde(alias = "id")]
    pub action_id: String,
    pub name: String,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResponse {
    #[serde(alias = "id")]
    pub action_id: String,
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
    #[serde(skip)]
    pub done_response: Option<Action>,
}

impl ActionResponse {
    fn new(id: &str, state: &str, progress: u8, errors: Vec<String>) -> Self {
        let timestamp = clock() as u64;

        ActionResponse {
            action_id: id.to_owned(),
            sequence: 0,
            timestamp,
            state: state.to_owned(),
            progress,
            errors,
            done_response: None,
        }
    }

    pub fn is_completed(&self) -> bool {
        self.state == "Completed"
    }

    pub fn is_failed(&self) -> bool {
        self.state == "Failed"
    }

    pub fn is_done(&self) -> bool {
        self.progress == 100
    }

    pub fn progress(id: &str, state: &str, progress: u8) -> Self {
        ActionResponse::new(id, state, progress, vec![])
    }

    pub fn success(id: &str) -> ActionResponse {
        ActionResponse::new(id, "Completed", 100, vec![])
    }

    pub fn done(id: &str, state: &str, response: Option<Action>) -> Self {
        let mut o = ActionResponse::new(id, state, 100, vec![]);
        o.done_response = response;
        o
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

    pub fn from_payload(payload: &Payload) -> Result<Self, serde_json::Error> {
        let intermediate = serde_json::to_value(payload)?;
        serde_json::from_value(intermediate)
    }

    pub fn to_payload(&self) -> Payload {
        let payload = json! {{
            "action_id": self.action_id,
            "state": self.state,
            "progress": self.progress,
            "errors": self.errors
        }};

        Payload {
            stream: String::from("action_status"),
            sequence: self.sequence,
            timestamp: self.timestamp,
            payload,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Cancellation {
    pub action_id: String,
    #[serde(rename = "name")]
    pub action_name: String,
}
