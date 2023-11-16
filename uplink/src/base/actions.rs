use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{Payload, Point};

use super::clock;

/// On the Bytebeam platform, an Action is how beamd and through it,
/// the end-user, can communicate the tasks they want to perform on
/// said device, in this case, uplink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    // action id
    #[serde(alias = "id")]
    pub action_id: String,
    // determines if action is a process
    pub kind: String,
    // action name
    pub name: String,
    // action payload. json. can be args/payload. depends on the invoked command
    pub payload: String,
    // Instant at which action must be timedout
    #[serde(skip)]
    pub deadline: Option<Instant>,
}

const DEFAULT_RESPONSE_STREAM: &str = "action_status";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResponse {
    #[serde(alias = "id")]
    pub action_id: String,
    #[serde(skip)]
    pub action_name: Option<String>,
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
            action_name: None,
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

    pub fn set_action_name(&mut self, action_name: String) {
        self.action_name = Some(action_name)
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
}

impl Point for ActionResponse {
    fn stream_name(&self) -> &str {
        self.action_name.as_ref().map(|n| n.as_str()).unwrap_or_else(|| DEFAULT_RESPONSE_STREAM)
    }

    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
