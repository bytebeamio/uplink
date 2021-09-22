use serde::{Deserialize, Serialize};

use std::sync::Arc;

use crate::base::{timestamp, Buffer, Package, Point};

/// Action response is how uplink informs the platform about the
/// progress of Actions in execution on the device.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    id: String,
    /// Timestamp on response creation
    timestamp: u64,
    /// Can be either running or failed
    state: String,
    /// Progress percentage for given Action
    progress: u8,
    /// List of errors faced on performing said Action
    errors: Vec<String>,
}

impl ActionResponse {
    /// Default/Execution begun Response
    pub fn new(id: &str) -> Self {
        ActionResponse {
            id: id.to_owned(),
            timestamp: timestamp(),
            state: "Running".to_owned(),
            progress: 0,
            errors: vec![],
        }
    }

    /// Execution completed successfully response
    pub fn success(id: &str) -> ActionResponse {
        ActionResponse {
            id: id.to_owned(),
            timestamp: timestamp(),
            state: "Completed".to_owned(),
            progress: 100,
            errors: vec![],
        }
    }

    /// Execution ended in failure response
    pub fn failure<E: Into<String>>(id: &str, error: E) -> ActionResponse {
        ActionResponse {
            id: id.to_owned(),
            timestamp: timestamp(),
            state: "Failed".to_owned(),
            progress: 100,
            errors: vec![error.into()],
        }
    }
}

impl Point for ActionResponse {
    fn sequence(&self) -> u32 {
        0
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<ActionResponse> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}
