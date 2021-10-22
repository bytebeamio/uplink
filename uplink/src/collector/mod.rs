use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::sync::Arc;

use crate::base::{Buffer, Package, Point};

pub mod simulator;
pub mod tcpjson;

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is in turn a json
// TODO which cloud will double deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    #[serde(skip_serializing)]
    pub(crate) stream: String,
    pub(crate) sequence: u32,
    pub(crate) timestamp: u64,
    #[serde(flatten)]
    pub(crate) payload: Value,
}

impl Point for Payload {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Payload> {
    fn topic(&self) -> Arc<String> {
        return self.topic.clone();
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}
