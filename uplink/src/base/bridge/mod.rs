pub use metrics::StreamMetrics;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::{fmt::Debug};

mod actions_lane;
mod data_lane;
mod delaymap;
mod metrics;

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is in turn a json
// TODO which cloud will double deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    #[serde(skip_serializing)]
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}
