use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::{fmt::Debug, sync::Arc};

mod actions_lane;
mod data_lane;
mod delaymap;
mod metrics;
pub(crate) mod stream;
mod streams;

use crate::{Action, ActionResponse, ActionRoute, Config};

use self::{
    actions_lane::{ActionsBridge, ActionsBridgeTx},
    data_lane::{DataBridge, DataBridgeTx},
};

use super::Compression;
pub use metrics::StreamMetrics;

pub trait Point: Send + Debug {
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

pub trait Package: Send + Debug {
    fn topic(&self) -> Arc<String>;
    fn stream(&self) -> Arc<String>;
    // TODO: Implement a generic Return type that can wrap
    // around custom serialization error types.
    fn serialize(&self) -> serde_json::Result<Vec<u8>>;
    fn anomalies(&self) -> Option<(String, usize)>;
    fn len(&self) -> usize;
    fn latency(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn compression(&self) -> Compression;
}

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

impl Point for Payload {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

pub struct Bridge {
    pub(crate) data: DataBridge,
    pub(crate) actions: ActionsBridge,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
        actions_rx: Receiver<Action>,
        shutdown_handle: Sender<()>,
    ) -> Self {
        let data = DataBridge::new(config.clone(), package_tx, metrics_tx);
        let actions = ActionsBridge::new(config, data.tx(), actions_rx, shutdown_handle);
        Self { data, actions }
    }

    pub fn tx(&self) -> BridgeTx {
        BridgeTx { data: self.data.tx(), actions: self.actions.tx() }
    }

    pub fn register_action_route(&mut self, route: ActionRoute) -> Receiver<Action> {
        self.actions.register_action_route(route)
    }

    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
    ) -> Option<Receiver<Action>> {
        self.actions.register_action_routes(routes)
    }
}

#[derive(Debug, Clone)]
pub struct BridgeTx {
    data: DataBridgeTx,
    actions: ActionsBridgeTx,
}

impl BridgeTx {
    pub async fn send_payload(&self, payload: Payload) {
        self.data.send_payload(payload).await
    }

    pub fn send_payload_sync(&self, payload: Payload) {
        self.data.send_payload_sync(payload)
    }

    pub async fn send_action_response(&self, response: ActionResponse) {
        self.actions.send_action_response(response).await
    }

    pub async fn trigger_shutdown(&self) {
        self.actions.trigger_shutdown().await
    }
}
