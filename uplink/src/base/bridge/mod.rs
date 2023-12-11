use flume::{Receiver, Sender};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::{fmt::Debug, sync::Arc};

mod actions_lane;
mod data_lane;
mod delaymap;
mod metrics;
pub(crate) mod stream;
mod streams;

use actions_lane::{ActionsBridge, Error};
pub use actions_lane::{CtrlTx as ActionsLaneCtrlTx, StatusTx};
use data_lane::DataBridge;
pub use data_lane::{CtrlTx as DataLaneCtrlTx, DataTx};

use super::mqtt::MqttShutdown;
use super::StreamConfig;
use crate::base::ActionRoute;
use crate::{Action, ActionResponse, Config};
pub use metrics::StreamMetrics;

pub trait Point: Send + Debug + Serialize + 'static {
    fn stream_name(&self) -> &str;
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

pub trait Package: Send + Debug {
    fn stream_config(&self) -> Arc<StreamConfig>;
    fn stream_name(&self) -> Arc<String>;
    // TODO: Implement a generic Return type that can wrap
    // around custom serialization error types.
    fn serialize(&self) -> serde_json::Result<Vec<u8>>;
    fn anomalies(&self) -> Option<(String, usize)>;
    fn len(&self) -> usize;
    fn latency(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
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
    fn stream_name(&self) -> &str {
        &self.stream
    }

    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Commands that can be used to remotely trigger action_lane shutdown
pub(crate) struct ActionBridgeShutdown;

/// Commands that can be used to remotely trigger data_lane shutdown
pub(crate) struct DataBridgeShutdown;

pub struct Bridge {
    pub(crate) data: DataBridge,
    pub(crate) actions: ActionsBridge,
    pub(crate) mqtt_shutdown: Sender<MqttShutdown>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
        actions_rx: Receiver<Action>,
        shutdown_handle: Sender<()>,
        mqtt_shutdown: Sender<MqttShutdown>,
    ) -> Self {
        let data = DataBridge::new(config.clone(), package_tx.clone(), metrics_tx.clone());
        let actions =
            ActionsBridge::new(config, package_tx, actions_rx, shutdown_handle, metrics_tx);
        Self { data, actions, mqtt_shutdown }
    }

    /// Handle to send data/action status messages
    pub fn bridge_tx(&self) -> BridgeTx {
        BridgeTx { data_tx: self.data.data_tx(), status_tx: self.actions.status_tx() }
    }

    pub(crate) fn ctrl_tx(&self) -> (actions_lane::CtrlTx, data_lane::CtrlTx) {
        (self.actions.ctrl_tx(), self.data.ctrl_tx())
    }

    pub fn register_action_route(
        &mut self,
        route: ActionRoute,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        self.actions.register_action_route(route, actions_tx)
    }

    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        self.actions.register_action_routes(routes, actions_tx)
    }
}

#[derive(Debug, Clone)]
pub struct BridgeTx {
    pub data_tx: DataTx,
    pub status_tx: StatusTx,
}

impl BridgeTx {
    pub async fn send_payload(&self, payload: Payload) {
        self.data_tx.send_payload(payload).await
    }

    pub fn send_payload_sync(&self, payload: Payload) {
        self.data_tx.send_payload_sync(payload)
    }

    pub async fn send_action_response(&self, response: ActionResponse) {
        self.status_tx.send_action_response(response).await
    }
}
