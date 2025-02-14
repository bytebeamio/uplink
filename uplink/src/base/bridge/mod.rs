use flume::{bounded, Receiver, Sender};
pub use metrics::StreamMetrics;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::{fmt::Debug, sync::Arc};

mod actions_lane;
mod data_lane;
mod delaymap;
mod metrics;
pub mod stream;
mod streams;

pub use actions_lane::{ActionsBridge, Error};
use data_lane::DataBridge;
pub use data_lane::{CtrlTx as DataLaneCtrlTx};

use crate::uplink_config::{ActionRoute, Config, DeviceConfig};
use crate::{Action, ActionCallback, ActionResponse};
use crate::base::bridge::stream::MessageBuffer;

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

/// Commands that can be used to remotely trigger data_lane shutdown
pub struct DataBridgeShutdown;

pub struct Bridge {
    pub data: DataBridge,
    pub actions: ActionsBridge,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        device_config: Arc<DeviceConfig>,
        package_tx: Sender<Box<MessageBuffer>>,
        metrics_tx: Sender<StreamMetrics>,
        actions_rx: Receiver<Action>,
        actions_callback: Option<ActionCallback>,
    ) -> Self {
        let data = DataBridge::new(
            config.clone(),
            device_config.clone(),
            package_tx.clone(),
            metrics_tx.clone(),
        );
        let actions = ActionsBridge::new(
            config,
            actions_rx,
            data.data_tx.clone(),
            actions_callback,
        );
        Self { data, actions }
    }

    /// Handle to send data/action status messages
    pub fn bridge_tx(&self) -> BridgeTx {
        BridgeTx { data_tx: self.data.data_tx.clone(), status_tx: self.actions.status_tx.clone() }
    }

    pub(crate) fn ctrl_tx(&self) -> data_lane::CtrlTx {
        self.data.ctrl_tx()
    }

    pub fn register_action_route(&mut self, route: ActionRoute) -> Result<Receiver<Action>, Error> {
        let (actions_tx, actions_rx) = bounded(1);
        self.actions.register_action_route(route, actions_tx)?;

        Ok(actions_rx)
    }

    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
    ) -> Result<Receiver<Action>, Error> {
        let (actions_tx, actions_rx) = bounded(16);
        self.actions.register_action_routes(routes, actions_tx)?;

        Ok(actions_rx)
    }
}

#[derive(Debug, Clone)]
pub struct BridgeTx {
    pub data_tx: Sender<Payload>,
    pub status_tx: Sender<ActionResponse>,
}

impl BridgeTx {
    pub async fn send_payload(&self, payload: Payload) {
        let _ = self.data_tx.send_async(payload).await;
    }

    pub fn send_payload_sync(&self, payload: Payload) {
       let _ = self.data_tx.send(payload);
    }

    pub async fn send_action_response(&self, response: ActionResponse) {
        let _ = self.status_tx.send_async(response).await;
    }

    pub fn send_action_response_sync(&self, response: ActionResponse) {
        let _ = self.status_tx.send(response);
    }
}
