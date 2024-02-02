use base::{CollectorRx, CollectorTx, Payload};
use flume::{bounded, Receiver, Sender};

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

use crate::config::{ActionRoute, Config, StreamConfig};
use base::{Action, ActionResponse};
pub use metrics::StreamMetrics;
pub use stream::MAX_BUFFER_SIZE;

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
/// Commands that can be used to remotely trigger action_lane shutdown
pub(crate) struct ActionBridgeShutdown;

/// Commands that can be used to remotely trigger data_lane shutdown
pub(crate) struct DataBridgeShutdown;

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
        let data = DataBridge::new(config.clone(), package_tx.clone(), metrics_tx.clone());
        let actions =
            ActionsBridge::new(config, package_tx, actions_rx, shutdown_handle, metrics_tx);
        Self { data, actions }
    }

    /// Handle to send data/action status messages
    pub fn bridge_tx(&self) -> BridgeTx {
        BridgeTx { data_tx: self.data.data_tx(), status_tx: self.actions.status_tx() }
    }

    pub(crate) fn ctrl_tx(&self) -> (actions_lane::CtrlTx, data_lane::CtrlTx) {
        (self.actions.ctrl_tx(), self.data.ctrl_tx())
    }

    pub fn register_action_route(&mut self, route: ActionRoute) -> Result<ActionsRx, Error> {
        let (actions_tx, actions_rx) = bounded(1);
        self.actions.register_action_route(route, actions_tx)?;

        Ok(ActionsRx { actions_rx })
    }

    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
    ) -> Result<ActionsRx, Error> {
        let (actions_tx, actions_rx) = bounded(1);
        self.actions.register_action_routes(routes, actions_tx)?;

        Ok(ActionsRx { actions_rx })
    }
}

#[derive(Debug, Clone)]
pub struct BridgeTx {
    pub data_tx: DataTx,
    pub status_tx: StatusTx,
}

#[async_trait::async_trait]
impl CollectorTx for BridgeTx {
    async fn send_payload(&mut self, payload: Payload) {
        self.data_tx.send_payload(payload).await
    }

    fn send_payload_sync(&mut self, payload: Payload) {
        self.data_tx.send_payload_sync(payload)
    }

    async fn send_action_response(&mut self, response: ActionResponse) {
        self.status_tx.send_action_response(response).await
    }
}

#[derive(Debug, Clone)]
pub struct ActionsRx {
    pub actions_rx: Receiver<Action>,
}

#[async_trait::async_trait]
impl CollectorRx for ActionsRx {
    async fn recv_action(&mut self) -> Option<Action> {
        self.actions_rx.recv_async().await.ok()
    }
}
