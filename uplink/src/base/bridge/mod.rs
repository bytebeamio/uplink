use std::{fmt::Debug, sync::Arc};

use flume::{bounded, Receiver, Sender};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod actions_lane;
mod data_lane;
mod delaymap;
mod metrics;
pub mod stream;
mod streams;

pub use actions_lane::{ActionsBridge, CtrlTx as ActionsLaneCtrlTx, Error, StatusTx};
pub use data_lane::{CtrlTx as DataLaneCtrlTx, DataBridge, DataTx};
pub use metrics::StreamMetrics;

use crate::config::{ActionRoute, Config, DeviceConfig, StreamConfig};
use crate::{Action, ActionResponse};

/// Trait representing a data point with timestamp and sequence information
pub trait Point: Send + Debug + Serialize + 'static {
    fn stream_name(&self) -> &str;
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

/// Trait for a data package, with configuration and serialization methods
pub trait Package: Send + Debug {
    fn stream_config(&self) -> Arc<StreamConfig>;
    fn stream_name(&self) -> Arc<String>;
    fn serialize(&self) -> serde_json::Result<Vec<u8>>;
    fn anomalies(&self) -> Option<(String, usize)>;
    fn len(&self) -> usize;
    fn latency(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Represents the payload structure for messages sent over the bridge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    // Stream name associated with the payload
    #[serde(skip_serializing)]
    pub stream: String,
    // Sequence number of the payload
    pub sequence: u32,
    // Timestamp of the payload
    pub timestamp: u64,
    // JSON payload data
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

/// Command for shutting down the action lane
pub(crate) struct ActionBridgeShutdown;

/// Command for shutting down the data lane
pub(crate) struct DataBridgeShutdown;

/// Main bridge structure, managing data and action flows
pub struct Bridge {
    // Manages data flow and processing
    pub(crate) data: DataBridge,
    // Manages actions flow and processing
    pub(crate) actions: ActionsBridge,
}

impl Bridge {
    /// Creates a new Bridge instance with configuration, device, and control channels
    pub fn new(
        config: Arc<Config>,
        device_config: Arc<DeviceConfig>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
        actions_rx: Receiver<Action>,
        shutdown_handle: Sender<()>,
    ) -> Self {
        let data = DataBridge::new(
            config.clone(),
            device_config.clone(),
            package_tx.clone(),
            metrics_tx.clone(),
        );
        let actions = ActionsBridge::new(
            config,
            device_config,
            package_tx,
            actions_rx,
            shutdown_handle,
            metrics_tx,
        );
        Self { data, actions }
    }

    /// Provides handles for data and action status message transmission
    pub fn bridge_tx(&self) -> BridgeTx {
        BridgeTx { data_tx: self.data.data_tx(), status_tx: self.actions.status_tx() }
    }

    /// Provides control transmitters for actions and data lanes
    pub(crate) fn ctrl_tx(&self) -> (actions_lane::CtrlTx, data_lane::CtrlTx) {
        (self.actions.ctrl_tx(), self.data.ctrl_tx())
    }

    /// Registers a route for handling a specific action, returning a receiver for actions
    pub fn register_action_route(&mut self, route: ActionRoute) -> Result<Receiver<Action>, Error> {
        let (actions_tx, actions_rx) = bounded(1);
        self.actions.register_action_route(route, actions_tx)?;

        Ok(actions_rx)
    }

    /// Registers multiple action routes for batch handling of actions
    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
    ) -> Result<Receiver<Action>, Error> {
        let (actions_tx, actions_rx) = bounded(1);
        self.actions.register_action_routes(routes, actions_tx)?;

        Ok(actions_rx)
    }
}

/// Provides message transmission capabilities for payloads and action responses
#[derive(Debug, Clone)]
pub struct BridgeTx {
    // Data transmission channel
    pub data_tx: DataTx,
    // Action status transmission channel
    pub status_tx: StatusTx,
}

impl BridgeTx {
    /// Sends a payload asynchronously
    pub async fn send_payload(&self, payload: Payload) {
        self.data_tx.send_payload(payload).await
    }

    /// Sends a payload synchronously
    pub fn send_payload_sync(&self, payload: Payload) {
        self.data_tx.send_payload_sync(payload)
    }

    /// Sends an action response asynchronously
    pub async fn send_action_response(&self, response: ActionResponse) {
        self.status_tx.send_action_response(response).await
    }

    /// Sends an action response synchronously
    pub fn send_action_response_sync(&self, response: ActionResponse) {
        self.status_tx.send_action_response_sync(response)
    }
}
