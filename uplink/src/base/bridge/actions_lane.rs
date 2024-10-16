use flume::{bounded, Receiver, Sender, TrySendError};
use log::{debug, error, info, warn};
use tokio::select;
use tokio::time::{interval};

use std::{collections::HashMap, fmt::Debug, sync::Arc};
use std::fmt::{Display, Formatter};
use crate::base::actions::Cancellation;
use crate::config::{ActionRoute, Config, DeviceConfig};
use crate::{Action, ActionResponse};

use super::streams::Streams;
use super::{ActionBridgeShutdown, Package, StreamMetrics};

#[derive(Debug)]
pub enum Error {
    DuplicateActionRoutes { action_name: String },
    InvalidActionName { action_name: String }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {}

pub const RESERVED_ACTION_NAMES: [&str; 3] = ["*", "launch_shell", "cancel_action"];

pub struct ActionsBridge {
    /// Full configuration
    config: Arc<Config>,
    /// Tx handle to give to apps
    status_tx: Sender<ActionResponse>,
    /// Rx to receive action status from apps
    status_rx: Receiver<ActionResponse>,
    /// Actions incoming from backend
    actions_rx: Receiver<Action>,
    /// Contains stream to send ActionResponses on
    streams: Streams<ActionResponse>,
    /// Apps registered with the bridge
    /// NOTE: Sometimes action_routes could overlap, the latest route
    /// to be registered will be used in such a circumstance.
    action_routes: HashMap<String, ActionRouter>,
    /// Action redirections
    action_redirections: HashMap<String, String>,
}

impl ActionsBridge {
    pub fn new(
        config: Arc<Config>,
        device_config: Arc<DeviceConfig>,
        package_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        let (status_tx, status_rx) = bounded(10);
        let action_redirections = config.action_redirections.clone();

        let mut streams_config = HashMap::new();
        let mut action_status = config.action_status.clone();

        // TODO: Should be removed once action response batching is supported on platform
        if action_status.batch_size > 1 {
            warn!("Buffer size of `action_status` stream restricted to 1")
        }
        action_status.batch_size = 1;

        streams_config.insert("action_status".to_owned(), action_status);
        let mut streams = Streams::new(1, device_config, package_tx, metrics_tx);
        streams.config_streams(streams_config);

        Self {
            status_tx,
            status_rx,
            config,
            actions_rx,
            streams,
            action_routes: HashMap::with_capacity(16),
            action_redirections,
        }
    }

    pub fn register_action_route(
        &mut self,
        ActionRoute { name, cancellable }: ActionRoute,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        let action_router = ActionRouter { actions_tx, cancellable };
        if RESERVED_ACTION_NAMES.iter().find(|&&n| n == name).is_some() {
            return Err(Error::InvalidActionName { action_name: name });
        }
        if self.action_routes.insert(name.clone(), action_router).is_some() {
            return Err(Error::DuplicateActionRoutes { action_name: name });
        }

        Ok(())
    }

    pub fn register_action_routes<R: Into<ActionRoute>, V: IntoIterator<Item = R>>(
        &mut self,
        routes: V,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        for route in routes {
            self.register_action_route(route.into(), actions_tx.clone())?;
        }

        Ok(())
    }

    /// Handle to send action status messages from connected application
    pub fn status_tx(&self) -> StatusTx {
        StatusTx { inner: self.status_tx.clone() }
    }

    pub async fn start(&mut self) -> Result<(), String> {

        let mut metrics_timeout = interval(self.config.stream_metrics.timeout);

        loop {
            select! {
                action = self.actions_rx.recv_async() => {
                    let action = action.map_err(|e| format!("Encountered error when receiving action from broker: {e:?}"))?;

                    // Reactlabs setup processes logs generated by uplink
                    info!("Received action: {:?}", action);
                    self.forward_action_response(
                        ActionResponse::progress(action.action_id.as_str(), "ReceivedByUplink", 0)
                    ).await;

                    self.handle_incoming_action(action).await;
                }

                response = self.status_rx.recv_async() => {
                    let response = response.map_err(|e| format!("Encountered error when receiving status from a collector: {e:?}"))?;
                    self.forward_action_response(response).await;
                }

                // Flush streams that timeout
                Some(timedout_stream) = self.streams.stream_timeouts.next(), if self.streams.stream_timeouts.has_pending() => {
                    debug!("Flushing stream = {timedout_stream}");
                    if let Err(e) = self.streams.flush_stream(&timedout_stream).await {
                        error!("Failed to flush stream = {timedout_stream}. Error = {e}");
                    }
                }

                // Flush all metrics when timed out
                _ = metrics_timeout.tick() => {
                    if let Err(e) = self.streams.check_and_flush_metrics() {
                        debug!("Failed to flush stream metrics. Error = {e}");
                    }
                }

            }
        }
    }

    async fn handle_incoming_action(&mut self, action: Action) {
        Box::pin(async move {
            if action.name == "cancel_action" {
                match serde_json::from_str::<Cancellation>(action.payload.as_str()) {
                    Ok(payload) => {
                        self.try_route_action(payload.action_name.as_str(), action).await;
                    }
                    Err(_) => {
                        log::error!("Invalid cancel action payload: {:#?}", action.payload);
                        self.forward_action_response(
                            ActionResponse::failure(action.action_id.as_str(), format!("Invalid cancel action payload: {:#?}", action.payload))
                        ).await;
                    }
                }
            } else {
                self.try_route_action(action.name.as_str(), action.clone()).await;
            }
        }).await;
    }

    /// Handle received actions
    async fn try_route_action(&mut self, route_id: &str, action: Action) {
        match self.action_routes.get(route_id) {
            Some(route) => {
                if let Err(e) = route.try_send(action.clone()) {
                    log::error!("Could not forward action to collector: {e}");
                    self.forward_action_response(ActionResponse::failure(action.action_id.as_str(), format!("Could not forward action to collector: {e}"))).await;
                }
            }
            None => {
                match self
                    .action_routes
                    .get("*") {
                    Some(bus_route) => {
                        if let Err(e) = bus_route.try_send(action.clone()) {
                            log::error!("Could not forward action to collector: {e}");
                            self.forward_action_response(ActionResponse::failure(action.action_id.as_str(), format!("Could not forward action to collector: {e}"))).await;
                        } else {
                            debug!("Action routed to broker");
                        }
                    }
                    None => {
                        self.forward_action_response(ActionResponse::failure(action.action_id.as_str(), format!("Uplink isn't configured to handle actions of type {route_id}"))).await;
                    }
                }
            }
        }
    }

    async fn forward_action_response(&mut self, response: ActionResponse) {
        info!("Action response = {:?}", response);

        self.streams.forward(response.clone()).await;

        if let Some(mut redirected_action) = response.done_response {
            if let Some(target_action) = self.action_redirections.get(redirected_action.name.as_str()) {
                target_action.clone_into(&mut redirected_action.name);
                self.handle_incoming_action(redirected_action).await;
            }
        }
    }
}

#[derive(Debug)]
pub struct ActionRouter {
    pub(crate) actions_tx: Sender<Action>,
    cancellable: bool,
}

impl ActionRouter {
    #[allow(clippy::result_large_err)]
    /// Forwards action to the appropriate application and returns the instance in time at which it should be timedout if incomplete
    pub fn try_send(&self, action: Action) -> Result<(), TrySendError<Action>> {
        self.actions_tx.try_send(action)?;
        Ok(())
    }

    pub fn is_cancellable(&self) -> bool {
        self.cancellable
    }
}

/// Handle for apps to send action status to bridge
#[derive(Debug, Clone)]
pub struct StatusTx {
    pub inner: Sender<ActionResponse>,
}

impl StatusTx {
    pub async fn send_action_response(&self, response: ActionResponse) {
        self.inner.send_async(response).await.unwrap()
    }

    pub fn send_action_response_sync(&self, response: ActionResponse) {
        self.inner.send(response).unwrap()
    }
}

/// Handle to send control messages to action lane
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub(crate) inner: Sender<ActionBridgeShutdown>,
}

impl CtrlTx {
    /// Triggers shutdown of `bridge::actions_lane`
    pub async fn trigger_shutdown(&self) {
        self.inner.send_async(ActionBridgeShutdown).await.unwrap()
    }
}
