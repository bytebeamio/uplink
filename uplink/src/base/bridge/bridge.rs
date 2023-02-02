use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use flume::{bounded, Receiver, RecvError, SendError, Sender, TrySendError};
use log::{error, info};
use tokio::{
    select,
    time::{self, interval, Sleep},
};

use super::{Package, Payload, Stream, StreamMetrics};
use crate::{collector::utils::Streams, Action, ActionResponse, Config};

#[derive(Debug)]
pub enum Event {
    /// App name and handle for brige to send actions to the app
    RegisterActionRoute(String, Sender<Action>),
    /// Data sent by the app
    Data(Payload),
    /// Sometime apps can choose to directly send action response instead
    /// sending in `Payload` form
    ActionResponse(ActionResponse),
}

pub struct Bridge {
    /// All configuration
    config: Arc<Config>,
    /// Tx handle to give to apps
    bridge_tx: Sender<Event>,
    /// Rx to receive events from apps
    bridge_rx: Receiver<Event>,
    /// Handle to send batched data to serialzer
    package_tx: Sender<Box<dyn Package>>,
    /// Handle to send stream metrics to monitor
    metrics_tx: Sender<StreamMetrics>,
    /// Actions incoming from backend
    actions_rx: Receiver<Action>,
    /// Action responses going to backend
    action_status: Stream<ActionResponse>,
    /// Apps registered with the bridge
    /// NOTE: Sometimes action_routes could overlap, the latest route
    /// to be registered will be used in such a circumstance.
    action_routes: HashMap<String, Sender<Action>>,
    /// Current action that is being processed
    current_action: Option<CurrentAction>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Bridge {
        let (bridge_tx, bridge_rx) = bounded(10);

        Bridge {
            action_status,
            bridge_tx,
            bridge_rx,
            package_tx,
            metrics_tx,
            config,
            actions_rx,
            action_routes: HashMap::with_capacity(10),
            current_action: None,
        }
    }

    pub fn tx(&mut self) -> BridgeTx {
        BridgeTx { events_tx: self.bridge_tx.clone() }
    }

    fn clear_current_action(&mut self) {
        self.current_action = None;
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut metrics_timeout = interval(Duration::from_secs(self.config.stream_metrics.timeout));
        let mut streams =
            Streams::new(self.config.clone(), self.package_tx.clone(), self.metrics_tx.clone())
                .await;
        let mut end = Box::pin(time::sleep(Duration::from_secs(u64::MAX)));

        loop {
            select! {
                // TODO: Remove if guard
                action = self.actions_rx.recv_async(), if self.current_action.is_none() => {
                    let action = action?;
                    let action_id = action.action_id.clone();
                    info!("Received action: {:?}", action_id);

                    // NOTE: Don't do any blocking operations here
                    // TODO: Remove blocking here. Audit all blocking functions here
                    if let Err(e) = self.try_route_action(action.clone()) {
                        // Ignore sending failure status to backend. This makes
                        // backend retry action.
                        //
                        // TODO: Do we need this? Shouldn't backend have an easy way to
                        // retry failed actions in bulk?
                        if self.config.ignore_actions_if_no_clients {
                            error!("No clients connected, ignoring action = {:?}", action_id);
                            self.current_action = None;
                            continue
                        }

                        error!("Failed to route action to app. Error = {:?}", e);
                        self.forward_action_error(action, e).await;
                        continue
                    }

                    self.current_action = Some(CurrentAction::new(action.clone()));
                    let response = ActionResponse::progress(&action_id, "Received", 0);
                    self.forward_action_response(response).await;
                }
                event = self.bridge_rx.recv_async() => {
                    let event = event?;
                    match event {
                        Event::RegisterActionRoute(name, tx) => {
                            self.action_routes.insert(name, tx);
                        }
                        Event::Data(v) => {
                            streams.forward(v).await;
                        }
                        Event::ActionResponse(response) => {
                            self.forward_action_response(response).await;
                        }
                    }
                }
                _ = &mut self.current_action.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                    // Won't panic until the end of time
                    let action = self.current_action.take().unwrap();
                    error!("Timeout waiting for action response. Action ID = {}", action.id);
                    self.forward_action_error(action.action, Error::ActionTimeout).await;
                }
                // Flush streams that timeout
                Some(timedout_stream) = streams.stream_timeouts.next(), if streams.stream_timeouts.has_pending() => {
                    info!("Flushing stream = {}", timedout_stream);
                    if let Err(e) = streams.flush_stream(&timedout_stream).await {
                        error!("Failed to flush stream = {}. Error = {}", timedout_stream, e);
                    }
                }
                // Flush all metrics when timed out
                _ = metrics_timeout.tick() => {
                    if let Err(e) = streams.check_and_flush_metrics() {
                        error!("Failed to flush stream metrics. Error = {}", e);
                    }
                }
            }
        }
    }

    /// Handle received actions
    fn try_route_action(&mut self, action: Action) -> Result<(), Error> {
        let action_name = action.name.clone();
        match self.action_routes.get(&action_name) {
            Some(app_tx) => {
                app_tx.try_send(action)?;
                Ok(())
            }
            None => Err(Error::NoRoute(action.name)),
        }
    }

    async fn forward_action_response(&mut self, response: ActionResponse) {
        let inflight_action = match &mut self.current_action {
            Some(v) => v,
            None => {
                error!("Action timed out already/not present, ignoring response: {:?}", response);
                return;
            }
        };

        if *inflight_action.id != response.action_id {
            error!("response id({}) != active action({})", response.action_id, inflight_action.id);
            return;
        }

        info!("Action response = {:?}", response);
        if response.is_completed() || response.is_failed() {
            self.clear_current_action();
        }

        if let Err(e) = self.action_status.fill(response).await {
            error!("Failed to fill. Error = {:?}", e);
        }
    }

    async fn forward_action_error(&mut self, action: Action, error: Error) {
        let status = ActionResponse::failure(&action.action_id, error.to_string());

        if let Err(e) = self.action_status.fill(status).await {
            error!("Failed to send status. Error = {:?}", e);
        }

        self.clear_current_action();
    }
}

struct CurrentAction {
    pub id: String,
    pub action: Action,
    pub timeout: Pin<Box<Sleep>>,
}

impl CurrentAction {
    pub fn new(action: Action) -> CurrentAction {
        CurrentAction {
            id: action.action_id.clone(),
            action,
            timeout: Box::pin(time::sleep(Duration::from_secs(30))),
        }
    }

    pub fn reset_timeout(&mut self) {
        self.timeout = Box::pin(time::sleep(Duration::from_secs(30)));
    }
}

#[derive(Debug, Clone)]
pub struct BridgeTx {
    // Handle for apps to send events to bridge
    pub(crate) events_tx: Sender<Event>,
}

impl BridgeTx {
    pub async fn register_action_route<S: Into<String>>(&self, name: S) -> Receiver<Action> {
        let (actions_tx, actions_rx) = bounded(0);
        let event = Event::RegisterActionRoute(name.into(), actions_tx);

        // Bridge should always be up and hence unwrap is ok
        self.events_tx.send_async(event).await.unwrap();
        actions_rx
    }

    pub async fn register_action_routes<S: Into<String>, V: IntoIterator<Item = S>>(
        &self,
        names: V,
    ) -> Receiver<Action> {
        let (actions_tx, actions_rx) = bounded(0);

        for name in names {
            let event = Event::RegisterActionRoute(name.into(), actions_tx.clone());
            // Bridge should always be up and hence unwrap is ok
            self.events_tx.send_async(event).await.unwrap();
        }

        actions_rx
    }

    pub async fn send_payload(&self, payload: Payload) -> Result<(), SendError<Event>> {
        let event = Event::Data(payload);
        self.events_tx.send_async(event).await
    }

    pub fn send_payload_sync(&self, payload: Payload) -> Result<(), SendError<Event>> {
        let event = Event::Data(payload);
        self.events_tx.send(event)
    }

    pub async fn send_action_response(
        &self,
        response: ActionResponse,
    ) -> Result<(), SendError<Event>> {
        let event = Event::ActionResponse(response);
        self.events_tx.send_async(event).await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Receiver error {0}")]
    Recv(#[from] RecvError),
    #[error("Action receiver busy {0}")]
    TrySend(#[from] TrySendError<Action>),
    #[error("No route for action {0}")]
    NoRoute(String),
    #[error("Action timedout")]
    ActionTimeout,
}
