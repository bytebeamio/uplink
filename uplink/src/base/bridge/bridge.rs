use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use flume::{bounded, Receiver, RecvError, SendError, Sender};
use log::{error, info};
use tokio::{
    select,
    time::{self, interval, Sleep},
};

use super::{stream, Package, Payload, Stream, StreamMetrics};
use crate::{collector::utils::Streams, Action, ActionResponse, Config};

pub enum Event {
    /// App name and handle for brige to send actions to the app
    RegisterApp(String, Sender<Action>),
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
    apps: HashMap<String, Sender<Action>>,
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
            apps: HashMap::with_capacity(10),
        }
    }

    pub fn tx(&mut self) -> BridgeTx {
        BridgeTx { events_tx: self.bridge_tx.clone() }
    }

    pub fn register_app(&mut self, name: &str) -> (Sender<Event>, Receiver<Action>) {
        let (action_tx, action_rx) = bounded(1);
        self.apps.insert(name.to_owned(), action_tx);
        (self.bridge_tx.clone(), action_rx)
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut metrics_timeout = interval(Duration::from_secs(60));
        let mut streams =
            Streams::new(self.config.clone(), self.package_tx.clone(), self.metrics_tx.clone())
                .await;
        loop {
            let mut end = Box::pin(time::sleep(Duration::from_secs(u64::MAX)));

            // NOTE: We only expect one action to be processed over uplink's bridge at a time
            // - set to None when
            // -- timeout ends
            // -- A response with status "Completed" is received
            // - set to a value when
            // -- it is currently None and a new action is received
            // - timeout is updated
            // -- when a non "Completed" action is received
            let mut current_action: Option<CurrentAction> = None;
            loop {
                select! {
                    action = self.actions_rx.recv_async(), if current_action.is_none() => {
                        let action = action?;
                        let action_id = action.action_id.clone();

                        // if self.actions_tx.send(action).is_ok() {
                        //     info!("Received action: {:?}", action_id);
                        //     current_action = Some(CurrentAction::new(&action_id));
                        //     let response = ActionResponse::progress(&action_id, "Received", 0);
                        //     self.action_status.fill(response).await?;
                        // }

                        // else if self.config.ignore_actions_if_no_clients {
                        //     error!("No clients connected, ignoring action = {:?}", action_id);
                        // } else {
                        //     error!("Bridge down!! Action ID = {}", action_id);
                        //     let status = ActionResponse::failure(&action_id, "Bridge down");
                        //     if let Err(e) = self.action_status.fill(status).await {
                        //         error!("Failed to send busy status. Error = {:?}", e);
                        //     }
                        // }
                    }
                    event = self.bridge_rx.recv_async() => {
                        let event = event?;
                        match event {
                            Event::RegisterApp(name, tx) => {
                                self.apps.insert(name, tx);
                            }
                            Event::Data(v) => {
                                streams.forward(v).await;
                            }
                            Event::ActionResponse(response) => {
                                let inflight_action = match &mut current_action {
                                    Some(v) => v,
                                    None => {
                                        error!("Action timed out already or not present, ignoring response: {:?}", response);
                                        continue;
                                    }
                                };

                                if *inflight_action.id != response.action_id {
                                    error!("action_id in action_status({}) does not match that of active action ({})", response.action_id, inflight_action.id);
                                    continue;
                                }

                                if &response.state == "Completed" || &response.state == "Failed" {
                                    current_action = None;
                                }

                                if let Err(e) = self.action_status.fill(response).await {
                                    error!("Failed to fill. Error = {:?}", e);
                                }
                            }
                        }
                    }
                    _ = &mut current_action.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                        let action = current_action.take().unwrap();
                        error!("Timeout waiting for action response. Action ID = {}", action.id);

                        // Send failure response to cloud
                        let status = ActionResponse::failure(&action.id, "Action timed out");
                        if let Err(e) = self.action_status.fill(status).await {
                            error!("Failed to fill. Error = {:?}", e);
                        }
                    }
                    // Flush streams that timeout
                    Some(timedout_stream) = streams.stream_timeouts.next(), if streams.stream_timeouts.has_pending() => {
                        if let Err(e) = streams.flush_stream(&timedout_stream).await {
                            error!("Failed to flush stream = {}. Error = {}", timedout_stream, e);
                        }
                    }
                    // Flush stream metrics that timeout
                    Some(timedout_stream) = streams.metrics_timeouts.next(), if streams.metrics_timeouts.has_pending() => {
                        if let Err(e) = streams.flush_stream_metrics(&timedout_stream).await {
                            error!("Failed to flush stream metrics = {}. Error = {}", timedout_stream, e);
                        }
                    }
                    // Force flush all metrics when timed out
                    _ = metrics_timeout.tick() => {
                        if let Err(e) = streams.check_and_flush_metrics() {
                            error!("Failed to flush stream metrics. Error = {}", e);
                        }
                    }
                }
            }
        }
    }
}

struct CurrentAction {
    pub id: String,
    pub timeout: Pin<Box<Sleep>>,
}

impl CurrentAction {
    pub fn new(id: &str) -> CurrentAction {
        CurrentAction { id: id.to_owned(), timeout: Box::pin(time::sleep(Duration::from_secs(30))) }
    }

    pub fn reset_timeout(&mut self) {
        self.timeout = Box::pin(time::sleep(Duration::from_secs(30)));
    }
}

#[derive(Debug, Clone)]
pub struct BridgeTx {
    // Handle for apps to send events to bridge
    events_tx: Sender<Event>,
}

impl BridgeTx {
    pub async fn register_app(&self, name: &str) -> Receiver<Action> {
        let (actions_tx, actions_rx) = bounded(1);
        let event = Event::RegisterApp(name.to_owned(), actions_tx);

        // Bridge should always be up and hence unwrap is ok
        self.events_tx.send_async(event).await.unwrap();
        actions_rx
    }

    pub async fn send_payload(&self, payload: Payload) {
        let event = Event::Data(payload);
        self.events_tx.send_async(event).await.unwrap()
    }

    pub async fn send_action_response(&self, response: ActionResponse) {
        let event = Event::ActionResponse(response);
        self.events_tx.send_async(event).await.unwrap()
    }
}

struct App {
    name: String,
    tx: Sender<Action>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Receiver error {0}")]
    Recv(#[from] RecvError),
    #[error("Sender error {0}")]
    Send(#[from] SendError<ActionResponse>),
    #[error("Stream done")]
    StreamDone,
    #[error("Stream error")]
    Stream(#[from] stream::Error),
}
