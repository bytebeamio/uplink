use flume::{bounded, Receiver, RecvError, Sender, TrySendError};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::time::{self, interval, Instant, Sleep};

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::{collections::HashMap, fmt::Debug, pin::Pin, sync::Arc, time::Duration};

use super::streams::Streams;
use super::{ActionBridgeShutdown, Package, StreamMetrics};
use crate::base::ActionRoute;
use crate::{Action, ActionResponse, Config};

const TUNSHELL_ACTION: &str = "launch_shell";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Receiver error {0}")]
    Recv(#[from] RecvError),
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Action receiver busy or down")]
    UnresponsiveReceiver,
    #[error("No route for action {0}")]
    NoRoute(String),
    #[error("Action timedout")]
    ActionTimeout,
    #[error("Another action is currently being processed")]
    Busy,
    #[error("Action Route clash: \"{0}\"")]
    ActionRouteClash(String),
}

struct RedirectionError(Action);

pub struct ActionsBridge {
    /// All configuration
    config: Arc<Config>,
    /// Tx handle to give to apps
    status_tx: Sender<ActionResponse>,
    /// Rx to receive action status from apps
    status_rx: Receiver<ActionResponse>,
    /// Actions incoming from backend
    actions_rx: Receiver<Action>,
    /// Contains stream to send ActionResponses on
    streams: Streams,
    /// Apps registered with the bridge
    /// NOTE: Sometimes action_routes could overlap, the latest route
    /// to be registered will be used in such a circumstance.
    action_routes: HashMap<String, ActionRouter>,
    /// Action redirections
    action_redirections: HashMap<String, String>,
    /// Current action that is being processed
    current_action: Option<CurrentAction>,
    parallel_actions: HashSet<String>,
    ctrl_rx: Receiver<ActionBridgeShutdown>,
    ctrl_tx: Sender<ActionBridgeShutdown>,
    shutdown_handle: Sender<()>,
}

impl ActionsBridge {
    pub fn new(
        config: Arc<Config>,
        package_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        shutdown_handle: Sender<()>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        let (status_tx, status_rx) = bounded(10);
        let action_redirections = config.action_redirections.clone();
        let (ctrl_tx, ctrl_rx) = bounded(1);

        let mut streams_config = HashMap::new();
        streams_config.insert("action_status".to_owned(), config.action_status.clone());
        let mut streams = Streams::new(config.clone(), package_tx, metrics_tx);
        streams.config_streams(streams_config);

        Self {
            status_tx,
            status_rx,
            config,
            actions_rx,
            streams,
            action_routes: HashMap::with_capacity(10),
            action_redirections,
            current_action: None,
            parallel_actions: HashSet::new(),
            shutdown_handle,
            ctrl_rx,
            ctrl_tx,
        }
    }

    pub fn register_action_route(
        &mut self,
        ActionRoute { name, timeout }: ActionRoute,
        actions_tx: Sender<Action>,
    ) -> Result<(), Error> {
        let duration = Duration::from_secs(timeout);
        let action_router = ActionRouter { actions_tx, duration };
        if self.action_routes.insert(name.clone(), action_router).is_some() {
            return Err(Error::ActionRouteClash(name));
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

    pub fn tx(&self) -> ActionsBridgeTx {
        ActionsBridgeTx { status_tx: self.status_tx.clone(), shutdown_handle: self.ctrl_tx.clone() }
    }

    fn clear_current_action(&mut self) {
        self.current_action.take();
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut metrics_timeout = interval(Duration::from_secs(self.config.stream_metrics.timeout));
        let mut end: Pin<Box<Sleep>> = Box::pin(time::sleep(Duration::from_secs(u64::MAX)));
        self.load_saved_action()?;

        loop {
            select! {
                action = self.actions_rx.recv_async() => {
                    let action = action?;
                    self.handle_action(action).await;
                }
                response = self.status_rx.recv_async() => {
                    let response = response?;
                    self.forward_action_response(response).await;
                }
                _ = &mut self.current_action.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                    let action = self.current_action.take().unwrap();
                    error!("Timeout waiting for action response. Action ID = {}", action.id);
                    self.forward_action_error(action.action, Error::ActionTimeout).await;

                    // Remove action because it timedout
                    self.clear_current_action()
                }
                // Flush streams that timeout
                Some(timedout_stream) = self.streams.stream_timeouts.next(), if self.streams.stream_timeouts.has_pending() => {
                    debug!("Flushing stream = {}", timedout_stream);
                    if let Err(e) = self.streams.flush_stream(&timedout_stream).await {
                        error!("Failed to flush stream = {}. Error = {}", timedout_stream, e);
                    }
                }
                // Flush all metrics when timed out
                _ = metrics_timeout.tick() => {
                    if let Err(e) = self.streams.check_and_flush_metrics() {
                        debug!("Failed to flush stream metrics. Error = {}", e);
                    }
                }
                // Handle a shutdown signal
                _ = self.ctrl_rx.recv_async() => {
                    if let Err(e) = self.save_current_action() {
                        error!("Failed to save current action: {e}");
                    }
                    // NOTE: there might be events still waiting for recv on bridge_rx
                    self.shutdown_handle.send(()).unwrap();

                    return Ok(())
                }
            }
        }
    }

    async fn handle_action(&mut self, action: Action) {
        let action_id = action.action_id.clone();
        // Reactlabs setup processes logs generated by uplink
        info!("Received action: {:?}", action);

        if let Some(current_action) = &self.current_action {
            if action.name != TUNSHELL_ACTION {
                warn!(
                    "Another action is currently occupying uplink; action_id = {}",
                    current_action.id
                );
                self.forward_action_error(action, Error::Busy).await;
                return;
            }
        }

        // NOTE: Don't do any blocking operations here
        // TODO: Remove blocking here. Audit all blocking functions here
        let error = match self.try_route_action(action.clone()) {
            Ok(_) => {
                let response = ActionResponse::progress(&action_id, "Received", 0);
                self.forward_action_response(response).await;
                return;
            }
            Err(e) => e,
        };

        // Remove action because it couldn't be routed
        self.clear_current_action();

        // Ignore sending failure status to backend. This makes
        // backend retry action.
        //
        // TODO: Do we need this? Shouldn't backend have an easy way to
        // retry failed actions in bulk?
        if self.config.ignore_actions_if_no_clients {
            error!("No clients connected, ignoring action = {:?}", action_id);
            return;
        }

        error!("Failed to route action to app. Error = {:?}", error);
        self.forward_action_error(action, error).await;
    }

    /// Save current action information in persistence
    fn save_current_action(&mut self) -> Result<(), Error> {
        let current_action = match self.current_action.take() {
            Some(c) => c,
            None => return Ok(()),
        };
        let mut path = self.config.persistence_path.clone();
        path.push("current_action");
        info!("Storing current action in persistence; path: {}", path.display());
        current_action.write_to_disk(path)?;

        Ok(())
    }

    /// Load a saved action from persistence, performed on startup
    fn load_saved_action(&mut self) -> Result<(), Error> {
        let mut path = self.config.persistence_path.clone();
        path.push("current_action");

        if path.is_file() {
            let current_action = CurrentAction::read_from_disk(path)?;
            info!("Loading saved action from persistence; action_id: {}", current_action.id);
            self.current_action = Some(current_action)
        }

        Ok(())
    }

    /// Handle received actions
    fn try_route_action(&mut self, action: Action) -> Result<(), Error> {
        let route = self
            .action_routes
            .get(&action.name)
            .ok_or_else(|| Error::NoRoute(action.name.clone()))?;

        let duration = route.try_send(action.clone()).map_err(|_| Error::UnresponsiveReceiver)?;
        // current action left unchanged in case of new tunshell action
        if action.name == TUNSHELL_ACTION {
            self.parallel_actions.insert(action.action_id);
            return Ok(());
        }

        self.current_action = Some(CurrentAction::new(action, duration));

        Ok(())
    }

    async fn forward_action_response(&mut self, response: ActionResponse) {
        if self.parallel_actions.contains(&response.action_id) {
            self.forward_parallel_action_response(response).await;

            return;
        }

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
        self.streams.forward(response.as_payload()).await;

        if response.is_completed() || response.is_failed() {
            self.clear_current_action();
            return;
        }

        // Forward actions included in the config to the appropriate forward route, when
        // they have reached 100% progress but haven't been marked as "Completed"/"Finished".
        if response.is_done() {
            let mut action = inflight_action.action.clone();

            if let Some(a) = response.done_response {
                action = a;
            }

            if let Err(RedirectionError(action)) = self.redirect_action(action).await {
                // NOTE: send success reponse for actions that don't have redirections configured
                warn!("Action redirection is not configured for: {:?}", action);
                let response = ActionResponse::success(&action.action_id);
                self.streams.forward(response.as_payload()).await;

                self.clear_current_action();
            }
        }
    }

    async fn redirect_action(&mut self, mut action: Action) -> Result<(), RedirectionError> {
        let fwd_name = self
            .action_redirections
            .get(&action.name)
            .ok_or_else(|| RedirectionError(action.clone()))?;

        debug!(
            "Redirecting action: {} ~> {}; action_id = {}",
            action.name, fwd_name, action.action_id,
        );

        action.name = fwd_name.to_owned();

        if let Err(e) = self.try_route_action(action.clone()) {
            error!("Failed to route action to app. Error = {:?}", e);
            self.forward_action_error(action, e).await;

            // Remove action because it couldn't be forwarded
            self.clear_current_action()
        }

        Ok(())
    }

    async fn forward_parallel_action_response(&mut self, response: ActionResponse) {
        info!("Action response = {:?}", response);
        self.streams.forward(response.as_payload()).await;

        if response.is_completed() || response.is_failed() {
            self.parallel_actions.remove(&response.action_id);
        }
    }

    async fn forward_action_error(&mut self, action: Action, error: Error) {
        let response = ActionResponse::failure(&action.action_id, error.to_string());

        self.streams.forward(response.as_payload()).await;
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SaveAction {
    pub id: String,
    pub action: Action,
    pub timeout: Duration,
}

struct CurrentAction {
    pub id: String,
    pub action: Action,
    pub timeout: Pin<Box<Sleep>>,
}

impl CurrentAction {
    pub fn new(action: Action, duration: Duration) -> CurrentAction {
        CurrentAction {
            id: action.action_id.clone(),
            action,
            timeout: Box::pin(time::sleep(duration)),
        }
    }

    pub fn write_to_disk(self, path: PathBuf) -> Result<(), Error> {
        let timeout = self.timeout.as_ref().deadline() - Instant::now();
        let save_action = SaveAction { id: self.id, action: self.action, timeout };
        let json = serde_json::to_string(&save_action)?;
        fs::write(path, json)?;

        Ok(())
    }

    pub fn read_from_disk(path: PathBuf) -> Result<Self, Error> {
        let read = fs::read(&path)?;
        let json: SaveAction = serde_json::from_slice(&read)?;
        fs::remove_file(path)?;

        Ok(CurrentAction {
            id: json.id,
            action: json.action,
            timeout: Box::pin(time::sleep(json.timeout)),
        })
    }
}

#[derive(Debug)]
pub struct ActionRouter {
    pub(crate) actions_tx: Sender<Action>,
    duration: Duration,
}

impl ActionRouter {
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, action: Action) -> Result<Duration, TrySendError<Action>> {
        self.actions_tx.try_send(action)?;

        Ok(self.duration)
    }
}

#[derive(Debug, Clone)]
pub struct ActionsBridgeTx {
    // Handle for apps to send action status to bridge
    pub(crate) status_tx: Sender<ActionResponse>,
    pub(crate) shutdown_handle: Sender<ActionBridgeShutdown>,
}

impl ActionsBridgeTx {
    pub async fn send_action_response(&self, response: ActionResponse) {
        self.status_tx.send_async(response).await.unwrap()
    }

    pub async fn trigger_shutdown(&self) {
        self.shutdown_handle.send_async(ActionBridgeShutdown).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use flume::{bounded, Receiver, Sender};
    use tokio::{runtime::Runtime, select};

    use crate::{
        base::{ActionRoute, StreamConfig, StreamMetricsConfig},
        Action, ActionResponse, Config,
    };

    use super::*;

    fn default_config() -> Config {
        Config {
            stream_metrics: StreamMetricsConfig {
                enabled: false,
                timeout: 10,
                ..Default::default()
            },
            action_status: StreamConfig { flush_period: 2, ..Default::default() },
            ..Default::default()
        }
    }

    fn create_bridge(
        config: Arc<Config>,
    ) -> (ActionsBridge, Sender<Action>, Receiver<Box<dyn Package>>) {
        let (data_tx, data_rx) = bounded(10);
        let (actions_tx, actions_rx) = bounded(10);
        let (shutdown_handle, _) = bounded(1);
        let (metrics_tx, _) = bounded(1);
        let bridge = ActionsBridge::new(config, data_tx, actions_rx, shutdown_handle, metrics_tx);

        (bridge, actions_tx, data_rx)
    }

    fn spawn_bridge(mut bridge: ActionsBridge) {
        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async { bridge.start().await.unwrap() });
        });
    }

    struct Responses {
        rx: Receiver<Box<dyn Package>>,
        responses: Vec<ActionResponse>,
    }

    impl Responses {
        fn next(&mut self) -> ActionResponse {
            if self.responses.is_empty() {
                let status = self.rx.recv().unwrap().serialize().unwrap();
                self.responses = serde_json::from_slice(&status).unwrap();
            }

            self.responses.remove(0)
        }
    }

    #[tokio::test]
    async fn timeout_on_diff_routes() {
        let tmpdir = tempdir::TempDir::new("bridge").unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();
        let config = Arc::new(default_config());
        let (mut bridge, actions_tx, data_rx) = create_bridge(config);
        let route_1 = ActionRoute { name: "route_1".to_string(), timeout: 10 };

        let (route_tx, route_1_rx) = bounded(1);
        bridge.register_action_route(route_1, route_tx).unwrap();

        let (route_tx, route_2_rx) = bounded(1);
        let route_2 = ActionRoute { name: "route_2".to_string(), timeout: 30 };
        bridge.register_action_route(route_2, route_tx).unwrap();

        spawn_bridge(bridge);

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                loop {
                    select! {
                        action = route_1_rx.recv_async() => {
                            let action = action.unwrap();
                            assert_eq!(action.action_id, "1".to_owned());
                        }

                        action = route_2_rx.recv_async() => {
                            let action = action.unwrap();
                            assert_eq!(action.action_id, "2".to_owned());
                        }
                    }
                }
            });
        });

        std::thread::sleep(Duration::from_secs(1));

        let action_1 = Action {
            action_id: "1".to_string(),
            kind: "test".to_string(),
            name: "route_1".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action_1).unwrap();

        let mut responses = Responses { rx: data_rx, responses: vec![] };

        let status = responses.next();
        assert_eq!(status.state, "Received".to_owned());
        let start = status.timestamp;

        let status = responses.next();
        // verify response is timeout failure
        assert!(status.is_failed());
        assert_eq!(status.action_id, "1".to_owned());
        assert_eq!(status.errors, ["Action timedout"]);
        let elapsed = status.timestamp - start;
        // verify timeout in 10s
        assert_eq!(elapsed / 1000, 10);

        let action_2 = Action {
            action_id: "2".to_string(),
            kind: "test".to_string(),
            name: "route_2".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action_2).unwrap();

        let status = responses.next();
        assert_eq!(status.state, "Received".to_owned());
        let start = status.timestamp;

        let status = responses.next();
        // verify response is timeout failure
        assert!(status.is_failed());
        assert_eq!(status.action_id, "2".to_owned());
        assert_eq!(status.errors, ["Action timedout"]);
        let elapsed = status.timestamp - start;
        // verify timeout in 30s
        assert_eq!(elapsed / 1000, 30);
    }

    #[tokio::test]
    async fn recv_action_while_current_action_exists() {
        let tmpdir = tempdir::TempDir::new("bridge").unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();
        let config = Arc::new(default_config());
        let (mut bridge, actions_tx, data_rx) = create_bridge(config);

        let test_route = ActionRoute { name: "test".to_string(), timeout: 30 };

        let (route_tx, action_rx) = bounded(1);
        bridge.register_action_route(test_route, route_tx).unwrap();

        spawn_bridge(bridge);

        std::thread::spawn(move || loop {
            let action = action_rx.recv().unwrap();
            assert_eq!(action.action_id, "1".to_owned());
        });

        std::thread::sleep(Duration::from_secs(1));

        let action_1 = Action {
            action_id: "1".to_string(),
            kind: "test".to_string(),
            name: "test".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action_1).unwrap();

        let mut responses = Responses { rx: data_rx, responses: vec![] };

        let status = responses.next();
        assert_eq!(status.action_id, "1".to_owned());
        assert_eq!(status.state, "Received".to_owned());

        let action_2 = Action {
            action_id: "2".to_string(),
            kind: "test".to_string(),
            name: "test".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action_2).unwrap();

        let status = responses.next();
        // verify response is uplink occupied failure
        assert!(status.is_failed());
        assert_eq!(status.action_id, "2".to_owned());
        assert_eq!(status.errors, ["Another action is currently being processed"]);
    }

    #[tokio::test]
    async fn complete_response_on_no_redirection() {
        let tmpdir = tempdir::TempDir::new("bridge").unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();
        let config = Arc::new(default_config());
        let (mut bridge, actions_tx, data_rx) = create_bridge(config);

        let test_route = ActionRoute { name: "test".to_string(), timeout: 30 };

        let (route_tx, action_rx) = bounded(1);
        bridge.register_action_route(test_route, route_tx).unwrap();
        let bridge_tx = bridge.tx();

        spawn_bridge(bridge);

        std::thread::spawn(move || loop {
            let action = action_rx.recv().unwrap();
            assert_eq!(action.action_id, "1".to_owned());
            std::thread::sleep(Duration::from_secs(1));
            let response = ActionResponse::progress("1", "Tested", 100);
            Runtime::new().unwrap().block_on(bridge_tx.send_action_response(response));
        });

        std::thread::sleep(Duration::from_secs(1));

        let action = Action {
            action_id: "1".to_string(),
            kind: "test".to_string(),
            name: "test".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action).unwrap();

        let mut responses = Responses { rx: data_rx, responses: vec![] };

        let status = responses.next();
        assert_eq!(status.state, "Received".to_owned());

        let status = responses.next();
        assert!(status.is_done());
        assert_eq!(status.state, "Tested");

        let status = responses.next();
        assert!(status.is_completed());
    }

    #[tokio::test]
    async fn no_complete_response_between_redirection() {
        let tmpdir = tempdir::TempDir::new("bridge").unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();
        let mut config = default_config();
        config.action_redirections.insert("test".to_string(), "redirect".to_string());
        let (mut bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));
        let bridge_tx_1 = bridge.tx();
        let bridge_tx_2 = bridge.tx();

        let (route_tx, action_rx_1) = bounded(1);
        let test_route = ActionRoute { name: "test".to_string(), timeout: 30 };
        bridge.register_action_route(test_route, route_tx).unwrap();

        let (route_tx, action_rx_2) = bounded(1);
        let redirect_route = ActionRoute { name: "redirect".to_string(), timeout: 30 };
        bridge.register_action_route(redirect_route, route_tx).unwrap();

        spawn_bridge(bridge);

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let action = action_rx_1.recv().unwrap();
            assert_eq!(action.action_id, "1".to_owned());
            std::thread::sleep(Duration::from_secs(1));
            let response = ActionResponse::progress("1", "Tested", 100);
            rt.block_on(bridge_tx_1.send_action_response(response));
        });

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let action = action_rx_2.recv().unwrap();
            assert_eq!(action.action_id, "1".to_owned());
            let response = ActionResponse::progress("1", "Redirected", 0);
            rt.block_on(bridge_tx_2.send_action_response(response));
            std::thread::sleep(Duration::from_secs(1));
            let response = ActionResponse::success("1");
            rt.block_on(bridge_tx_2.send_action_response(response));
        });

        std::thread::sleep(Duration::from_secs(1));

        let action = Action {
            action_id: "1".to_string(),
            kind: "test".to_string(),
            name: "test".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action).unwrap();

        let mut responses = Responses { rx: data_rx, responses: vec![] };

        let status = responses.next();
        assert_eq!(status.state, "Received".to_owned());

        let status = responses.next();
        assert!(status.is_done());
        assert_eq!(status.state, "Tested");

        let status = responses.next();
        assert!(!status.is_completed());
        assert_eq!(status.state, "Redirected");

        let status = responses.next();
        assert!(status.is_completed());
    }

    #[tokio::test]
    async fn accept_regular_actions_during_tunshell() {
        let tmpdir = tempdir::TempDir::new("bridge").unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();
        let config = default_config();
        let (mut bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));
        let bridge_tx_1 = bridge.tx();
        let bridge_tx_2 = bridge.tx();

        let (route_tx, action_rx_1) = bounded(1);
        let tunshell_route = ActionRoute { name: TUNSHELL_ACTION.to_string(), timeout: 30 };
        bridge.register_action_route(tunshell_route, route_tx).unwrap();

        let (route_tx, action_rx_2) = bounded(1);
        let test_route = ActionRoute { name: "test".to_string(), timeout: 30 };
        bridge.register_action_route(test_route, route_tx).unwrap();

        spawn_bridge(bridge);

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let action = action_rx_1.recv().unwrap();
            assert_eq!(action.action_id, "1");
            let response = ActionResponse::progress(&action.action_id, "Launched", 0);
            rt.block_on(bridge_tx_1.send_action_response(response));
            std::thread::sleep(Duration::from_secs(3));
            let response = ActionResponse::success(&action.action_id);
            rt.block_on(bridge_tx_1.send_action_response(response));
        });

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let action = action_rx_2.recv().unwrap();
            assert_eq!(action.action_id, "2");
            let response = ActionResponse::progress(&action.action_id, "Running", 0);
            rt.block_on(bridge_tx_2.send_action_response(response));
            std::thread::sleep(Duration::from_secs(1));
            let response = ActionResponse::success(&action.action_id);
            rt.block_on(bridge_tx_2.send_action_response(response));
        });

        std::thread::sleep(Duration::from_secs(1));

        let action = Action {
            action_id: "1".to_string(),
            kind: "tunshell".to_string(),
            name: "launch_shell".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let action = Action {
            action_id: "2".to_string(),
            kind: "test".to_string(),
            name: "test".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action).unwrap();

        let mut responses = Responses { rx: data_rx, responses: vec![] };

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "1");
        assert_eq!(state, "Received");

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "1");
        assert_eq!(state, "Launched");

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "2");
        assert_eq!(state, "Received");

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "2");
        assert_eq!(state, "Running");

        let status = responses.next();
        assert_eq!(status.action_id, "2");
        assert!(status.is_completed());

        let status = responses.next();
        assert_eq!(status.action_id, "1");
        assert!(status.is_completed());
    }

    #[tokio::test]
    async fn accept_tunshell_during_regular_action() {
        let tmpdir = tempdir::TempDir::new("bridge").unwrap();
        std::env::set_current_dir(&tmpdir).unwrap();
        let config = default_config();
        let (mut bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));
        let bridge_tx_1 = bridge.tx();
        let bridge_tx_2 = bridge.tx();

        let (route_tx, action_rx_1) = bounded(1);
        let test_route = ActionRoute { name: "test".to_string(), timeout: 30 };
        bridge.register_action_route(test_route, route_tx).unwrap();

        let (route_tx, action_rx_2) = bounded(1);
        let tunshell_route = ActionRoute { name: TUNSHELL_ACTION.to_string(), timeout: 30 };
        bridge.register_action_route(tunshell_route, route_tx).unwrap();

        spawn_bridge(bridge);

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let action = action_rx_1.recv().unwrap();
            assert_eq!(action.action_id, "1");
            let response = ActionResponse::progress(&action.action_id, "Running", 0);
            rt.block_on(bridge_tx_1.send_action_response(response));
            std::thread::sleep(Duration::from_secs(3));
            let response = ActionResponse::success(&action.action_id);
            rt.block_on(bridge_tx_1.send_action_response(response));
        });

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let action = action_rx_2.recv().unwrap();
            assert_eq!(action.action_id, "2");
            let response = ActionResponse::progress(&action.action_id, "Launched", 0);
            rt.block_on(bridge_tx_2.send_action_response(response));
            std::thread::sleep(Duration::from_secs(1));
            let response = ActionResponse::success(&action.action_id);
            rt.block_on(bridge_tx_2.send_action_response(response));
        });

        std::thread::sleep(Duration::from_secs(1));

        let action = Action {
            action_id: "1".to_string(),
            kind: "test".to_string(),
            name: "test".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let action = Action {
            action_id: "2".to_string(),
            kind: "tunshell".to_string(),
            name: "launch_shell".to_string(),
            payload: "test".to_string(),
        };
        actions_tx.send(action).unwrap();

        let mut responses = Responses { rx: data_rx, responses: vec![] };

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "1");
        assert_eq!(state, "Received");

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "1");
        assert_eq!(state, "Running");

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "2");
        assert_eq!(state, "Received");

        let ActionResponse { action_id, state, .. } = responses.next();
        assert_eq!(action_id, "2");
        assert_eq!(state, "Launched");

        let status = responses.next();
        assert_eq!(status.action_id, "2");
        assert!(status.is_completed());

        let status = responses.next();
        assert_eq!(status.action_id, "1");
        assert!(status.is_completed());
    }
}
