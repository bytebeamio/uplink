use std::{sync::Arc, time::Duration};

use flume::{bounded, Receiver, Sender};
use tokio::{runtime::Runtime, select};

use uplink::{
    base::bridge::{ActionsBridge, Package},
    config::{ActionRoute, Config, StreamConfig, StreamMetricsConfig},
    Action, ActionResponse,
};

fn default_config() -> Config {
    Config {
        stream_metrics: StreamMetricsConfig {
            enabled: false,
            timeout: Duration::from_secs(10),
            ..Default::default()
        },
        action_status: StreamConfig { flush_period: Duration::from_secs(2), ..Default::default() },
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
    let route_1 = ActionRoute {
        name: "route_1".to_string(),
        timeout: Duration::from_secs(10),
        cancellable: false,
    };

    let (route_tx, route_1_rx) = bounded(1);
    bridge.register_action_route(route_1, route_tx).unwrap();

    let (route_tx, route_2_rx) = bounded(1);
    let route_2 = ActionRoute {
        name: "route_2".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
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

    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };

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

    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };

    let (route_tx, action_rx) = bounded(1);
    bridge.register_action_route(test_route, route_tx).unwrap();
    let bridge_tx = bridge.status_tx();

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
    let bridge_tx_1 = bridge.status_tx();
    let bridge_tx_2 = bridge.status_tx();

    let (route_tx, action_rx_1) = bounded(1);
    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
    bridge.register_action_route(test_route, route_tx).unwrap();

    let (route_tx, action_rx_2) = bounded(1);
    let redirect_route = ActionRoute {
        name: "redirect".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
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
    let bridge_tx_1 = bridge.status_tx();
    let bridge_tx_2 = bridge.status_tx();

    let (route_tx, action_rx_1) = bounded(1);
    let tunshell_route = ActionRoute {
        name: "launch_shell".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
    bridge.register_action_route(tunshell_route, route_tx).unwrap();

    let (route_tx, action_rx_2) = bounded(1);
    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
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
        name: "launch_shell".to_string(),
        payload: "test".to_string(),
    };
    actions_tx.send(action).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "2".to_string(),
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
    let bridge_tx_1 = bridge.status_tx();
    let bridge_tx_2 = bridge.status_tx();

    let (route_tx, action_rx_1) = bounded(1);
    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
    bridge.register_action_route(test_route, route_tx).unwrap();

    let (route_tx, action_rx_2) = bounded(1);
    let tunshell_route = ActionRoute {
        name: "launch_shell".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
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
        name: "test".to_string(),
        payload: "test".to_string(),
    };
    actions_tx.send(action).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "2".to_string(),
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

#[tokio::test]
async fn cancel_action() {
    let tmpdir = tempdir::TempDir::new("bridge").unwrap();
    std::env::set_current_dir(&tmpdir).unwrap();
    let config = default_config();
    let (mut bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));

    let bridge_tx_1 = bridge.status_tx();
    let (route_tx, action_rx_1) = bounded(1);
    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: true,
    };
    bridge.register_action_route(test_route, route_tx).unwrap();

    spawn_bridge(bridge);

    std::thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let action = action_rx_1.recv().unwrap();
        assert_eq!(action.action_id, "1");
        let response = ActionResponse::progress(&action.action_id, "Running", 0);
        rt.block_on(bridge_tx_1.send_action_response(response));
        let cancel_action = action_rx_1.recv().unwrap();
        assert_eq!(cancel_action.action_id, "2");
        assert_eq!(cancel_action.name, "cancel_action");
        let response = ActionResponse::failure(&action.action_id, "Cancelled");
        rt.block_on(bridge_tx_1.send_action_response(response));
    });

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "1".to_string(),
        name: "test".to_string(),
        payload: "test".to_string(),
    };
    actions_tx.send(action).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "2".to_string(),
        name: "cancel_action".to_string(),
        payload: r#"{"action_id": "1", "name": "test"}"#.to_string(),
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

    let status = responses.next();
    assert_eq!(status.action_id, "1");
    assert!(status.is_failed());
    assert_eq!(status.errors, ["Cancelled"]);

    let status = responses.next();
    assert_eq!(status.action_id, "2");
    assert!(status.is_completed());
}

#[tokio::test]
async fn cancel_action_failure_not_executing() {
    let tmpdir = tempdir::TempDir::new("bridge").unwrap();
    std::env::set_current_dir(&tmpdir).unwrap();
    let config = default_config();
    let (bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));

    spawn_bridge(bridge);

    let action = Action {
        action_id: "2".to_string(),
        name: "cancel_action".to_string(),
        payload: r#"{"action_id": "1", "name": "test"}"#.to_string(),
    };
    actions_tx.send(action).unwrap();

    let mut responses = Responses { rx: data_rx, responses: vec![] };

    let status = responses.next();
    assert_eq!(status.action_id, "2");
    assert!(status.is_failed());
    assert_eq!(
        status.errors,
        ["Cancellation request received for action currently not in execution!"]
    );
}

#[tokio::test]
async fn cancel_action_failure_on_completion() {
    let tmpdir = tempdir::TempDir::new("bridge").unwrap();
    std::env::set_current_dir(&tmpdir).unwrap();
    let config = default_config();
    let (mut bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));

    let bridge_tx_1 = bridge.status_tx();
    let (route_tx, action_rx_1) = bounded(1);
    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
    bridge.register_action_route(test_route, route_tx).unwrap();

    spawn_bridge(bridge);

    std::thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let action = action_rx_1.recv().unwrap();
        assert_eq!(action.action_id, "1");
        let response = ActionResponse::progress(&action.action_id, "Running", 0);
        rt.block_on(bridge_tx_1.send_action_response(response));

        std::thread::sleep(Duration::from_secs(2));
        let response = ActionResponse::success(&action.action_id);
        rt.block_on(bridge_tx_1.send_action_response(response));
    });

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "1".to_string(),
        name: "test".to_string(),
        payload: "test".to_string(),
    };
    actions_tx.send(action).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "2".to_string(),
        name: "cancel_action".to_string(),
        payload: r#"{"action_id": "1", "name": "test"}"#.to_string(),
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

    let status = responses.next();
    assert_eq!(status.action_id, "1");
    assert!(status.is_completed());

    let status = responses.next();
    assert_eq!(status.action_id, "2");
    assert!(status.is_failed());
    assert_eq!(status.errors, ["Cancellation request failed as action completed execution!"]);
}

#[tokio::test]
async fn cancel_action_between_redirect() {
    let tmpdir = tempdir::TempDir::new("bridge").unwrap();
    std::env::set_current_dir(&tmpdir).unwrap();
    let mut config = default_config();
    config.action_redirections.insert("test".to_string(), "redirect".to_string());
    let (mut bridge, actions_tx, data_rx) = create_bridge(Arc::new(config));

    let bridge_tx_1 = bridge.status_tx();
    let (route_tx, action_rx_1) = bounded(1);
    let test_route = ActionRoute {
        name: "test".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
    bridge.register_action_route(test_route, route_tx).unwrap();

    let bridge_tx_2 = bridge.status_tx();
    let (route_tx, action_rx_2) = bounded(1);
    let test_route = ActionRoute {
        name: "redirect".to_string(),
        timeout: Duration::from_secs(30),
        cancellable: false,
    };
    bridge.register_action_route(test_route, route_tx).unwrap();

    spawn_bridge(bridge);

    std::thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let action = action_rx_2.recv().unwrap();
        assert_eq!(action.action_id, "1");
        let response = ActionResponse::progress(&action.action_id, "Running", 0);
        rt.block_on(bridge_tx_1.send_action_response(response));
        std::thread::sleep(Duration::from_secs(3));
        let response = ActionResponse::progress(&action.action_id, "Finished", 100);
        rt.block_on(bridge_tx_1.send_action_response(response));
    });

    std::thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let action = action_rx_1.recv().unwrap();
        assert_eq!(action.action_id, "1");
        let response = ActionResponse::progress(&action.action_id, "Running", 0);
        rt.block_on(bridge_tx_2.send_action_response(response));
        std::thread::sleep(Duration::from_secs(3));
        let response = ActionResponse::progress(&action.action_id, "Finished", 100);
        rt.block_on(bridge_tx_2.send_action_response(response));
    });

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "1".to_string(),
        name: "test".to_string(),
        payload: "test".to_string(),
    };
    actions_tx.send(action).unwrap();

    std::thread::sleep(Duration::from_secs(1));

    let action = Action {
        action_id: "2".to_string(),
        name: "cancel_action".to_string(),
        payload: r#"{"action_id": "1", "name": "test"}"#.to_string(),
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

    let status = responses.next();
    assert_eq!(status.action_id, "1");
    assert!(status.is_done());

    let status = responses.next();
    assert_eq!(status.action_id, "2");
    assert!(status.is_completed());

    let status = responses.next();
    assert_eq!(status.action_id, "1");
    assert!(status.is_failed());
    assert_eq!(status.errors, ["Action cancelled by action_id: 2"]);
}
