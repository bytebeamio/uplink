use std::thread::spawn;

use flume::{bounded, Receiver};
use uplink::{
    base::bridge::{BridgeTx, DataTx, StatusTx},
    collector::script_runner::ScriptRunner,
    Action, ActionResponse,
};

fn create_bridge() -> (BridgeTx, Receiver<ActionResponse>) {
    let (inner, _) = bounded(2);
    let data_tx = DataTx { inner };
    let (inner, status_rx) = bounded(2);
    let status_tx = StatusTx { inner };

    (BridgeTx { data_tx, status_tx }, status_rx)
}

#[test]
fn empty_payload() {
    let (bridge_tx, status_rx) = create_bridge();

    let (actions_tx, actions_rx) = bounded(1);
    let script_runner = ScriptRunner::new(actions_rx, bridge_tx);
    spawn(move || script_runner.start().unwrap());

    actions_tx
        .send(Action {
            action_id: "1".to_string(),
            name: "test".to_string(),
            payload: "".to_string(),
        })
        .unwrap();

    let ActionResponse { state, errors, .. } = status_rx.recv().unwrap();
    assert_eq!(state, "Failed");
    assert_eq!(errors, ["Failed to deserialize action payload: EOF while parsing a value at line 1 column 0; payload: \"\""]);
}

#[test]
fn missing_path() {
    let (bridge_tx, status_rx) = create_bridge();

    let (actions_tx, actions_rx) = bounded(1);
    let script_runner = ScriptRunner::new(actions_rx, bridge_tx);

    spawn(move || script_runner.start().unwrap());

    actions_tx
        .send(Action {
            action_id: "1".to_string(),
            name: "test".to_string(),
            payload: "{\"url\": \"...\", \"content_length\": 0,\"file_name\": \"...\"}".to_string(),
        })
        .unwrap();

    let ActionResponse { state, errors, .. } = status_rx.recv().unwrap();
    assert_eq!(state, "Failed");
    assert_eq!(errors, ["Action payload doesn't contain path for script execution; payload: \"{\\\"url\\\": \\\"...\\\", \\\"content_length\\\": 0,\\\"file_name\\\": \\\"...\\\"}\""]);
}
