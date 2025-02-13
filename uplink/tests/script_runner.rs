use std::thread::spawn;

use flume::bounded;
use uplink::{
    base::bridge::{BridgeTx, DataTx},
    collector::script_runner::ScriptRunner,
    Action, ActionResponse,
};

#[test]
fn empty_payload() {
    let (tx, _) = bounded(2);
    let (inner, status_rx) = bounded(2);
    let bridge_tx = BridgeTx { data_tx: DataTx { inner: tx }, status_tx: inner };

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
    let (tx, _) = bounded(2);
    let (inner, status_rx) = bounded(2);
    let bridge_tx = BridgeTx { data_tx: DataTx { inner: tx }, status_tx: inner };

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
