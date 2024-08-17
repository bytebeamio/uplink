use std::{
    sync::atomic::{AtomicU16, Ordering},
    thread::{sleep, spawn},
    time::Duration,
};

use flume::{bounded, Receiver, Sender};
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use serde_json::json;

use uplink::{
    base::bridge::{BridgeTx, DataTx, StatusTx},
    collector::bus::Bus,
    config::{BusConfig, JoinerConfig},
    Action, ActionResponse,
};

const OFFEST: AtomicU16 = AtomicU16::new(0);

fn setup() -> (u16, Sender<Action>, Receiver<ActionResponse>) {
    let offset = OFFEST.fetch_add(1, Ordering::Relaxed);
    let (port, console_port) = (1883 + offset, 3030 + offset);
    let config = BusConfig { port, console_port, joins: JoinerConfig { output_streams: vec![] } };

    let (data_tx, _data_rx) = bounded(1);
    let (status_tx, status_rx) = bounded(1);
    let bridge_tx =
        BridgeTx { data_tx: DataTx { inner: data_tx }, status_tx: StatusTx { inner: status_tx } };
    let (actions_tx, actions_rx) = bounded(1);
    spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

    (port, actions_tx, status_rx)
}

/// This test verifies that action status messages published to the bus are correctly received.
#[test]
fn recv_action_and_respond() {
    let (port, actions_tx, status_rx) = setup();

    let opt = MqttOptions::new("test", "localhost", port);
    let (client, mut conn) = Client::new(opt, 1);

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    client.subscribe("/actions/abc", QoS::AtMostOnce).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };
    let Event::Incoming(_) = conn.recv().unwrap().unwrap() else { panic!() };
    sleep(Duration::from_millis(100));

    let action =
        Action { action_id: "123".to_owned(), name: "abc".to_owned(), payload: "".to_owned() };
    let expected_action = action.clone();
    actions_tx.send(action).unwrap();

    let Event::Incoming(Packet::Publish(publish)) =
        conn.recv_timeout(Duration::from_millis(500)).unwrap().unwrap()
    else {
        panic!()
    };
    let action = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(expected_action, action);

    let action_status = ActionResponse {
        action_id: "123".to_owned(),
        sequence: 1,
        timestamp: 0,
        state: "abc".to_owned(),
        progress: 234,
        errors: vec!["Testing".to_owned()],
        done_response: None,
    };
    client
        .publish("/actions/123/status", QoS::AtMostOnce, false, json!(action_status).to_string())
        .unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    assert_eq!(action_status, status_rx.recv_timeout(Duration::from_millis(200)).unwrap());
}

/// This test verifies that action status is set to failed for actions which are not subscribed to on the bus
#[test]
fn mark_unregistered_action_as_failed() {
    let (_, actions_tx, status_rx) = setup();

    let action =
        Action { action_id: "123".to_owned(), name: "abc".to_owned(), payload: "".to_owned() };
    actions_tx.send(action).unwrap();

    let ActionResponse { action_id, state, errors, .. } =
        status_rx.recv_timeout(Duration::from_millis(200)).unwrap();
    assert_eq!(action_id, "123");
    assert_eq!(state, "Failed");
    assert_eq!(errors, ["Action was not expected"]);
}
