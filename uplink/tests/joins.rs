//! Each test follows a similar structure:
//! - Setup Configuration: Define the bus configuration, including join configurations, push intervals, and other parameters.
//! - Initialize Channels: Create bounded channels for data and status transmission.
//! - Start the Bus: Spawn a new thread to start the bus with the given configuration and channels.
//! - Setup MQTT Client: Configure and connect the MQTT client to the bus.
//! - Publish Messages: Publish JSON messages to the defined input streams.
//! - Receive and Verify: Receive the output messages and verify that they match the expected results.

use std::{
    sync::atomic::{AtomicU16, Ordering},
    thread::{sleep, spawn},
    time::Duration,
};

use flume::{bounded, Receiver, Sender};
use rumqttc::{Client, Connection, Event, MqttOptions, Packet, Publish, QoS};
use serde::Deserialize;

use serde_json::{json, Value};
use uplink::{
    base::bridge::{BridgeTx, DataTx, Payload, StatusTx},
    collector::bus::Bus,
    config::{
        BusConfig, Field, InputConfig, JoinConfig, JoinerConfig, NoDataAction, PushInterval,
        SelectConfig,
    },
    Action, ActionResponse,
};

const OFFEST: AtomicU16 = AtomicU16::new(0);

fn setup(
    joins: JoinerConfig,
) -> (Receiver<Payload>, Receiver<ActionResponse>, Sender<Action>, Client, Connection) {
    let offset = OFFEST.fetch_add(1, Ordering::Relaxed);
    let (port, console_port) = (1883 + offset, 3030 + offset);

    let config = BusConfig { console_port, port, joins };

    let (data_tx, data_rx) = bounded(1);
    let (status_tx, status_rx) = bounded(1);
    let bridge_tx =
        BridgeTx { data_tx: DataTx { inner: data_tx }, status_tx: StatusTx { inner: status_tx } };
    let (actions_tx, actions_rx) = bounded(1);
    spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

    let opt = MqttOptions::new("test", "localhost", port);
    let (client, conn) = Client::new(opt, 1);

    (data_rx, status_rx, actions_tx, client, conn)
}

/// This test checks if data published to the input stream is received as-is on the output stream.
#[test]
fn as_is_data_from_bus() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "as_is".to_owned(),
            construct_from: vec![InputConfig {
                input_stream: "input".to_owned(),
                select_fields: SelectConfig::All,
            }],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnNewData,
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input", QoS::AtMostOnce, false, input.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(100)).unwrap()
    else {
        panic!()
    };
    assert_eq!(stream, "as_is");
    assert_eq!(payload, input);
}

/// This test ensures that data from two different input streams is joined correctly and published to the output stream when new data is received.
#[test]
fn join_two_streams_on_new_data_from_bus() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::All,
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::All,
                },
            ],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnNewData,
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_two = json!({"field_x": 456, "field_y": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(200)).unwrap()
    else {
        panic!()
    };
    assert_eq!(stream, "output");
    assert_eq!(payload, input_one);

    let Payload { stream, sequence: 2, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(100)).unwrap()
    else {
        panic!()
    };
    assert_eq!(stream, "output");
    assert_eq!(payload, input_two);
}

/// This test checks the joining of data from two streams based on a timeout interval, ensuring the correct output even if data is received after the timeout.
#[test]
fn join_two_streams_on_timeout_from_bus() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::All,
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::All,
                },
            ],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let input_two = json!({"field_x": 456, "field_y": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(1000)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123, "field_2": "abc", "field_x": 456, "field_y": "xyz"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test validates that only selected fields from an input stream are published to the output stream.
#[test]
fn select_from_stream_on_bus() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![InputConfig {
                input_stream: "input".to_owned(),
                select_fields: SelectConfig::Fields(vec![Field {
                    original: "field_1".to_owned(),
                    renamed: None,
                }]),
            }],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnNewData,
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input", QoS::AtMostOnce, false, input.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(500)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test checks that selected fields from two different streams are combined and published correctly to the output stream.
#[test]
fn select_from_two_streams_on_bus() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::Fields(vec![Field {
                        original: "field_1".to_owned(),
                        renamed: None,
                    }]),
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::Fields(vec![Field {
                        original: "field_x".to_owned(),
                        renamed: None,
                    }]),
                },
            ],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let input_two = json!({"field_x": 456, "field_y": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(1000)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123, "field_x": 456});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test verifies that the system correctly handles flushing of streams, ensuring that when no new data arrives, keys are droppped/set to null.
#[test]
fn null_after_flush() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::All,
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::All,
                },
            ],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123, "field_2": "abc"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let input_two = json!({"field_3": 456, "field_4": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 2, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_3": 456, "field_4": "xyz"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let input_one = json!({"field_1": 789, "field_2": "efg"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let input_two = json!({"field_3": 098, "field_4": "zyx"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 3, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 789, "field_2": "efg","field_3": 098, "field_4": "zyx"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test checks that the system correctly handles data when configured with PreviousValue, ensuring that the last known values are used when no new data arrives.
#[test]
fn previous_value_after_flush() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::All,
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::All,
                },
            ],
            no_data_action: NoDataAction::PreviousValue,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123, "field_2": "abc"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let input_two = json!({"field_3": 456, "field_4": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 2, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123, "field_2": "abc", "field_3": 456, "field_4": "xyz"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let input_one = json!({"field_1": 789, "field_2": "efg"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let input_two = json!({"field_3": 098, "field_4": "zyx"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 3, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 789, "field_2": "efg", "field_3": 098, "field_4": "zyx"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test checks how the system handles two input streams that have similar fields without renaming them.
/// The expected behavior is to merge the fields, with the latest value for duplicated fields being used.
#[test]
fn two_streams_with_similar_fields_no_rename() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::Fields(vec![
                        Field { original: "field_a".to_owned(), renamed: None },
                        Field { original: "field_b".to_owned(), renamed: None },
                    ]),
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::Fields(vec![
                        Field { original: "field_a".to_owned(), renamed: None },
                        Field { original: "field_c".to_owned(), renamed: None },
                    ]),
                },
            ],
            no_data_action: NoDataAction::PreviousValue,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_a": 123, "field_b": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_a": 123, "field_b": "abc"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let input_two = json!({"field_a": 456, "field_c": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 2, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_a": 456, "field_b": "abc", "field_c": "xyz"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test checks how the system handles two input streams that have similar fields but with renaming to avoid field conflicts.
/// The expected behavior is to have the fields renamed as specified and merged into the output.
#[test]
fn two_streams_with_similar_fields_renamed() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::Fields(vec![
                        Field {
                            original: "field_a".to_owned(),
                            renamed: Some("field_a1".to_owned()),
                        },
                        Field { original: "field_b".to_owned(), renamed: None },
                    ]),
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::Fields(vec![
                        Field {
                            original: "field_a".to_owned(),
                            renamed: Some("field_a2".to_owned()),
                        },
                        Field { original: "field_c".to_owned(), renamed: None },
                    ]),
                },
            ],
            no_data_action: NoDataAction::PreviousValue,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: false,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

    let input_one = json!({"field_a": 123, "field_b": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_a1": 123, "field_b": "abc"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let input_two = json!({"field_a": 456, "field_c": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let Payload { stream, sequence: 2, payload, .. } =
        data_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_a1": 123, "field_a2": 456, "field_b": "abc", "field_c": "xyz"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);
}

/// This test is to validate the behavior of the bus when it is configured to push a joined stream back onto the bus.
/// In this test the client subscribes to the output stream on the bus, publishes data onto input streams and then expects the joined data back from the bus.
#[test]
fn publish_joined_stream_back_on_bus() {
    let (data_rx, _, _, client, mut conn) = setup(JoinerConfig {
        output_streams: vec![JoinConfig {
            name: "output".to_owned(),
            construct_from: vec![
                InputConfig {
                    input_stream: "input_one".to_owned(),
                    select_fields: SelectConfig::All,
                },
                InputConfig {
                    input_stream: "input_two".to_owned(),
                    select_fields: SelectConfig::All,
                },
            ],
            no_data_action: NoDataAction::Null,
            push_interval: PushInterval::OnTimeout(Duration::from_secs(1)),
            publish_on_service_bus: true,
        }],
    });

    sleep(Duration::from_millis(100));
    let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };
    client.subscribe("/streams/output", QoS::AtMostOnce).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };
    let Event::Incoming(Packet::SubAck(_)) =
        conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
    else {
        panic!()
    };

    let input_one = json!({"field_1": 123, "field_2": "abc"});
    client.publish("/streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap() else {
        panic!()
    };

    let input_two = json!({"field_x": 456, "field_y": "xyz"});
    client.publish("/streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
    let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

    let Payload { stream, sequence: 1, payload, .. } =
        data_rx.recv_timeout(Duration::from_millis(1000)).unwrap()
    else {
        panic!()
    };
    let output = json!({"field_1": 123, "field_2": "abc", "field_x": 456, "field_y": "xyz"});
    assert_eq!(stream, "output");
    assert_eq!(payload, output);

    let Event::Incoming(Packet::Publish(Publish { topic, payload, .. })) =
        conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
    else {
        panic!()
    };
    {
        #[derive(Deserialize)]
        struct Payload {
            sequence: u32,
            #[allow(dead_code)]
            timestamp: u64,
            #[serde(flatten)]
            payload: Value,
        }
        let Payload { sequence, payload, .. } = serde_json::from_slice(&payload).unwrap();
        assert_eq!(topic, "/streams/output");
        assert_eq!(sequence, 1);
        assert_eq!(payload, output);
    }
}
