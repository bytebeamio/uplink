use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use rumqttc::{Publish, QoS, Request};
use tokio::spawn;

use uplink::{
    base::{
        bridge::Payload,
        serializer::{
            tests::{default_config, defaults},
            write_to_storage,
        },
    },
    config::{Persistence, StreamConfig},
    mock::MockCollector,
    Storage,
};

#[tokio::test]
// Ensures that the data of streams are removed based on preference
async fn preferential_send_on_network() {
    let mut config = default_config();
    config.stream_metrics.timeout = Duration::from_secs(1000);
    config.persistence_path = PathBuf::from(".tmp.serializer_test");
    let persistence = Persistence { max_file_size: 1024 * 1024, max_file_count: 1 };
    config.streams.extend([
        (
            "one".to_owned(),
            StreamConfig {
                topic: "topic/one".to_string(),
                priority: 1,
                persistence,
                ..Default::default()
            },
        ),
        (
            "two".to_owned(),
            StreamConfig {
                topic: "topic/two".to_string(),
                priority: 2,
                persistence,
                ..Default::default()
            },
        ),
        (
            "top".to_owned(),
            StreamConfig {
                topic: "topic/top".to_string(),
                priority: u8::MAX,
                persistence,
                ..Default::default()
            },
        ),
    ]);

    let publish = |topic: String, i: u32| Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic,
        pkid: 0,
        payload: {
            let serialized = serde_json::to_vec(&vec![Payload {
                stream: Default::default(),
                sequence: i,
                timestamp: 0,
                payload: serde_json::from_str("{\"msg\": \"Hello, World!\"}").unwrap(),
            }])
            .unwrap();

            Bytes::from(serialized)
        },
    };
    let persistence_path = |path: &PathBuf, stream_name: &str| {
        let mut path = path.to_owned();
        path.push(stream_name);
        create_dir_all(&path).unwrap();

        path
    };

    // write packets for one, two and top onto disk
    let mut one = Storage::new("topic/one", 1024 * 1024);
    one.set_persistence(persistence_path(&config.persistence_path, "one"), 1).unwrap();
    write_to_storage(publish("topic/one".to_string(), 4), &mut one).unwrap();
    write_to_storage(publish("topic/one".to_string(), 5), &mut one).unwrap();
    one.flush().unwrap();

    let mut two = Storage::new("topic/two", 1024 * 1024);
    two.set_persistence(persistence_path(&config.persistence_path, "two"), 1).unwrap();
    write_to_storage(publish("topic/two".to_string(), 3), &mut two).unwrap();
    two.flush().unwrap();

    let mut top = Storage::new("topic/top", 1024 * 1024);
    top.set_persistence(persistence_path(&config.persistence_path, "top"), 1).unwrap();
    write_to_storage(publish("topic/top".to_string(), 1), &mut top).unwrap();
    write_to_storage(publish("topic/top".to_string(), 2), &mut top).unwrap();
    top.flush().unwrap();

    // start serializer in the background
    let config = Arc::new(config);
    let (serializer, data_tx, req_rx) = defaults(config);

    spawn(async { serializer.start().await.unwrap() });

    let mut default = MockCollector::new(
        "default",
        StreamConfig { topic: "topic/default".to_owned(), batch_size: 1, ..Default::default() },
        data_tx,
    );
    default.send(6).await.unwrap();
    default.send(7).await.unwrap();

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/top");
    assert_eq!(payload, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/top");
    assert_eq!(payload, "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/two");
    assert_eq!(payload, "[{\"sequence\":3,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/one");
    assert_eq!(payload, "[{\"sequence\":4,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/one");
    assert_eq!(payload, "[{\"sequence\":5,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":6,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":7,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    remove_dir_all(".tmp.serializer_test").unwrap();
}
