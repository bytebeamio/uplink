use std::{fs::create_dir_all, path::PathBuf, sync::Arc, thread, time::Duration};

use bytes::Bytes;
use flume::bounded;
use rumqttc::{Publish, QoS, Request};
use tempdir::TempDir;
use tokio::{runtime::Runtime, spawn, time::sleep};

use uplink::{
    base::{bridge::Payload, serializer::Serializer},
    config::{Config, Persistence, StreamConfig},
    mock::{MockClient, MockCollector},
    Storage,
};

fn publish(topic: String, i: u32) -> Publish {
    Publish {
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
    }
}

#[tokio::test]
// Ensures that the data of streams are removed based on preference
async fn preferential_send_on_network() {
    let temp_dir = TempDir::new("preferential_send").unwrap();
    let mut config = Config::default();
    config.default_buf_size = 1024 * 1024;
    config.mqtt.max_packet_size = 1024 * 1024;
    config.stream_metrics.timeout = Duration::from_secs(1000);
    config.persistence_path = PathBuf::from(temp_dir.path());
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

    let persistence_path = |path: &PathBuf, stream_name: &str| {
        let mut path = path.to_owned();
        path.push(stream_name);
        create_dir_all(&path).unwrap();

        path
    };

    // write packets for one, two and top onto disk
    let mut one = Storage::new("topic/one", 1024 * 1024, false);
    one.set_persistence(persistence_path(&config.persistence_path, "one"), 1).unwrap();
    one.write(publish("topic/one".to_string(), 4)).unwrap();
    one.write(publish("topic/one".to_string(), 5)).unwrap();
    one.flush().unwrap();

    let mut two = Storage::new("topic/two", 1024 * 1024, false);
    two.set_persistence(persistence_path(&config.persistence_path, "two"), 1).unwrap();
    two.write(publish("topic/two".to_string(), 3)).unwrap();
    two.flush().unwrap();

    let mut top = Storage::new("topic/top", 1024 * 1024, false);
    top.set_persistence(persistence_path(&config.persistence_path, "top"), 1).unwrap();
    top.write(publish("topic/top".to_string(), 1)).unwrap();
    top.write(publish("topic/top".to_string(), 2)).unwrap();
    top.flush().unwrap();

    let config = Arc::new(config);
    let (data_tx, data_rx) = bounded(1);
    let (net_tx, req_rx) = bounded(1);
    let (metrics_tx, _metrics_rx) = bounded(1);
    let client = MockClient { net_tx };
    let serializer = Serializer::new(config, data_rx, client, metrics_tx).unwrap();

    // start serializer in the background
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
}

#[tokio::test]
// Ensures that data pushed maintains FIFO order
async fn fifo_data_push() {
    let mut config = Config::default();
    config.default_buf_size = 1024 * 1024;
    config.mqtt.max_packet_size = 1024 * 1024;
    let config = Arc::new(config);
    let (data_tx, data_rx) = bounded(1);
    let (net_tx, req_rx) = bounded(1);
    let (metrics_tx, _metrics_rx) = bounded(1);
    let client = MockClient { net_tx };
    let serializer = Serializer::new(config, data_rx, client, metrics_tx).unwrap();

    // start serializer in the background
    thread::spawn(|| _ = Runtime::new().unwrap().block_on(serializer.start()));

    spawn(async {
        let mut default = MockCollector::new(
            "default",
            StreamConfig { topic: "topic/default".to_owned(), batch_size: 1, ..Default::default() },
            data_tx,
        );
        for i in 0.. {
            default.send(i).await.unwrap();
        }
    });

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":0,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
}

#[tokio::test]
// Ensures that live data is pushed first if configured to do so
async fn prefer_live_data() {
    let mut config = Config::default();
    config.default_buf_size = 1024 * 1024;
    config.mqtt.max_packet_size = 1024 * 1024;
    config.default_live_data_first = true;
    let config = Arc::new(config);
    let (data_tx, data_rx) = bounded(0);
    let (net_tx, req_rx) = bounded(0);
    let (metrics_tx, _metrics_rx) = bounded(1);
    let client = MockClient { net_tx };
    let serializer = Serializer::new(config, data_rx, client, metrics_tx).unwrap();

    // start serializer in the background
    thread::spawn(|| _ = Runtime::new().unwrap().block_on(serializer.start()));

    spawn(async {
        let mut default = MockCollector::new(
            "default",
            StreamConfig { topic: "topic/default".to_owned(), batch_size: 1, ..Default::default() },
            data_tx,
        );
        for i in 0.. {
            default.send(i).await.unwrap();
            sleep(Duration::from_millis(250)).await;
        }
    });

    sleep(Duration::from_millis(750)).await;
    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":0,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":2,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");

    let Request::Publish(Publish { topic, payload, .. }) = req_rx.recv_async().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "topic/default");
    assert_eq!(payload, "[{\"sequence\":1,\"timestamp\":0,\"msg\":\"Hello, World!\"}]");
}
