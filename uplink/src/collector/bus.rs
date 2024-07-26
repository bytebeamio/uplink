use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use flume::{bounded, Receiver, Sender};
use log::{error, warn};
use rumqttd::{
    local::{LinkRx, LinkTx},
    protocol::Publish,
    Broker, Config, ConnectionSettings, Forward, Notification, RouterConfig, ServerSettings,
};
use serde_json::{json, Map, Value};
use tokio::{select, task::JoinSet, time::interval};

use crate::{
    base::{
        bridge::{BridgeTx, Payload},
        clock, ServiceBusRx, ServiceBusTx,
    },
    config::{BusConfig, Field, JoinConfig, NoDataAction, PushInterval, SelectConfig},
    spawn_named_thread, Action,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Link error: {0}")]
    Link(#[from] rumqttd::local::LinkError),
    #[error("Parse error: {0}")]
    Parse(#[from] std::net::AddrParseError),
    #[error("Rumqttd error: {0}")]
    Rumqttd(#[from] rumqttd::Error),
    #[error("Recv error: {0}")]
    Recv(#[from] flume::RecvError),
}

pub struct BusRx {
    rx: LinkRx,
}

impl ServiceBusRx<Publish> for BusRx {
    fn recv(&mut self) -> Option<Publish> {
        loop {
            return match self.rx.recv() {
                Ok(Some(Notification::Forward(Forward { publish, .. }))) => Some(publish),
                Err(_) => None,
                _ => continue,
            };
        }
    }

    async fn recv_async(&mut self) -> Option<Publish> {
        loop {
            return match self.rx.recv_async().await {
                Ok(Some(Notification::Forward(Forward { publish, .. }))) => Some(publish),
                Err(_) => None,
                _ => continue,
            };
        }
    }
}

pub struct BusTx {
    tx: LinkTx,
}

impl ServiceBusTx for BusTx {
    type Error = Error;

    fn publish_data(&mut self, data: Payload) -> Result<(), Self::Error> {
        let topic = format!("streams/{}", data.stream);
        let payload = serde_json::to_vec(&data)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }

    fn update_action_status(&mut self, status: crate::ActionResponse) -> Result<(), Self::Error> {
        let topic = "streams/action_status".to_owned();
        let payload = serde_json::to_vec(&status)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }

    fn subscribe_to_streams(
        &mut self,
        streams: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), Self::Error> {
        for stream in streams {
            let filter = format!("streams/{}", stream.into());
            self.tx.subscribe(filter)?;
        }
        self.tx.subscribe("streams/action_status")?;

        Ok(())
    }

    fn register_action(&mut self, name: impl Into<String>) -> Result<(), Self::Error> {
        let filter = format!("actions/{}", name.into());
        self.tx.subscribe(filter)?;

        Ok(())
    }

    fn deregister_action(&mut self, name: impl Into<String>) -> Result<(), Self::Error> {
        let filter = format!("actions/{}", name.into());
        self.tx.unsubscribe(filter)?;

        Ok(())
    }

    fn push_action(&mut self, action: Action) -> Result<(), Self::Error> {
        let topic = format!("streams/{}", action.name);
        let payload = serde_json::to_vec(&action)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }
}

pub struct Bus {
    rx: BusRx,
    tx: BusTx,
    bridge_tx: BridgeTx,
    actions_rx: Receiver<Action>,
    config: BusConfig,
}

impl Bus {
    pub fn new(config: BusConfig, bridge_tx: BridgeTx, actions_rx: Receiver<Action>) -> Self {
        let router = RouterConfig {
            max_segment_size: 1024,
            max_connections: 10,
            max_segment_count: 10,
            max_outgoing_packet_count: 1024,
            ..Default::default()
        };
        let connections = ConnectionSettings {
            connection_timeout_ms: 10000,
            max_payload_size: 1073741824,
            max_inflight_count: 10,
            auth: None,
            external_auth: None,
            dynamic_filters: false,
        };
        let server = ServerSettings {
            name: "service_bus".to_owned(),
            listen: format!("127.0.0.1:{}", config.port).parse::<SocketAddr>().unwrap(),
            tls: None,
            next_connection_delay_ms: 0,
            connections,
        };
        let servers = [("service_bus".to_owned(), server)].into_iter().collect();
        let mut broker =
            Broker::new(Config { id: 0, router, v4: Some(servers), ..Default::default() });
        let (tx, rx) = broker.link("uplink").unwrap();
        spawn_named_thread("Broker", move || {
            if let Err(e) = broker.start() {
                error!("{e}")
            }
        });

        Self { tx: BusTx { tx }, rx: BusRx { rx }, bridge_tx, actions_rx, config }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        let (back_tx, back_rx) = bounded(0);
        let mut router =
            Router::new(self.config.joins.output_streams.clone(), self.bridge_tx.clone(), back_tx)
                .await;
        let mut input_streams = HashSet::new();
        for join in &self.config.joins.output_streams {
            for input in &join.construct_from {
                input_streams.insert(input.input_stream.to_owned());
            }
        }
        if let Err(e) = self.tx.subscribe_to_streams(input_streams) {
            error!("{e}");
            return;
        }

        loop {
            select! {
                Ok(action) = self.actions_rx.recv_async() => {
                    if let Err(e) = self.tx.push_action(action) {
                        error!("{e}")
                    }
                }

                Some(publish) = self.rx.recv_async() => {
                    if publish.topic == "streams/action_status" {
                        let Ok(status) = serde_json::from_slice(&publish.payload) else {
                            error!("Couldn't parse payload as action status");
                            continue;
                        };
                        self.bridge_tx.send_action_response_sync(status);
                        continue;
                    }

                    let Ok(data) = serde_json::from_slice::<Map<String, Value>>(&publish.payload) else {
                        error!("Couldn't parse payload as data payload");
                        continue;
                    };
                    let topic = String::from_utf8(publish.topic.to_vec()).unwrap();
                    // Expected topic structure: `streams/{stream_name}`
                    let Some (stream_name) = topic.split('/').last() else {
                        error!("unexpected topic structure: {topic}");
                        continue
                    };
                    router.map(stream_name.to_owned(), data).await;
                }

                Ok(data) = back_rx.recv_async() => {
                    if let Err(e) = self.tx.publish_data(data) {
                        error!("{e}");
                    }
                }

                _ = router.tasks.join_next() => {}
            }
        }
    }
}

type Json = Map<String, Value>;

struct Router {
    map: HashMap<String, Vec<Sender<(String, Json)>>>,
    tasks: JoinSet<()>,
}

impl Router {
    async fn new(configs: Vec<JoinConfig>, bridge_tx: BridgeTx, back_tx: Sender<Payload>) -> Self {
        let mut map: HashMap<String, Vec<Sender<(String, Json)>>> = HashMap::new();
        let mut tasks = JoinSet::new();
        for config in configs {
            let (tx, rx) = bounded(1);
            let mut fields = HashMap::new();
            for stream in &config.construct_from {
                if let SelectConfig::Fields(selected_fields) = &stream.select_fields {
                    let renames: &mut HashMap<String, Field> =
                        fields.entry(stream.input_stream.to_owned()).or_default();
                    for field in selected_fields {
                        renames.insert(field.original.to_owned(), field.to_owned());
                    }
                }
                if let Some(senders) = map.get_mut(&stream.input_stream) {
                    senders.push(tx.clone());
                    continue;
                }
                map.insert(stream.input_stream.to_owned(), vec![tx.clone()]);
            }
            let joiner = Joiner {
                rx,
                joined: Map::new(),
                config,
                tx: bridge_tx.clone(),
                fields,
                back_tx: back_tx.clone(),
                sequence: 0,
            };
            tasks.spawn(joiner.start());
        }

        Router { map, tasks }
    }
    async fn map(&mut self, input_stream: String, json: Json) {
        let Some(iter) = self.map.get(&input_stream) else { return };
        for tx in iter {
            _ = tx.send_async((input_stream.clone(), json.clone())).await;
        }
    }
}

struct Joiner {
    rx: Receiver<(String, Json)>,
    joined: Json,
    config: JoinConfig,
    fields: HashMap<String, HashMap<String, Field>>,
    tx: BridgeTx,
    back_tx: Sender<Payload>,
    sequence: u32,
}

impl Joiner {
    async fn start(mut self) {
        let PushInterval::OnTimeout(period) = self.config.push_interval else {
            loop {
                match self.rx.recv_async().await {
                    Ok((stream_name, json)) => self.update(stream_name, json),
                    Err(e) => {
                        error!("{e}");
                        return;
                    }
                }
                self.send_data().await;
            }
        };
        let mut ticker = interval(period);
        loop {
            select! {
                r = self.rx.recv_async() => {
                    match r {
                        Ok((stream_name, json)) => self.update(stream_name, json),
                        Err(e) => {
                            error!("{e}");
                            return;
                        }
                    }
                }

                _ = ticker.tick() => {
                    self.send_data().await
                }
            }
        }
    }

    // Don't insert timestamp values if data is not to be pushed instantly, never insert sequence
    fn is_insertable(&self, key: &str) -> bool {
        match key {
            "timestamp" => self.config.push_interval == PushInterval::OnNewData,
            key => key != "sequence",
        }
    }

    fn update(&mut self, stream_name: String, json: Json) {
        if let Some(map) = self.fields.get(&stream_name) {
            for (mut key, value) in json {
                // drop unenumerated keys from json
                let Some(field) = map.get(&key) else { continue };
                if let Some(name) = &field.renamed {
                    name.clone_into(&mut key);
                }

                if self.is_insertable(&key) {
                    self.joined.insert(key, value);
                }
            }
        } else {
            // Select All if no mapping exists
            for (key, value) in json {
                if self.is_insertable(&key) {
                    self.joined.insert(key, value);
                }
            }
        }
    }

    async fn send_data(&mut self) {
        if self.joined.is_empty() {
            return;
        }
        self.sequence += 1;

        #[inline]
        fn parse_as_u64(value: Value) -> Option<u64> {
            let parsed = value.as_i64().map(|t| t as u64);
            if parsed.is_none() {
                warn!("timestamp: {value:?} has unexpected type; defaulting to system time")
            }
            parsed
        }

        // timestamp value should pass as is for instant push, else be the system time
        let timestamp = match self.joined.remove("timestamp").and_then(parse_as_u64) {
            Some(t) => t,
            _ => clock() as u64,
        };
        let payload = Payload {
            stream: self.config.name.clone(),
            sequence: self.sequence,
            timestamp,
            payload: json!(self.joined),
        };
        if self.config.publish_on_service_bus {
            _ = self.back_tx.send_async(payload.clone()).await;
        }
        self.tx.send_payload(payload).await;
        if self.config.no_data_action == NoDataAction::Null {
            self.joined.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    //! Each test follows a similar structure:
    //! - Setup Configuration: Define the bus configuration, including join configurations, push intervals, and other parameters.
    //! - Initialize Channels: Create bounded channels for data and status transmission.
    //! - Start the Bus: Spawn a new thread to start the bus with the given configuration and channels.
    //! - Setup MQTT Client: Configure and connect the MQTT client to the bus.
    //! - Publish Messages: Publish JSON messages to the defined input streams.
    //! - Receive and Verify: Receive the output messages and verify that they match the expected results.

    use std::{
        thread::{sleep, spawn},
        time::Duration,
    };

    use rumqttc::{Client, Event, MqttOptions, Packet, QoS};

    use crate::{
        base::bridge::{DataTx, StatusTx},
        config::{InputConfig, JoinerConfig},
        ActionResponse,
    };

    use super::*;

    /// This test checks if data published to the input stream is received as-is on the output stream.
    #[test]
    fn as_is_data_from_bus() {
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1884, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1884);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input", QoS::AtMostOnce, false, input.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

        let Payload { stream, sequence: 1, payload, .. } =
            data_rx.recv_timeout(Duration::from_millis(100)).unwrap()
        else {
            panic!()
        };
        assert_eq!(stream, "as_is");
        assert_eq!(payload, input);
    }

    /// This test verifies that action status messages published to the bus are correctly received.
    #[test]
    fn as_is_status_from_bus() {
        let config = BusConfig { port: 1885, joins: JoinerConfig { output_streams: vec![] } };

        let (data_tx, _data_rx) = bounded(1);
        let (status_tx, status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1885);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

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
            .publish(
                "streams/action_status",
                QoS::AtMostOnce,
                false,
                json!(action_status).to_string(),
            )
            .unwrap();
        let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

        assert_eq!(action_status, status_rx.recv_timeout(Duration::from_millis(100)).unwrap());
    }

    /// This test ensures that data from two different input streams is joined correctly and published to the output stream when new data is received.
    #[test]
    fn join_two_streams_on_new_data_from_bus() {
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1886, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1886);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input_one = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

        let input_two = json!({"field_x": 456, "field_y": "xyz"});
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
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
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1887, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1887);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input_one = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
            panic!()
        };

        let input_two = json!({"field_x": 456, "field_y": "xyz"});
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
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
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1888, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1888);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input", QoS::AtMostOnce, false, input.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1889, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1889);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input_one = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
            panic!()
        };

        let input_two = json!({"field_x": 456, "field_y": "xyz"});
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
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
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1890, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1890);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input_one = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
            panic!()
        };

        let input_two = json!({"field_3": 098, "field_4": "zyx"});
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
        let joins = JoinerConfig {
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
        };
        let config = BusConfig { port: 1891, joins };

        let (data_tx, data_rx) = bounded(1);
        let (status_tx, _status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (_actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

        let opt = MqttOptions::new("test", "localhost", 1891);
        let (client, mut conn) = Client::new(opt, 1);

        sleep(Duration::from_millis(100));
        let Event::Incoming(Packet::ConnAck(_)) = conn.recv().unwrap().unwrap() else { panic!() };

        let input_one = json!({"field_1": 123, "field_2": "abc"});
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
        client.publish("streams/input_one", QoS::AtMostOnce, false, input_one.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
            panic!()
        };

        let input_two = json!({"field_3": 098, "field_4": "zyx"});
        client.publish("streams/input_two", QoS::AtMostOnce, false, input_two.to_string()).unwrap();
        let Event::Outgoing(_) = conn.recv_timeout(Duration::from_millis(200)).unwrap().unwrap()
        else {
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
}
