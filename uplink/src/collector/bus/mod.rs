use std::{collections::HashSet, net::SocketAddr};

use flume::{bounded, Receiver};
use joins::Router;
use log::error;
use rumqttd::{
    local::{LinkRx, LinkTx},
    protocol::Publish,
    Broker, Config, ConnectionSettings, Forward, Notification, RouterConfig, ServerSettings,
};
use serde_json::{Map, Value};
use tokio::select;

use crate::{
    base::bridge::{BridgeTx, Payload},
    config::BusConfig,
    spawn_named_thread, Action,
};

mod joins;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Link error: {0}")]
    Link(#[from] rumqttd::local::LinkError),
    #[error("Parse error: {0}")]
    Parse(#[from] std::net::AddrParseError),
    #[error("Recv error: {0}")]
    Recv(#[from] flume::RecvError),
}

pub struct BusRx {
    rx: LinkRx,
}

impl BusRx {
    async fn recv_async(&mut self) -> Option<Publish> {
        loop {
            return match self.rx.next().await {
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

impl BusTx {
    fn publish_data(&mut self, data: Payload) -> Result<(), Error> {
        let topic = format!("/streams/{}", data.stream);
        let payload = serde_json::to_vec(&data)?;
        self.tx.publish(topic, payload)?;

        Ok(())
    }

    fn subscribe_to_streams(
        &mut self,
        streams: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), Error> {
        for stream in streams {
            let filter = format!("/streams/{}", stream.into());
            self.tx.subscribe(filter)?;
        }

        Ok(())
    }

    fn run_action(&mut self, action: Action) -> Result<(), Error> {
        let topic = format!("/actions/{}", action.name);
        let response_topic = format!("/actions/{}/status", action.action_id);
        let payload = serde_json::to_vec(&action)?;

        self.tx.subscribe(response_topic)?;
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
                    if let Err(e) = self.tx.run_action(action) {
                        error!("{e}")
                    }
                }

                Some(publish) = self.rx.recv_async() => {
                    if publish.topic.starts_with(b"/actions/") && publish.topic.ends_with(b"/status") {
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

#[cfg(test)]
mod tests {
    use std::{
        thread::{sleep, spawn},
        time::Duration,
    };

    use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
    use serde_json::json;

    use crate::{
        base::bridge::{DataTx, StatusTx},
        config::JoinerConfig,
        ActionResponse,
    };

    use super::*;

    /// This test verifies that action status messages published to the bus are correctly received.
    #[test]
    fn recv_action_and_respond() {
        let port = 1883;
        let config = BusConfig { port, joins: JoinerConfig { output_streams: vec![] } };

        let (data_tx, _data_rx) = bounded(1);
        let (status_tx, status_rx) = bounded(1);
        let bridge_tx = BridgeTx {
            data_tx: DataTx { inner: data_tx },
            status_tx: StatusTx { inner: status_tx },
        };
        let (actions_tx, actions_rx) = bounded(1);
        spawn(|| Bus::new(config, bridge_tx, actions_rx).start());

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
            .publish(
                "/actions/123/status",
                QoS::AtMostOnce,
                false,
                json!(action_status).to_string(),
            )
            .unwrap();
        let Event::Outgoing(_) = conn.recv().unwrap().unwrap() else { panic!() };

        assert_eq!(action_status, status_rx.recv_timeout(Duration::from_millis(200)).unwrap());
    }
}
