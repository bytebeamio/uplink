use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use flume::{bounded, Receiver};
use joins::Router;
use log::error;
use reqwest::get;
use rumqttd::{
    local::{LinkRx, LinkTx},
    protocol::Publish,
    Broker, Config, ConnectionSettings, ConsoleSettings, Forward, Notification, RouterConfig,
    ServerSettings,
};
use serde_json::{Map, Value};
use tokio::select;

use crate::{
    base::bridge::{BridgeTx, Payload},
    config::BusConfig,
    spawn_named_thread, Action, ActionResponse,
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
    #[error("Req error: {0}")]
    Req(#[from] reqwest::Error),
    #[error("Action was not expected")]
    NoRoute,
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
    console_url: String,
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

    async fn run_action(&mut self, action: &Action) -> Result<(), Error> {
        let topic = format!("/actions/{}", action.name);

        let url = format!("http://{}/subscriptions", self.console_url);
        let body = get(url).await?.bytes().await?;
        let subscriptions: HashMap<String, Vec<String>> = serde_json::from_slice(&body)?;

        if !subscriptions.get(&topic).is_some_and(|s| !s.is_empty()) {
            return Err(Error::NoRoute);
        }

        let response_topic = format!("/actions/{}/status", action.action_id);
        let payload = serde_json::to_vec(action)?;

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
        let mut console = ConsoleSettings::default();
        let console_url = format!("127.0.0.1:{}", config.console_port);
        console_url.clone_into(&mut console.listen);
        let servers = [("service_bus".to_owned(), server)].into_iter().collect();
        let mut broker = Broker::new(Config {
            id: 0,
            router,
            v4: Some(servers),
            console: Some(console),
            ..Default::default()
        });
        let (tx, rx) = broker.link("uplink").unwrap();
        spawn_named_thread("Broker", move || {
            if let Err(e) = broker.start() {
                error!("{e}")
            }
        });

        Self { tx: BusTx { tx, console_url }, rx: BusRx { rx }, bridge_tx, actions_rx, config }
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
                    if let Err(e) = self.tx.run_action(&action).await {
                        error!("{e}");
                        let status = ActionResponse::failure(&action.action_id, e.to_string());
                        self.bridge_tx.send_action_response(status).await;
                    }
                }

                Some(publish) = self.rx.recv_async() => {
                    if publish.topic.starts_with(b"/actions/") && publish.topic.ends_with(b"/status") {
                        let Ok(status) = serde_json::from_slice(&publish.payload) else {
                            error!("Couldn't parse payload as action status");
                            continue;
                        };
                        self.bridge_tx.send_action_response(status).await;
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
