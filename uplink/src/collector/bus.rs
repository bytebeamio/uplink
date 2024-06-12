use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use flume::{bounded, Receiver, Sender};
use log::error;
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
    config::{BusConfig, JoinConfig, NoDataAction},
    Action,
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
    pub fn new(
        config: BusConfig,
        bridge_tx: BridgeTx,
        actions_rx: Receiver<Action>,
    ) -> Result<Self, Error> {
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
            listen: format!("127.0.0.1:{}", config.port).parse::<SocketAddr>()?,
            tls: None,
            next_connection_delay_ms: 0,
            connections,
        };
        let servers = [("service_bus".to_owned(), server)].into_iter().collect();
        let mut broker =
            Broker::new(Config { id: 0, router, v4: Some(servers), ..Default::default() });
        let (tx, rx) = broker.link("uplink")?;
        broker.start()?;

        Ok(Self { tx: BusTx { tx }, rx: BusRx { rx }, bridge_tx, actions_rx, config })
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
                    for (key, value) in dbg!(data) {
                        router.map(key, value).await
                    }
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

struct Router {
    map: HashMap<String, Vec<Sender<(String, Value)>>>,
    tasks: JoinSet<()>,
}

impl Router {
    async fn new(configs: Vec<JoinConfig>, bridge_tx: BridgeTx, back_tx: Sender<Payload>) -> Self {
        let mut map: HashMap<String, Vec<Sender<(String, Value)>>> = HashMap::new();
        let mut tasks = JoinSet::new();
        for config in configs {
            let (tx, rx) = bounded(1);
            if let Some(senders) = map.get_mut(&config.name) {
                senders.push(tx);
                continue;
            }
            map.insert(config.name.to_string(), vec![tx]);

            let mut field_renames = HashMap::new();
            for stream in &config.construct_from {
                for field in &stream.select_fields {
                    if let Some(name) = &field.renamed {
                        field_renames.insert(
                            format!("{}.{}", stream.input_stream, field.original),
                            name.to_owned(),
                        );
                    }
                }
            }
            let joiner = Joiner {
                rx,
                joined: Map::new(),
                config,
                tx: bridge_tx.clone(),
                field_renames,
                back_tx: back_tx.clone(),
            };
            tasks.spawn(joiner.start());
        }

        Router { map, tasks }
    }
    async fn map(&mut self, key: String, value: Value) {
        let Some(iter) = self.map.get(&key) else { return };
        for tx in iter {
            _ = tx.send_async((key.clone(), value.clone())).await;
        }
    }
}

struct Joiner {
    rx: Receiver<(String, Value)>,
    joined: Map<String, Value>,
    config: JoinConfig,
    field_renames: HashMap<String, String>,
    tx: BridgeTx,
    back_tx: Sender<Payload>,
}

impl Joiner {
    async fn start(mut self) {
        let mut sequence = 0;
        let mut ticker = interval(self.config.time_interval);
        loop {
            select! {
                Ok((mut key, value)) = self.rx.recv_async() => {
                    if let Some(rename) = self.field_renames.get(&key){
                        rename.clone_into(&mut key);
                    }
                    self.joined.insert(key, value);
                }
                _ = ticker.tick() => {
                    if self.joined.is_empty() {
                        continue
                    }
                    sequence += 1;
                    let payload = Payload {
                        stream: self.config.name.clone(),
                        sequence,
                        timestamp: clock() as u64,
                        payload: json!(self.joined)
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
        }
    }
}
