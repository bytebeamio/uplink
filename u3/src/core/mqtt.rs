use crate::core::storage::PersistenceFile;
use crate::utils::{clock, chain};
use crate::{AppContext, DataRow, PublishItem};
use bytes::BytesMut;
use flume::{Receiver, Sender};
use log::{debug, error, info, warn};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, Packet, Publish, QoS, Request,
    TlsConfiguration, Transport,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::io::Error;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;

#[derive(Deserialize)]
pub struct Action {
    pub id: String,
    pub name: String,
    pub payload: String,
}

pub async fn send_action_response(
    data_tx: &Sender<DataRow>,
    action_id: impl ToString,
    state: impl ToString,
    progress: u8,
    errors: &[String],
) {
    let _ = data_tx
        .send_async(DataRow {
            stream: "action_status".to_string(),
            data: PublishItem {
                sequence: 0,
                timestamp: clock(),
                data: json!({
                    "action_id": action_id.to_string(),
                    "state": state.to_string(),
                    "progress": progress,
                    "errors": errors
                }),
            },
        })
        .await;
}

pub struct MqttConnectionHandler {
    context: Arc<AppContext>,
    data_tx: Sender<DataRow>,
    mqtt_rx: Receiver<Publish>,
    actions_mapping: HashMap<String, Sender<Action>>,

    client: AsyncClient,
    eventloop: EventLoop,
    metrics_sequence: u32,
    metrics: MqttMetrics,
}

impl MqttConnectionHandler {
    pub fn new(
        context: Arc<AppContext>,
        data_tx: Sender<DataRow>,
        mqtt_rx: Receiver<Publish>,
        actions_mapping: HashMap<String, Sender<Action>>,
    ) -> Self {
        let options = mqttoptions(&context);
        let (client, mut eventloop) = AsyncClient::new(options, 0);
        eventloop.network_options.set_connection_timeout(context.cfg.mqtt.network_timeout);
        let mut handler = Self {
            context: context.clone(),
            data_tx,
            mqtt_rx,
            actions_mapping,
            client: client.clone(),
            eventloop,
            metrics_sequence: 0,
            metrics: MqttMetrics::default(),
        };
        use std::str::FromStr;
        let persistence_file =
            PersistenceFile::new(&context.cfg.persistence_path, "inflight.bin".to_owned());
        if let Err(e) = handler.reload_from_inflight_file(&persistence_file) {
            error!("couldn't read inflight file: {e:?}");
        }
        let _ = persistence_file.delete();
        handler
    }

    pub async fn run(mut self) {
        let mut transfer_task = chain(self.mqtt_rx.clone(), self.client.request_tx.clone());
        tokio::pin!(transfer_task);
        let mut disconnection_wait_timer = None;
        let mut subscribe_for_actions = false;
        let actions_topic = format!("/tenants/{}/devices/{}/actions", self.context.auth.project_id, self.context.auth.device_id);
        let client = self.client.clone();
        loop {
            select! {
                _ = &mut transfer_task => {},
                event = self.eventloop.poll(), if disconnection_wait_timer.is_none() => {
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                            info!("connected to broker");
                            self.metrics.connections += 1;
                            subscribe_for_actions = true;
                        }
                        Ok(Event::Incoming(Incoming::Publish(p))) => {
                            self.metrics.actions_received += 1;
                            if p.topic != actions_topic {
                                error!("unsolicited publish on topic({:?})", p.topic);
                            } else {
                                let s = std::str::from_utf8(&p.payload).unwrap();
                                if let Ok(action) = serde_json::from_str::<Action>(s) {
                                    // TODO: send_async inside mqtt select
                                    if let Some(handler) = self.actions_mapping.get(&action.name) {
                                        let _ = handler.send_async(action).await;
                                    } else {
                                        let _ = self.data_tx.send_async(DataRow {
                                            stream: "action_status".to_string(),
                                            data: PublishItem {
                                                sequence: 0,
                                                timestamp: clock(),
                                                data: json!({
                                                    "action_id": action.id,
                                                    "state": "Failed",
                                                    "progress": 100,
                                                    "errors": "uplink isn't configured to handler this action",
                                                }),
                                            },
                                        }).await;
                                    }
                                } else {
                                    error!("received invalid payload from broker as action: {s}");
                                }
                            }
                        }
                        Ok(Event::Incoming(packet)) => {
                            debug!("incoming = {:?}", packet);
                            match packet {
                                Packet::PubAck(puback) => {
                                    self.metrics.pubacks += 1;
                                },
                                Packet::PingResp => {
                                    self.metrics.ping_responses += 1;
                                    self.metrics.inflight = self.eventloop.state.inflight();
                                    self.check_and_flush_metrics();
                                }
                                _ => {}
                            }
                        }
                        Ok(Event::Outgoing(packet)) => {
                            debug!("outgoing = {:?}", packet);
                            match packet {
                                rumqttc::Outgoing::Publish(_) => self.metrics.publishes += 1,
                                rumqttc::Outgoing::PingReq => self.metrics.ping_requests += 1,
                                _ => {}
                            }
                        }
                        Err(error) => {
                            self.metrics.connection_retries += 1;
                            error!(
                                "disconnected: reconnects = {:<3} publishes = {:<3} pubacks = {:<3} pingreqs = {:<3} pingresps = {:<3} error = \"{error:>20}\"",
                                self.metrics.connection_retries,
                                self.metrics.publishes,
                                self.metrics.pubacks,
                                self.metrics.ping_requests,
                                self.metrics.ping_responses,
                            );
                            disconnection_wait_timer = Some(sleep(Duration::from_secs(3)));
                            continue;
                        }
                    }
                },
                resp = client.subscribe(&actions_topic, QoS::AtLeastOnce), if subscribe_for_actions => {
                    subscribe_for_actions = false;
                    if let Err(e) = resp {
                        error!("failed to subscribe for actions. Error = {:?}", e);
                    }
                }
                _ = async { disconnection_wait_timer.take().unwrap().await }, if disconnection_wait_timer.is_some() => {
                    disconnection_wait_timer = None;
                }
            }
        }
    }

    pub fn check_and_flush_metrics(&mut self) {
        let metrics = self.metrics.clone();
        info!(
            "{:>35}: publishes = {:<3} pubacks = {:<3} pingreqs = {:<3} pingresps = {:<3} inflight = {}",
            "connected",
            metrics.publishes,
            metrics.pubacks,
            metrics.ping_requests,
            metrics.ping_responses,
            metrics.inflight
        );

        // this goes to serializer which is supposed to never block
        let _ = self.data_tx.send(DataRow {
            stream: "uplink_mqtt_metrics".to_string(),
            data: PublishItem {
                sequence: self.metrics_sequence,
                timestamp: clock(),
                data: serde_json::to_value(&self.metrics).unwrap(),
            },
        });
        self.metrics_sequence += 1;
        self.metrics.publishes = 0;
        self.metrics.pubacks = 0;
        self.metrics.ping_requests = 0;
        self.metrics.ping_responses = 0;
        self.metrics.connections = 0;
        self.metrics.connection_retries = 0;
        self.metrics.inflight = 0;
    }

    /// Checks for and loads data pending in persistence/inflight file
    /// once done, deletes the file, while writing incoming data into storage.
    fn reload_from_inflight_file(&mut self, file: &PersistenceFile) -> anyhow::Result<()> {
        let tenant_filter = format!("/tenants/{}/devices/{}", self.context.auth.project_id, self.context.auth.device_id);
        let path = file.path();
        if !path.is_file() {
            return Ok(());
        }
        info!("reloading mqtt inflight messages from last shutdown");
        let mut buf = BytesMut::new();
        file.read(&mut buf)?;

        loop {
            match Packet::read(&mut buf, self.context.cfg.mqtt.max_packet_size) {
                Ok(Packet::Publish(publish)) => {
                    if publish.topic.starts_with(&tenant_filter) {
                        self.eventloop.pending.push_back(Request::Publish(publish))
                    } else {
                        warn!("inflight file has data with wrong tenant|device!");
                    }
                }
                Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
                Err(rumqttc::Error::InsufficientBytes(_)) => break,
                Err(e) => {
                    error!("Error reading from file: {e}");
                    break;
                }
            }
        }
        info!("mqtt inflight messages loaded successfully");
        Ok(())
    }
}

impl Drop for MqttConnectionHandler {
    fn drop(&mut self) {
        self.eventloop.clean();
        let publishes: Vec<&Publish> = self
            .eventloop
            .pending
            .iter()
            .filter_map(|request| match request {
                Request::Publish(publish) => Some(publish),
                _ => None,
            })
            .collect();

        if publishes.is_empty() {
            info!("no inflight messages");
        } else {
            let file =
                PersistenceFile::new(&self.context.cfg.persistence_path, "inflight.bin".to_string());
            let mut buf = BytesMut::new();
            for publish in publishes {
                if let Err(e) = publish.write(&mut buf) {
                    error!("couldn't serialize an inflight message: {e:?}");
                }
            }
            match file.write(&mut buf) {
                Ok(_) => {
                    info!("Pending publishes written to disk: {}", file.path().display());
                }
                Err(e) => {
                    error!("couldn't write inflight messages to disk: {e:?}");
                }
            }
        }
    }
}

fn mqttoptions(config: &AppContext) -> MqttOptions {
    let mut mqttoptions =
        MqttOptions::new(&config.auth.device_id, &config.auth.broker, config.auth.port);
    mqttoptions
        .set_max_packet_size(config.cfg.mqtt.max_packet_size, config.cfg.mqtt.max_packet_size);
    mqttoptions.set_keep_alive(Duration::from_secs(config.cfg.mqtt.keep_alive));
    mqttoptions.set_inflight(config.cfg.mqtt.max_inflight);

    if let Some(auth) = config.auth.authentication.clone() {
        let ca = auth.ca_certificate.into_bytes();
        let device_certificate = auth.device_certificate.into_bytes();
        let device_private_key = auth.device_private_key.into_bytes();
        let transport = Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: Some((device_certificate, device_private_key)),
        });

        mqttoptions.set_transport(transport);
    }

    mqttoptions
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct MqttMetrics {
    pub publishes: usize,
    pub pubacks: usize,
    pub ping_requests: usize,
    pub ping_responses: usize,
    pub inflight: u16,
    pub actions_received: usize,
    pub connections: usize,
    pub connection_retries: usize,
}
