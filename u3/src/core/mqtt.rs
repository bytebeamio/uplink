use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;
use flume::{Receiver, Sender};
use log::{debug, error, info};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use serde::Serialize;
use serde_json::json;
use tokio::select;
use tokio::time::sleep;
use crate::{AppConfig, DataRow, PublishItem, CONFIG};
use crate::utils::clock;

pub struct Action {
    pub action_id: String,
    pub name: String,
    pub payload: String,
}

pub async fn send_action_response(data_tx: &Sender<DataRow>, action_id: impl ToString, state: impl ToString, progress: u8, errors: &[String]) {
    let _ = data_tx.send_async(DataRow {
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
    }).await;
}

pub struct MqttConnectionHandler {
    data_tx: Sender<DataRow>,
    actions_mapping: HashMap<String, Sender<Action>>,

    client: AsyncClient,
    eventloop: EventLoop,
    metrics_sequence: u32,
    metrics: MqttMetrics,
}

impl MqttConnectionHandler {
    pub fn new(data_tx: Sender<DataRow>, actions_mapping: HashMap<String, Sender<Action>>) -> (AsyncClient, Self) {
        CONFIG.with(|config| {
            let options = mqttoptions(config);
            let (client, mut eventloop) = AsyncClient::new(options, 0);
            eventloop.network_options.set_connection_timeout(config.cfg.mqtt.network_timeout);
            // TODO: load inflight messages from disk
            (client.clone(), Self {
                data_tx,
                actions_mapping,
                client,
                eventloop,
                metrics_sequence: 0,
                metrics: MqttMetrics::default(),
            })
        })
    }

    pub async fn run(mut self) {
        let mut disconnection_wait_timer = None;
        let mut subscribe_for_actions = false;
        let actions_topic = CONFIG.with(|c| format!("/tenants/{}/devices/{}/actions", c.auth.project_id, c.auth.device_id));
        let client = self.client.clone();
        loop {
            select! {
                event = self.eventloop.poll(), if disconnection_wait_timer.is_none() => {
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                            info!("Connected to broker. Session present = {}", connack.session_present);
                            self.metrics.connections += 1;
                            subscribe_for_actions = true;
                        }
                        Ok(Event::Incoming(Incoming::Publish(p))) => {
                            self.metrics.actions_received += 1;
                            if p.topic != actions_topic {
                                error!("Unsolicited publish on topic({:?})", p.topic);
                            } else {
                                // TODO: send action to collector
                            }
                        }
                        Ok(Event::Incoming(packet)) => {
                            debug!("Incoming = {:?}", packet);
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
                            debug!("Outgoing = {:?}", packet);
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
                    match resp {
                        Ok(..) => info!("Subscribe -> {:?}", actions_topic),
                        Err(e) => error!("Failed to send subscription. Error = {:?}", e),
                    }
                }
                _ = disconnection_wait_timer.take().unwrap_or(sleep(Duration::MAX)), if disconnection_wait_timer.is_some() => {
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
}

impl Drop for MqttConnectionHandler {
    fn drop(&mut self) {
        // TODO: save inflight messages to disk
    }
}

fn mqttoptions(config: &AppConfig) -> MqttOptions {
    let mut mqttoptions =
        MqttOptions::new(&config.auth.device_id, &config.auth.broker, config.auth.port);
    mqttoptions.set_max_packet_size(config.cfg.mqtt.max_packet_size, config.cfg.mqtt.max_packet_size);
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