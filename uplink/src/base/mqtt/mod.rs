use bytes::BytesMut;
use flume::{Receiver, Sender, TrySendError};
use log::{debug, error, info};
use thiserror::Error;
use tokio::time::Duration;
use tokio::{select, task};

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use crate::{Action, Config};
use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, Incoming, Key, MqttOptions, Publish, QoS,
    Request, TlsConfiguration, Transport,
};
use std::sync::Arc;

pub use self::metrics::MqttMetrics;

mod metrics;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("TrySend error {0}")]
    TrySend(Box<TrySendError<Action>>),
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Mqtt error {0}")]
    Mqtt(#[from] rumqttc::mqttbytes::Error),
}

impl From<flume::TrySendError<Action>> for Error {
    fn from(e: flume::TrySendError<Action>) -> Self {
        Self::TrySend(Box::new(e))
    }
}

pub struct MqttShutdown;

/// Interface implementing MQTT protocol to communicate with broker
pub struct Mqtt {
    /// Uplink config
    config: Arc<Config>,
    /// Client handle
    client: AsyncClient,
    /// Event loop handle
    eventloop: EventLoop,
    /// Handles to channels between threads
    native_actions_tx: Sender<Action>,
    /// Metrics
    metrics: MqttMetrics,
    /// Metrics tx
    metrics_tx: Sender<MqttMetrics>,
    ctrl_rx: Receiver<MqttShutdown>,
}

impl Mqtt {
    pub fn new(
        config: Arc<Config>,
        actions_tx: Sender<Action>,
        metrics_tx: Sender<MqttMetrics>,
        ctrl_rx: Receiver<MqttShutdown>,
    ) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let (client, mut eventloop) = AsyncClient::new(options, 1);
        eventloop.network_options.set_connection_timeout(config.mqtt.network_timeout);

        Mqtt {
            config,
            client,
            eventloop,
            native_actions_tx: actions_tx,
            metrics: MqttMetrics::new(),
            metrics_tx,
            ctrl_rx,
        }
    }

    /// Returns a client handle to MQTT interface
    pub fn client(&mut self) -> AsyncClient {
        self.client.clone()
    }

    /// Shutdown eventloop and write inflight publish packets to disk
    pub fn persist_inflight(&mut self) -> Result<(), Error> {
        self.eventloop.clean();
        let pending = std::mem::take(&mut self.eventloop.pending);
        let publishes: Vec<Publish> = pending
            .filter_map(|request| match &request {
                Request::Publish(publish) => Some(publish.clone()),
                _ => None,
            })
            .collect();

        if publishes.is_empty() {
            return Ok(());
        }

        let mut path = self.config.persistence_path.clone();
        path.push("inflight");
        debug!("Writing pending publishes to disk: {}", path.display());
        let mut f = fs::File::create(path)?;
        let mut buf = BytesMut::new();

        for publish in publishes {
            publish.write(&mut buf)?;
        }

        f.write_all(&buf)?;
        f.flush()?;

        Ok(())
    }

    /// Poll eventloop to receive packets from broker
    pub async fn start(mut self) {
        loop {
            select! {
                event = self.eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                            info!("Connected to broker. Session present = {}", connack.session_present);
                            let subscription = self.config.actions_subscription.clone();
                            let client = self.client();

                            self.metrics.add_connection();

                            // This can potentially block when client from other threads
                            // have already filled the channel due to bad network. So we spawn
                            task::spawn(async move {
                                match client.subscribe(&subscription, QoS::AtLeastOnce).await {
                                    Ok(..) => info!("Subscribe -> {:?}", subscription),
                                    Err(e) => error!("Failed to send subscription. Error = {:?}", e),
                                }
                            });
                        }
                        Ok(Event::Incoming(Incoming::Publish(p))) => {
                            self.metrics.add_action();
                            if let Err(e) = self.handle_incoming_publish(p) {
                                error!("Incoming publish handle failed. Error = {:?}", e);
                            }
                        }
                        Ok(Event::Incoming(packet)) => {
                            debug!("Incoming = {:?}", packet);
                            match packet {
                                rumqttc::Packet::PubAck(_) => self.metrics.add_puback(),
                                rumqttc::Packet::PingResp => {
                                    self.metrics.add_pingresp();
                                    let inflight = self.eventloop.state.inflight();
                                    self.metrics.update_inflight(inflight);
                                    if let Err(e) = self.check_and_flush_metrics() {
                                        error!("Failed to flush MQTT metrics. Erro = {:?}", e);
                                    }
                                }
                                _ => {}
                            }
                        }
                        Ok(Event::Outgoing(packet)) => {
                            debug!("Outgoing = {:?}", packet);
                            match packet {
                                rumqttc::Outgoing::Publish(_) => self.metrics.add_publish(),
                                rumqttc::Outgoing::PingReq => {
                                    self.metrics.add_pingreq();
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            self.metrics.add_reconnection();
                            self.check_disconnection_metrics(e);
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            continue;
                        }
                    }
                },
                Ok(MqttShutdown) = self.ctrl_rx.recv_async() => {
                    break;
                }
            }
        }

        // TODO: when uplink uses wills to handle unexpected disconnections, the following maybe useful,
        // till then sending a disconnect to broker is unnecessary.
        // // Try to disconnect from mqtt connection, else force persist. Timeout in a second
        // if let Err(e) = timeout(Duration::from_secs(1), self.disconnect()).await {
        //     error!("Mqtt disconnection timedout. Error = {:?}", e);
        // }

        if let Err(e) = self.persist_inflight() {
            error!("Couldn't persist inflight messages. Error = {:?}", e);
        }
    }

    // async fn disconnect(&mut self) {
    //     if let Err(e) = self.client.disconnect().await {
    //         error!("Client unable to trigger disconnect. Error = {:?}", e);
    //         return;
    //     }

    //     // poll eventloop till the outgoing disconnect packet comes up
    //     loop {
    //         if let Ok(Event::Outgoing(Outgoing::Disconnect)) = self.eventloop.poll().await {
    //             info!("Disconnection successful!");
    //             break;
    //         }
    //     }
    // }

    fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(), Error> {
        if self.config.actions_subscription != publish.topic {
            error!("Unsolicited publish on {}", publish.topic);
            return Ok(());
        }

        let action: Action = serde_json::from_slice(&publish.payload)?;
        info!("Action = {:?}", action);
        self.native_actions_tx.try_send(action)?;

        Ok(())
    }

    // Enable actual metrics timers when there is data. This method is called every minute by the bridge
    pub fn check_disconnection_metrics(&mut self, error: ConnectionError) {
        let metrics = self.metrics.clone();
        error!(
            "disconnected: reconnects = {:<3} publishes = {:<3} pubacks = {:<3} pingreqs = {:<3} pingresps = {:<3} error = \"{error:>20}\"",
            metrics.connection_retries,
            metrics.publishes,
            metrics.pubacks,
            metrics.ping_requests,
            metrics.ping_responses,
        );
    }

    // Enable actual metrics timers when there is data. This method is called every minute by the bridge
    pub fn check_and_flush_metrics(&mut self) -> Result<(), flume::TrySendError<MqttMetrics>> {
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

        self.metrics_tx.try_send(metrics)?;
        self.metrics.prepare_next();
        Ok(())
    }
}

fn mqttoptions(config: &Config) -> MqttOptions {
    // let (rsa_private, ca) = get_certs(&config.key.unwrap(), &config.ca.unwrap());
    let mut mqttoptions = MqttOptions::new(&config.device_id, &config.broker, config.port);
    mqttoptions.set_max_packet_size(config.mqtt.max_packet_size, config.mqtt.max_packet_size);
    mqttoptions.set_keep_alive(Duration::from_secs(config.mqtt.keep_alive));
    mqttoptions.set_inflight(config.mqtt.max_inflight);

    if let Some(auth) = config.authentication.clone() {
        let ca = auth.ca_certificate.into_bytes();
        let device_certificate = auth.device_certificate.into_bytes();
        let device_private_key = auth.device_private_key.into_bytes();
        let transport = Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: Some((device_certificate, Key::RSA(device_private_key))),
        });

        mqttoptions.set_transport(transport);
    }

    mqttoptions
}

fn _get_certs(key_path: &Path, ca_path: &Path) -> (Vec<u8>, Vec<u8>) {
    println!("{key_path:?}");
    let mut key = Vec::new();
    let mut key_file = File::open(key_path).unwrap();
    key_file.read_to_end(&mut key).unwrap();

    let mut ca = Vec::new();
    let mut ca_file = File::open(ca_path).unwrap();
    ca_file.read_to_end(&mut ca).unwrap();

    (key, ca)
}
