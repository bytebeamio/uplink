use bytes::BytesMut;
use flume::{bounded, Receiver, Sender, TrySendError};
use log::{debug, error, info};
use storage::PersistenceFile;
use thiserror::Error;
use tokio::time::Duration;
use tokio::{select, task};

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::{Action, Config};
use rumqttc::{
    read, AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions, Packet, Publish,
    QoS, Request, TlsConfiguration, Transport,
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
    #[error("Storage error {0}")]
    Storage(#[from] storage::Error),
}

impl From<flume::TrySendError<Action>> for Error {
    fn from(e: flume::TrySendError<Action>) -> Self {
        Self::TrySend(Box::new(e))
    }
}

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
    /// Control handles
    ctrl_rx: Receiver<MqttShutdown>,
    ctrl_tx: Sender<MqttShutdown>,
}

impl Mqtt {
    pub fn new(
        config: Arc<Config>,
        actions_tx: Sender<Action>,
        metrics_tx: Sender<MqttMetrics>,
    ) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let (client, mut eventloop) = AsyncClient::new(options, 0);
        eventloop.network_options.set_connection_timeout(config.mqtt.network_timeout);
        let (ctrl_tx, ctrl_rx) = bounded(1);

        Mqtt {
            config,
            client,
            eventloop,
            native_actions_tx: actions_tx,
            metrics: MqttMetrics::new(),
            metrics_tx,
            ctrl_tx,
            ctrl_rx,
        }
    }

    /// Returns a client handle to MQTT interface
    pub fn client(&mut self) -> AsyncClient {
        self.client.clone()
    }

    pub fn ctrl_tx(&self) -> CtrlTx {
        CtrlTx { inner: self.ctrl_tx.clone() }
    }

    /// Shutdown eventloop and write inflight publish packets to disk
    pub fn persist_inflight(&mut self) -> Result<(), Error> {
        self.eventloop.clean();
        let publishes: Vec<&mut Publish> = self
            .eventloop
            .pending
            .iter_mut()
            .filter_map(|request| match request {
                Request::Publish(publish) => Some(publish),
                _ => None,
            })
            .collect();

        if publishes.is_empty() {
            return Ok(());
        }

        let mut file = PersistenceFile::new(&self.config.persistence_path, "inflight".to_string())?;
        let mut buf = BytesMut::new();

        for publish in publishes {
            publish.pkid = 1;
            publish.write(&mut buf)?;
        }

        file.write(&mut buf)?;
        debug!("Pending publishes written to disk: {}", file.path().display());

        Ok(())
    }

    /// Checks for and loads data pending in persistence/inflight file
    /// once done, deletes the file, wile writing incoming data into storage.
    fn recover_inflight(&mut self) -> Result<(), Error> {
        // Read contents of inflight file into an in-memory buffer
        let mut file = PersistenceFile::new(&self.config.persistence_path, "inflight".to_string())?;
        let path = file.path();
        if !path.is_file() {
            return Ok(());
        }
        let mut buf = BytesMut::new();
        file.read(&mut buf)?;

        let max_packet_size = self.config.mqtt.max_packet_size;
        loop {
            // NOTE: This can fail when packet sizes > max_payload_size in config are written to disk.
            match read(&mut buf, max_packet_size) {
                Ok(Packet::Publish(publish)) => {
                    self.eventloop.pending.push_back(Request::Publish(publish))
                }
                Ok(packet) => unreachable!("Unexpected packet: {:?}", packet),
                Err(rumqttc::Error::InsufficientBytes(_)) => break,
                Err(e) => {
                    error!("Error reading from file: {e}");
                    break;
                }
            }
        }

        info!("Reloaded inflight publishes from previous session; removing file: {}", path.display());
        file.delete()?;

        Ok(())
    }

    /// Poll eventloop to receive packets from broker
    pub async fn start(mut self) {
        if let Err(e) = self.recover_inflight() {
            error!("Error recovering data from inflight file: {e}");
        }

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

        // TODO: when uplink uses last-wills to handle unexpected disconnections, try to disconnect from
        // mqtt connection, before force persisting in-flight publishes to disk. Timedout in a second.
        // But otherwise sending a disconnect to broker is unnecessary.

        if let Err(e) = self.persist_inflight() {
            error!("Couldn't persist inflight messages. Error = {:?}", e);
        }
    }

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
            client_auth: Some((device_certificate, device_private_key)),
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

/// Command to remotely trigger `Mqtt` shutdown
pub(crate) struct MqttShutdown;

/// Handle to send control messages to `Mqtt`
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub(crate) inner: Sender<MqttShutdown>,
}

impl CtrlTx {
    /// Triggers shutdown of `Mqtt`
    pub async fn trigger_shutdown(&self) {
        self.inner.send_async(MqttShutdown).await.unwrap()
    }
}
