use bytes::BytesMut;
use flume::{bounded, Receiver, Sender};
use log::{debug, error, info, warn};
use tokio::time::{sleep, Duration};
use tokio::{select, task};

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Mutex;

use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions, Packet, Publish, QoS,
    Request, TlsConfiguration, Transport,
};
use std::sync::Arc;

use crate::uplink_config::UplinkConfig;
use crate::Action;
use crate::base::bridge::Payload;
use crate::base::serializer::storage::PersistenceFile;
use crate::base::events::pusher::EventsPusher;
pub use self::metrics::MqttMetrics;

mod metrics;

/// Interface implementing MQTT protocol to communicate with broker
pub struct Mqtt {
    config: Arc<UplinkConfig>,
    pub client: AsyncClient,
    eventloop: EventLoop,
    shutdown_signal: Receiver<()>,
    actions_tx: Sender<Action>,
    metrics: MqttMetrics,
    metrics_tx: Sender<Payload>,
    network_up: Arc<Mutex<bool>>,
    actions_subscription: String,
    tenant_filter: String,
}

impl Mqtt {
    pub fn new(
        config: Arc<UplinkConfig>,
        actions_tx: Sender<Action>,
        metrics_tx: Sender<Payload>,
        shutdown_signal: Receiver<()>,
        network_up: Arc<Mutex<bool>>,
    ) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let (client, mut eventloop) = AsyncClient::new(options, 0);
        eventloop.network_options.set_connection_timeout(config.app_config.mqtt.network_timeout);
        let actions_subscription = format!("/tenants/{}/devices/{}/actions", config.credentials.project_id, config.credentials.device_id);
        let tenant_filter = format!("/tenants/{}/devices/{}", config.credentials.project_id, config.credentials.device_id);
        Mqtt {
            config,
            client,
            eventloop,
            shutdown_signal,
            actions_tx,
            metrics: MqttMetrics::new(),
            metrics_tx,
            network_up,
            actions_subscription,
            tenant_filter,
        }
    }

    /// Shutdown eventloop and write inflight publish packets to disk
    pub fn persist_inflight(&mut self) -> anyhow::Result<()> {
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
            return Ok(());
        }

        let file = PersistenceFile::new(&self.config.app_config.persistence_path, "inflight".to_string());
        let mut buf = BytesMut::new();

        for publish in publishes {
            publish.write(&mut buf)?;
        }

        file.write(&mut buf)?;
        info!("Pending publishes written to disk: {}", file.path().display());

        Ok(())
    }

    /// Checks for and loads data pending in persistence/inflight file
    /// once done, deletes the file, while writing incoming data into storage.
    fn reload_from_inflight_file(&mut self) -> anyhow::Result<()> {
        // Read contents of inflight file into an in-memory buffer
        let file = PersistenceFile::new(&self.config.app_config.persistence_path, "inflight".to_string());
        let path = file.path();
        if !path.is_file() {
            return Ok(());
        }
        let mut buf = BytesMut::new();
        file.read(&mut buf)?;

        let max_packet_size = self.config.app_config.mqtt.max_packet_size;
        loop {
            // NOTE: This can fail when packet sizes > max_payload_size in config are written to disk.
            match Packet::read(&mut buf, max_packet_size) {
                Ok(Packet::Publish(publish)) => {
                    if publish.topic.starts_with(&self.tenant_filter) {
                        self.eventloop.pending.push_back(Request::Publish(publish))
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

        info!("Pending publishes read from disk; removing file: {}", path.display());
        file.delete()?;

        Ok(())
    }

    /// Poll eventloop to receive packets from broker
    pub async fn start(mut self) {
        if let Err(e) = self.reload_from_inflight_file() {
            error!("Error recovering data from inflight file: {e}");
        }

        let (events_puback_tx, events_puback_rx) = bounded::<u16>(32);
        if self.config.app_config.console.enable_events {
            // we use two pkids after the rumqttc reserved pkids for events
            // the second pkid won't be sent until uplink has received an acknowledgement for the first pkid
            // and the first pkid won't be sent again until uplink receives an acknowledgement for the second pkid
            // which will only happen after all the messages with the first pkid are processed by the broker
            // this ensures that only two events pkids will be in transit at any time because mqtt standard guarantees
            // that PubAck pkids have to be in the same order as Publish pkids
            let max_inflight = self.eventloop.mqtt_options.inflight();
            let pusher_task = EventsPusher::new(
                events_puback_rx, self.client.clone(),
                self.config.credentials.project_id.clone(),
                self.config.credentials.device_id.clone(),
                [max_inflight+1, max_inflight+2],
                self.config.app_config.persistence_path.join("events.db"),
            );
            tokio::task::spawn(pusher_task.start());
        }

        let mut disconnection_wait_timer = None;
        loop {
            select! {
                event = self.eventloop.poll(), if disconnection_wait_timer.is_none() => {
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                            *self.network_up.lock().unwrap() = true;
                            info!("Connected to broker. Session present = {}", connack.session_present);
                            let subscription = self.actions_subscription.clone();
                            let client = self.client.clone();

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
                            self.handle_incoming_publish(p)
                        }
                        Ok(Event::Incoming(packet)) => {
                            debug!("Incoming = {:?}", packet);
                            match packet {
                                Packet::PubAck(puback) => {
                                    self.metrics.add_puback();
                                    let _ = events_puback_tx.try_send(puback.pkid);
                                },
                                Packet::PingResp => {
                                    self.metrics.add_pingresp();
                                    let inflight = self.eventloop.state.inflight();
                                    self.metrics.update_inflight(inflight);
                                    self.check_and_flush_metrics();
                                }
                                _ => {}
                            }
                        }
                        Ok(Event::Outgoing(packet)) => {
                            debug!("Outgoing = {:?}", packet);
                            match packet {
                                rumqttc::Outgoing::Publish(_) => self.metrics.add_publish(),
                                rumqttc::Outgoing::PingReq => self.metrics.add_pingreq(),
                                _ => {}
                            }
                        }
                        Err(e) => {
                            *self.network_up.lock().unwrap() = false;
                            self.metrics.add_reconnection();
                            self.check_disconnection_metrics(e);
                            disconnection_wait_timer = Some(sleep(Duration::from_secs(3)));
                            continue;
                        }
                    }
                },
                _ = disconnection_wait_timer.take().unwrap_or(sleep(Duration::MAX)), if disconnection_wait_timer.is_some() => {
                    disconnection_wait_timer = None;
                }
                Ok(_) = self.shutdown_signal.recv_async() => {
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

    fn handle_incoming_publish(&mut self, publish: Publish) {
        self.metrics.add_action();
        if self.actions_subscription != publish.topic {
            error!("Unsolicited publish on {}", publish.topic);
            return;
        }
        match serde_json::from_slice(&publish.payload) {
            Ok(action) => {
                info!("received action: {action:?}");
                let _ = self.actions_tx.try_send(action);
            }
            Err(_) => {
                error!("received invalid action from broker: {:?}", std::str::from_utf8(publish.payload.as_ref()));
            }
        }
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

        let _ = self.metrics_tx.try_send(metrics.to_payload());
        self.metrics.prepare_next();
    }
}

fn mqttoptions(config: &UplinkConfig) -> MqttOptions {
    // let (rsa_private, ca) = get_certs(&config.key.unwrap(), &config.ca.unwrap());
    let mut mqttoptions =
        MqttOptions::new(&config.credentials.device_id, &config.credentials.broker, config.credentials.port);
    mqttoptions.set_max_packet_size(config.app_config.mqtt.max_packet_size, config.app_config.mqtt.max_packet_size);
    mqttoptions.set_keep_alive(Duration::from_secs(config.app_config.mqtt.keep_alive));
    mqttoptions.set_inflight(config.app_config.mqtt.max_inflight);

    if let Some(auth) = config.credentials.authentication.clone() {
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
        let _ = self.inner.send_async(MqttShutdown).await;
    }
}
