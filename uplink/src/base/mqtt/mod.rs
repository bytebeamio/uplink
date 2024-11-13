use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use flume::{bounded, Receiver, Sender, TrySendError};
use log::{debug, error, info};
pub use metrics::MqttMetrics;
use rumqttc::{
    AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions, Outgoing, Packet,
    Publish, QoS, Request, TlsConfiguration, Transport,
};
use storage::PersistenceFile;
use thiserror::Error;
use tokio::{select, task, time::Duration};

use crate::{
    config::{Config, DeviceConfig},
    Action,
};

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
    /// True when network is connected
    network_up: Arc<Mutex<bool>>,
}

impl Mqtt {
    pub fn new(
        config: Arc<Config>,
        device_config: &DeviceConfig,
        actions_tx: Sender<Action>,
        metrics_tx: Sender<MqttMetrics>,
        network_up: Arc<Mutex<bool>>,
    ) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config, device_config);
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
            network_up,
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
        // Clean up any expired or invalid events in the event loop
        self.eventloop.clean();

        // Collect all pending `Publish` requests from the event loop
        let pending_publishes: Vec<&Publish> = self
            .eventloop
            .pending
            .iter()
            .filter_map(|request| match request {
                Request::Publish(publish) => Some(publish),
                _ => None,
            })
            .collect();

        // If there are no pending publishes, exit early
        if pending_publishes.is_empty() {
            return Ok(());
        }

        // Create a persistence file to store inflight messages on disk
        let file = PersistenceFile::new(&self.config.persistence_path, "inflight".to_string())?;
        let mut buf = BytesMut::new();

        // Serialize each publish message into the buffer
        for publish in pending_publishes {
            publish.write(&mut buf)?;
        }

        // Write the serialized buffer to the persistence file
        file.write(&mut buf)?;
        debug!("Pending publishes written to disk at: {}", file.path().display());

        Ok(())
    }

    /// Checks for and loads data pending in persistence/inflight file
    /// once done, deletes the file, while writing incoming data into storage.
    fn reload_from_inflight_file(&mut self) -> Result<(), Error> {
        // Attempt to read the contents of the inflight file into an in-memory buffer
        let file = PersistenceFile::new(&self.config.persistence_path, "inflight".to_string())?;
        let path = file.path();

        // If the inflight file doesn't exist, exit early
        if !path.is_file() {
            return Ok(());
        }

        // Initialize an in-memory buffer to store the contents of the inflight file
        let mut buffer = BytesMut::new();
        file.read(&mut buffer)?;

        // Define the maximum packet size allowed, as per the configuration
        let max_packet_size = self.config.mqtt.max_packet_size;

        // Deserialize packets from the buffer and add them back to the event loop's pending queue
        loop {
            match Packet::read(&mut buffer, max_packet_size) {
                // Add each `Publish` packet back to the pending queue
                Ok(Packet::Publish(publish)) => {
                    self.eventloop.pending.push_back(Request::Publish(publish));
                }
                // This should never happen, as only `Publish` packets are expected
                Ok(packet) => unreachable!("Unexpected packet type: {:?}", packet),
                // Break if there are not enough bytes left for a full packet
                Err(rumqttc::Error::InsufficientBytes(_)) => break,
                // Log an error if there's a problem reading a packet and stop the loop
                Err(e) => {
                    error!("Error reading packet from file: {e}");
                    break;
                }
            }
        }

        // Once all packets are processed, delete the inflight file to prevent re-reading on restart
        info!("Finished reading pending publishes from disk. Deleting file: {}", path.display());
        file.delete()?;

        Ok(())
    }

    /// Poll eventloop to receive packets from broker
    pub async fn start(mut self) {
        if let Err(e) = self.reload_from_inflight_file() {
            error!("Error recovering data from inflight file: {e}");
        }

        // Main event loop, handling incoming and outgoing events
        loop {
            select! {
                // Poll the event loop for any MQTT events
                event = self.eventloop.poll() => {
                    match event {
                        // Handle connection acknowledgment from the broker
                        Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                            *self.network_up.lock().unwrap() = true;
                            info!("Connected to broker. Session present = {}", connack.session_present);

                            // Record a new connection in the metrics
                            self.metrics.add_connection();

                            // Clone client and subscription details for non-blocking subscription task
                            let subscription = self.config.actions_subscription.clone();
                            let client = self.client();

                            // Spawn a subscription task to handle potential network back pressure without blocking
                            task::spawn(async move {
                                match client.subscribe(&subscription, QoS::AtLeastOnce).await {
                                    Ok(..) => info!("Successfully subscribed to {:?}", subscription),
                                    Err(e) => error!("Subscription failed. Error = {:?}", e),
                                }
                            });
                        },

                        // Process an incoming publish message from the broker
                        Ok(Event::Incoming(Incoming::Publish(publish))) => {
                            self.metrics.add_action();
                            if let Err(e) = self.handle_incoming_publish(publish) {
                                error!("Failed to process incoming publish message. Error = {:?}", e);
                            }
                        }

                        // Process acknowledgment of published messages
                        Ok(Event::Incoming(Incoming::PubAck(_))) => {
                            self.metrics.add_puback();
                        }

                        // Handle ping response from the broker, and update inflight count
                        Ok(Event::Incoming(Incoming::PingResp)) => {
                            self.metrics.add_pingresp();

                            // Update metrics with current inflight message count and flush if necessary
                            let inflight_count = self.eventloop.state.inflight();
                            self.metrics.update_inflight(inflight_count);
                            if let Err(e) = self.check_and_flush_metrics() {
                                error!("Error flushing MQTT metrics. Error = {:?}", e);
                            }
                        }

                        // Log any other incoming packets for debugging purposes
                        Ok(Event::Incoming(packet)) => {
                            debug!("Received unexpected incoming packet: {:?}", packet);
                        }

                        // Track outgoing publish requests in metrics
                        Ok(Event::Outgoing(Outgoing::Publish(_))) => {
                            self.metrics.add_publish();
                        }

                        // Track outgoing ping requests in metrics
                        Ok(Event::Outgoing(Outgoing::PingReq)) => {
                            self.metrics.add_pingreq();
                        }

                        // Log any other outgoing packets for debugging purposes
                        Ok(Event::Outgoing(packet)) => {
                            debug!("Sent unexpected outgoing packet: {:?}", packet);
                        }

                        // Handle any errors encountered in the event loop
                        Err(e) => {
                            *self.network_up.lock().unwrap() = false;
                            self.metrics.add_reconnection();

                            // Check and log disconnection details, and pause before retrying
                            self.check_disconnection_metrics(e);
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        }
                    }
                },

                // Check for shutdown signal to exit the loop
                Ok(MqttShutdown) = self.ctrl_rx.recv_async() => {
                    break;
                }
            }
        }

        // Handle inflight messages by persisting them to disk on shutdown
        if let Err(e) = self.persist_inflight() {
            error!("Error persisting inflight messages to disk during shutdown. Error = {:?}", e);
        }
    }

    /// Processes an incoming `Publish` message, extracting and handling an `Action`.
    fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(), Error> {
        // Verify that the topic matches the expected subscription topic
        if self.config.actions_subscription != publish.topic {
            error!("Received publish on unexpected topic: {}", publish.topic);
            return Ok(());
        }

        // Deserialize the payload into an `Action` object
        let action: Action = serde_json::from_slice(&publish.payload)?;
        info!("Received Action: {:?}", action);

        // Send the action to the native actions channel immediately, or fail
        self.native_actions_tx.try_send(action)?;

        Ok(())
    }

    // Enable actual metrics timers when there is data. This method is called every minute by the bridge
    pub fn check_disconnection_metrics(&mut self, error: ConnectionError) {
        error!(
            "disconnected: reconnects = {:<3} publishes = {:<3} pubacks = {:<3} pingreqs = {:<3} pingresps = {:<3} error = \"{error:>20}\"",
            self.metrics.connection_retries,
            self.metrics.publishes,
            self.metrics.pubacks,
            self.metrics.ping_requests,
            self.metrics.ping_responses,
        );
    }

    // Enable actual metrics timers when there is data. This method is called every minute by the bridge
    pub fn check_and_flush_metrics(&mut self) -> Result<(), flume::TrySendError<MqttMetrics>> {
        info!(
            "{:>35}: publishes = {:<3} pubacks = {:<3} pingreqs = {:<3} pingresps = {:<3} inflight = {}",
            "connected",
            self.metrics.publishes,
            self.metrics.pubacks,
            self.metrics.ping_requests,
            self.metrics.ping_responses,
            self.metrics.inflight
        );

        self.metrics_tx.try_send(self.metrics.clone())?;
        self.metrics.prepare_next();
        Ok(())
    }
}

/// Creates and configures `MqttOptions` based on device and application configurations.
fn mqttoptions(config: &Config, device_config: &DeviceConfig) -> MqttOptions {
    // Initialize MQTT options with device ID, broker address, and port
    let mut mqttoptions =
        MqttOptions::new(&device_config.device_id, &device_config.broker, device_config.port);

    // Set packet size and keep-alive configurations
    mqttoptions.set_max_packet_size(config.mqtt.max_packet_size, config.mqtt.max_packet_size);
    mqttoptions.set_keep_alive(Duration::from_secs(config.mqtt.keep_alive));
    mqttoptions.set_inflight(config.mqtt.max_inflight);

    // Configure TLS transport if authentication details are provided
    if let Some(auth) = &device_config.authentication {
        let ca_certificate = auth.ca_certificate.clone().into_bytes();
        let device_certificate = auth.device_certificate.clone().into_bytes();
        let device_private_key = auth.device_private_key.clone().into_bytes();

        // Configure TLS with client authentication
        let tls_config = TlsConfiguration::Simple {
            ca: ca_certificate,
            alpn: None,
            client_auth: Some((device_certificate, device_private_key)),
        };

        mqttoptions.set_transport(Transport::Tls(tls_config));
    }

    mqttoptions
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
