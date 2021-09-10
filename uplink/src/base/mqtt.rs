use async_channel::{Sender, TrySendError};
use log::{debug, error, info};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, Key, MqttOptions, Publish, QoS, TlsConfiguration,
    Transport,
};
use thiserror::Error;
use tokio::{task, time::Duration};

use std::sync::Arc;

use crate::base::{actions::Action, Config};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Serde error {0}")]
    ActionForward(#[from] TrySendError<Action>),
}

/// Interface implementing MQTT protocol to communicate with broker
pub struct Mqtt {
    /// Shared reference to system config
    config: Arc<Config>,
    /// Client handle
    client: AsyncClient,
    /// Event loop handle
    eventloop: EventLoop,
    /// Handles to channels between threads
    native_actions_tx: Sender<Action>,
    bridge_actions_tx: Sender<Action>,
    /// Currently subscribed topic
    actions_subscription: String,
}

impl Mqtt {
    pub fn new(
        config: Arc<Config>,
        actions_tx: Sender<Action>,
        bridge_actions_tx: Sender<Action>,
    ) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let (client, eventloop) = AsyncClient::new(options, 10);
        let actions_subscription =
            format!("/tenants/{}/devices/{}/actions", config.project_id, config.device_id);
        Mqtt {
            config,
            client,
            eventloop,
            native_actions_tx: actions_tx,
            bridge_actions_tx,
            actions_subscription,
        }
    }

    /// Returns a client handle to MQTT interface
    pub fn client(&mut self) -> AsyncClient {
        self.client.clone()
    }

    /// Poll eventloop to receive packets from broker
    pub async fn start(&mut self) {
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    let subscription = self.actions_subscription.clone();
                    let client = self.client();

                    // This can potentially block when client from other threads
                    // have already filled the channel due to bad network. So we spawn
                    task::spawn(async move {
                        match client.subscribe(subscription.clone(), QoS::AtLeastOnce).await {
                            Ok(..) => info!("Subscribe -> {:?}", subscription),
                            Err(e) => error!("Failed to send subscription. Error = {:?}", e),
                        }
                    });
                }
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if let Err(e) = self.handle_incoming_publish(p) {
                        error!("Incoming publish handle failed. Error = {:?}", e);
                    }
                }
                Ok(Event::Incoming(i)) => info!("Incoming = {:?}", i),
                Ok(Event::Outgoing(o)) => debug!("Outgoing = {:?}", o),
                Err(e) => {
                    error!("Connection error = {:?}", e.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(), Error> {
        if publish.topic != self.actions_subscription {
            error!("Unsolicited publish on {}", publish.topic);
            return Ok(());
        }

        let action: Action = serde_json::from_slice(&publish.payload)?;
        debug!("Action = {:?}", action);

        if !self.config.actions.contains(&action.id) {
            self.bridge_actions_tx.try_send(action)?;
        } else {
            self.native_actions_tx.try_send(action)?;
        }

        Ok(())
    }
}

fn mqttoptions(config: &Config) -> MqttOptions {
    let mut mqttoptions = MqttOptions::new(&config.device_id, &config.broker, config.port);
    mqttoptions.set_max_packet_size(config.max_packet_size, config.max_packet_size);
    mqttoptions.set_keep_alive(60);
    mqttoptions.set_inflight(config.max_inflight);

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

