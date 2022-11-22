use flume::{Sender, TrySendError};
use log::{debug, error, info};
use thiserror::Error;
use tokio::task;
use tokio::time::Duration;

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::{Action, Config};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, Key, MqttOptions, Publish, QoS, TlsConfiguration,
    Transport,
};
use std::sync::Arc;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Serde error {0}")]
    ActionForward(#[from] TrySendError<Action>),
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
    /// Currently subscribed topic
    actions_subscription: String,
}

impl Mqtt {
    pub fn new(config: Arc<Config>, actions_tx: Sender<Action>) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let (client, eventloop) = AsyncClient::new(options, 10);
        let actions_subscription =
            format!("/tenants/{}/devices/{}/actions", config.project_id, config.device_id);
        Mqtt { config, client, eventloop, native_actions_tx: actions_tx, actions_subscription }
    }

    /// Returns a client handle to MQTT interface
    pub fn client(&mut self) -> AsyncClient {
        self.client.clone()
    }

    /// Poll eventloop to receive packets from broker
    pub async fn start(mut self) {
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
                Ok(Event::Incoming(i)) => debug!("Incoming = {:?}", i),
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
        if self.config.simulator.is_none() && publish.topic != self.actions_subscription {
            error!("Unsolicited publish on {}", publish.topic);
            return Ok(());
        }

        let mut action: Action = serde_json::from_slice(&publish.payload)?;

        // Collect device_id information from publish topic for simulation purpose
        if self.config.simulator.is_some() {
            let tokens: Vec<&str> = publish.topic.split('/').collect();
            let mut tokens = tokens.iter();
            while let Some(token) = tokens.next() {
                if token == &"devices" {
                    action.device_id = tokens.next().unwrap().to_string();
                }
            }
        } else {
            action.device_id = self.config.device_id.clone();
        }

        debug!("Action = {:?}", action);
        self.native_actions_tx.try_send(action)?;

        Ok(())
    }
}

fn mqttoptions(config: &Config) -> MqttOptions {
    // let (rsa_private, ca) = get_certs(&config.key.unwrap(), &config.ca.unwrap());
    let mut mqttoptions = MqttOptions::new(&config.device_id, &config.broker, config.port);
    mqttoptions.set_max_packet_size(config.max_packet_size, config.max_packet_size);
    mqttoptions.set_keep_alive(Duration::from_secs(60));
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

fn _get_certs(key_path: &Path, ca_path: &Path) -> (Vec<u8>, Vec<u8>) {
    println!("{:?}", key_path);
    let mut key = Vec::new();
    let mut key_file = File::open(key_path).unwrap();
    key_file.read_to_end(&mut key).unwrap();

    let mut ca = Vec::new();
    let mut ca_file = File::open(ca_path).unwrap();
    ca_file.read_to_end(&mut ca).unwrap();

    (key, ca)
}
