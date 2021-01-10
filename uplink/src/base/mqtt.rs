use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::time::Duration;

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::base::actions::Action;
use crate::base::Config;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Publish, QoS};
use std::sync::Arc;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
}

pub struct Mqtt {
    config: Arc<Config>,
    client: AsyncClient,
    eventloop: EventLoop,
    actions_tx: Sender<Action>,
    bridge_actions_tx: Sender<Action>,
    actions_subscription: String,
}

impl Mqtt {
    pub fn new(config: Arc<Config>, actions_tx: Sender<Action>, bridge_actions_tx: Sender<Action>) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let (client, eventloop) = AsyncClient::new(options, 10);
        let actions_subscription = format!("/devices/{}/actions", config.device_id);
        Mqtt { config, client, eventloop, actions_tx, bridge_actions_tx, actions_subscription }
    }

    pub fn client(&mut self) -> AsyncClient {
        self.client.clone()
    }

    pub async fn start(&mut self) {
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    match self.client.subscribe(self.actions_subscription.clone(), QoS::AtLeastOnce).await {
                        Ok(..) => {
                            info!("Subscribe -> {:?}", self.actions_subscription);
                        }
                        Err(e) => {
                            error!("Failed to send subscription. Error = {:?}", e);
                        }
                    }
                }
                Ok(Event::Incoming(Incoming::Publish(publish))) => {
                    if let Err(e) = self.handle_incoming_publish(publish) {
                        error!("Incoming publish handle failed. Error = {:?}", e);
                    }
                }
                Ok(Event::Incoming(i)) => {
                    debug!("Incoming = {:?}", i);
                }
                Ok(Event::Outgoing(o)) => debug!("Outgoing = {:?}", o),
                Err(e) => {
                    println!("Connection error = {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(), Error> {
        if publish.topic != self.actions_subscription {
            error!("Unsolicited publish on {}", publish.topic);
        }

        let action: Action = serde_json::from_slice(&publish.payload)?;
        debug!("Action = {:?}", action);
        if !self.config.actions.contains(&action.id) {
            if let Err(e) = self.bridge_actions_tx.try_send(action) {
                error!("Failed to forward bridge action. Error = {:?}", e);
            }

            return Ok(());
        }

        if let Err(e) = self.actions_tx.try_send(action) {
            error!("Failed to forward action. Error = {:?}", e);
        }

        Ok(())
    }
}

fn mqttoptions(config: &Config) -> MqttOptions {
    // let (rsa_private, ca) = get_certs(&config.key.unwrap(), &config.ca.unwrap());
    let mut mqttoptions = MqttOptions::new(&config.device_id, &config.broker, config.port);
    mqttoptions.set_keep_alive(120);
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
