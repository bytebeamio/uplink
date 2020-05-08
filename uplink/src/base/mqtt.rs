use rumq_client::{MqttEventLoop, eventloop, Subscribe, QoS, MqttOptions, Request, Notification, Publish};
use tokio::time::Duration;
use tokio::sync::mpsc::{Sender, Receiver};
use futures_util::stream::StreamExt;
use derive_more::From;

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::base::Config;
use crate::base::actions::Action;
use std::sync::Arc;

#[derive(Debug, From)]
pub enum Error {
    Serde(serde_json::Error),
}

pub struct Mqtt {
    config: Arc<Config>,
    eventloop: Option<MqttEventLoop>,
    actions_tx: Sender<Action>,
    bridge_actions_tx: Sender<Vec<u8>>,
    mqtt_tx: Sender<Request>,
    actions_subscription: String,
}

impl Mqtt {
    pub fn new(
        config: Arc<Config>,
        actions_tx: Sender<Action>,
        bridge_actions_tx: Sender<Vec<u8>>,
        mqtt_tx: Sender<Request>,
        mqtt_rx: Receiver<Request>
    ) -> Mqtt {
        // create a new eventloop and reuse it during every reconnection
        let options = mqttoptions(&config);
        let eventloop = eventloop(options, mqtt_rx);
        let actions_subscription = format!("/devices/{}/actions", config.device_id);

        Mqtt {
            config,
            eventloop: Some(eventloop),
            mqtt_tx,
            actions_tx,
            bridge_actions_tx,
            actions_subscription
        }
    }

    pub async fn start(&mut self) {
        let mut eventloop = self.eventloop.take().unwrap();

        loop {
            let mut stream = match eventloop.connect().await {
                Ok(s) => s,
                Err(e) => {
                    error!("Connection error = {:?}", e);
                    tokio::time::delay_for(Duration::from_secs(5)).await;
                    continue
                }
            };

            let subscription = Subscribe::new(self.actions_subscription.clone(), QoS::AtLeastOnce);
            let subscribe = Request::Subscribe(subscription);
            if let Err(e) = self.mqtt_tx.try_send(subscribe) {
                error!("Failed to send subscription. Error = {:?}", e);
            }

            while let Some(notification) = stream.next().await {
                // NOTE These should never block. Causes mqtt eventloop to halt
                match notification {
                    Notification::Publish(publish) => if let Err(e) = self.handle_incoming_publish(publish) {
                        error!("Incoming publish handle failed. Error = {:?}", e);
                    }
                    _ => {
                        debug!("Notification = {:?}", notification);
                    }
                }
            }

            // try reconnecting again in 5 seconds
            tokio::time::delay_for(Duration::from_secs(5)).await;
        }
    }

    fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(), Error> {
        if publish.topic_name != self.actions_subscription {
            error!("Unsolicited publish on {}", publish.topic_name);
        }

        let action: Action = serde_json::from_slice(&publish.payload)?;
        if self.config.actions.contains(&action.id) {
            if let Err(e) = self.bridge_actions_tx.try_send(publish.payload) {
                error!("Failed to forward bridge action. Error = {:?}", e);
            }

            return Ok(())
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
    mqttoptions.set_keep_alive(5);
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
