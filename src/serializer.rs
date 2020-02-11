use crate::collector::Packable;
use crate::Config;

use crossbeam_channel::Receiver;
use futures_util::stream::StreamExt;
use rumq_client::{self, eventloop, MqttOptions, Notification, QoS, Request};
use tokio::sync::mpsc::{self, channel, Sender};

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::thread;
use std::time::Duration;

pub struct Serializer {
    config: Config,
    collector_rx: Receiver<Box<dyn Packable + Send>>,
    mqtt_tx: Sender<Request>,
}

impl Serializer {
    pub fn new(config: Config, collector_rx: Receiver<Box<dyn Packable + Send>>) -> Serializer {
        let (requests_tx, requests_rx) = channel::<Request>(1);

        let options = mqttoptions(config.clone());

        thread::spawn(move || mqtt(options, requests_rx));

        Serializer {
            config,
            collector_rx,
            mqtt_tx: requests_tx,
        }
    }

    #[tokio::main(basic_scheduler)]
    pub async fn start(&mut self) {
        for data in self.collector_rx.iter() {
            let channel = &data.channel();

            let topic = self.config.channels.get(channel).unwrap().topic.clone();
            let payload = data.serialize();
            let qos = QoS::AtMostOnce;

            let publish = rumq_client::publish(topic, qos, payload);
            let publish = Request::Publish(publish);
            self.mqtt_tx.send(publish).await.unwrap();
        }
    }
}

#[tokio::main(basic_scheduler)]
async fn mqtt(options: MqttOptions, requests_rx: mpsc::Receiver<Request>) {
    // create a new eventloop and reuse it during every reconnection
    let mut eventloop = eventloop(options, requests_rx);

    loop {
        let mut stream = eventloop.stream();
        while let Some(notification) = stream.next().await {
            if let Notification::StreamEnd(err) = &notification {
                error!("Pipeline stream closed!! Error = {:?}", err);
                tokio::time::delay_for(Duration::from_secs(10)).await;
                continue;
            }

            debug!("Notification = {:?}", notification);
        }
    }
}

fn mqttoptions(config: Config) -> MqttOptions {
    // let (rsa_private, ca) = get_certs(&config.key.unwrap(), &config.ca.unwrap());
    let mut mqttoptions = MqttOptions::new(config.device_id, "localhost", 1883);
    mqttoptions.set_keep_alive(30);
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
