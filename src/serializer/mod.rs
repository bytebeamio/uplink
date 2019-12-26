use crate::collector::Batch;
use crate::Config;

use crossbeam_channel::Receiver;
use futures_util::stream::StreamExt;
use jsonwebtoken::{encode, Algorithm, Header};
use rumq_client::{self, eventloop, MqttOptions, Request};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, channel, Sender};

use std::fs::File;
use std::io::Read;
use std::ops::Add;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

pub struct Serializer {
    config: Config,
    collector_rx: Receiver<Box<dyn Batch + Send>>,
    mqtt_tx: Sender<Request>
}

impl Serializer {
    pub fn new(config: Config, collector_rx: Receiver<Box<dyn Batch + Send>>) -> Serializer {
        let (requests_tx, requests_rx) = channel::<Request>(1);
        
        let options = mqttoptions(config.clone());
        
        thread::spawn(move || {
            gcloud(options, requests_rx)    
        });
        
        Serializer {
            config,
            collector_rx,
            mqtt_tx: requests_tx
        }
    }

    #[tokio::main(basic_scheduler)]
    pub async fn start(&mut self) {
        for data in self.collector_rx.iter() {
            let channel = &data.channel();

            let topic = self.config.channels.get(channel).unwrap().topic.clone();
            let payload = data.serialize();

            let publish = rumq_client::publish(topic, payload);
            let publish = Request::Publish(publish);
            self.mqtt_tx.send(publish).await.unwrap();
        }
    }
}

#[tokio::main(basic_scheduler)]
async fn gcloud(options: MqttOptions, requests_rx: mpsc::Receiver<Request>) {
    // create a new eventloop and reuse it during every reconnection
    let mut eventloop = eventloop(options, requests_rx);
    
    loop {
        let mut stream = eventloop.stream();

        while let Some(notification) = stream.next().await {
            debug!("Notification = {:?}", notification);
        }
    }
}

fn mqttoptions(config: Config) -> MqttOptions {
        let (rsa_private, ca) = get_certs(&config.key.unwrap(), &config.ca.unwrap());
        let password = gen_iotcore_password(&rsa_private, "cloudlinc");

        let mut mqttoptions = MqttOptions::new(config.device_id, "mqtt.googleapis.com", 8883);
        mqttoptions.set_keep_alive(30).set_ca(ca).set_credentials("unused", &password);

        mqttoptions
}

fn get_certs(key_path: &Path, ca_path: &Path) -> (Vec<u8>, Vec<u8>) {
    println!("{:?}", key_path);
    let mut key = Vec::new();
    let mut key_file = File::open(key_path).unwrap();
    key_file.read_to_end(&mut key).unwrap();

    let mut ca = Vec::new();
    let mut ca_file = File::open(ca_path).unwrap();
    ca_file.read_to_end(&mut ca).unwrap();

    (key, ca)
}

fn gen_iotcore_password(key: &[u8], project: &str) -> String {
    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iat: u64,
        exp: u64,
        aud: String,
    }

    let jwt_header = Header::new(Algorithm::RS256);
    let iat = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let exp = SystemTime::now()
        .add(Duration::from_secs(300))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = Claims {
        iat,
        exp,
        aud: project.into(),
    };
    encode(&jwt_header, &claims, key).unwrap()
}
