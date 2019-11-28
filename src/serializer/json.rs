use crate::collector::Buffer;

use crossbeam_channel::Receiver;
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions, SecurityOptions};
use serde::Serialize;

use crate::Config;
use std::path::Path;
use std::fs::File;
use std::io::Read;

pub struct Serializer<T> {
    config: Config,
    collector_rx: Receiver<Buffer<T>>,
    mqtt_client: rumqtt::MqttClient,
}

impl<T: Serialize> Serializer<T> {
    pub(crate) fn new(config: Config, collector_rx: Receiver<Buffer<T>>) -> Serializer<T> {
        let reconnection_options = ReconnectOptions::AfterFirstSuccess(5);

        let key = &config.key.clone().unwrap();
        let ca = &config.ca.clone().unwrap();
        let (rsa_private, ca) = get_certs(key, ca);

        let security_options = SecurityOptions::GcloudIot("cloudlinc".to_owned(), rsa_private.to_vec(), 60);
        let client_id = &config.device_id;

        let mqtt_options = MqttOptions::new(client_id, "mqtt.googleapis.com", 8883)
            .set_keep_alive(60)
            .set_reconnect_opts(reconnection_options)
            .set_ca(ca)
            .set_security_opts(security_options);

        let (mqtt_client, _notifications) = MqttClient::start(mqtt_options).unwrap();

        Serializer {
            config: config,
            collector_rx,
            mqtt_client,
        }
    }

    pub(crate) fn start(&mut self) {
        let qos = QoS::AtLeastOnce;

        for data in self.collector_rx.iter() {
            let buffer = &data.buffer;
            let channel = &data.channel;

            let topic = self.config.channels.get(channel).unwrap().topic.clone();
            let payload = serde_json::to_string(buffer).unwrap();
            self.mqtt_client.publish(topic, qos, false, payload).unwrap();
        }
    }
}

fn get_certs(key_path: &Path, ca_path: &Path) -> (Vec<u8>, Vec<u8>) {
    let mut key = Vec::new();
    let mut key_file = File::open(key_path).unwrap();
    key_file.read_to_end(&mut key).unwrap();

    let mut ca = Vec::new();
    let mut ca_file = File::open(ca_path).unwrap();
    ca_file.read_to_end(&mut ca).unwrap();

    (key, ca)
}
