use crossbeam_channel::{Receiver, Select};
use rumqtt::MqttClient;
use rumqtt::{MqttOptions, QoS};
use crate::error::PublisherError;

type Data = String;
type Notification = String;

pub struct Publisher {
    data_rx: Receiver<Vec<Data>>,
    mqtt_client: MqttClient
}

impl Publisher {
    pub fn new(data_rx: Receiver<Vec<Data>>, mqttoptions: MqttOptions) -> Result<Publisher, PublisherError> {
        let (mqtt_client, _mqtt_incoming) = MqttClient::start(mqttoptions)?;

        Ok(Publisher {
            data_rx,
            mqtt_client
        })
    }

    pub fn start(&mut self) -> Result<(), PublisherError> {
        loop {
            let msg = self.data_rx.recv().unwrap();
            let msg = vec![1, 2, 3];
            self.mqtt_client.publish("hello/world", QoS::AtLeastOnce, false, msg).unwrap();
        }

        Ok(())
    }
}