use crate::base::{Config, Package};

use rumq_client::{self, QoS, Request};
use tokio::sync::mpsc::{Sender, Receiver};

pub struct Serializer {
    config:       Config,
    collector_rx: Receiver<Box<dyn Package>>,
    mqtt_tx:      Sender<Request>,
}

impl Serializer {
    pub fn new(config: Config, collector_rx: Receiver<Box<dyn Package>>, mqtt_tx: Sender<Request>) -> Serializer {
        Serializer { config, collector_rx, mqtt_tx }
    }

    pub async fn start(&mut self) {
        loop {
            let data = match self.collector_rx.recv().await {
                Some(data) => data,
                None => {
                    error!("Senders closed!!");
                    return
                }
            };

            let channel = &data.channel();
            let topic = self.config.channels.get(channel).unwrap().topic.clone();
            let payload = data.serialize();
            let qos = QoS::AtLeastOnce;

            let mut publish = rumq_client::Publish::new(topic, qos, payload);
            // NOTE this is required for serialization in persistentstream. Otherwise QoS 1 packets
            // with no packetid in the payload will be misinterpreted during deserialization
            // TODO See if there is a way to fix this
            publish.set_pkid(0);
            let publish = Request::Publish(publish);
            self.mqtt_tx.send(publish).await.unwrap();
        }
    }
}

