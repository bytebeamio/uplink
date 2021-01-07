use crate::base::{Config, Package};

use rumqttc::*;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub struct Serializer {
    config: Arc<Config>,
    collector_rx: Receiver<Box<dyn Package>>,
    client: AsyncClient,
}

impl Serializer {
    pub fn new(config: Arc<Config>, collector_rx: Receiver<Box<dyn Package>>, client: AsyncClient) -> Serializer {
        Serializer { config, collector_rx, client }
    }

    pub async fn start(&mut self) {
        loop {
            let data = match self.collector_rx.recv().await {
                Some(data) => data,
                None => {
                    error!("Senders closed!!");
                    return;
                }
            };

            let channel = &data.channel();
            let topic = self.config.channels.get(channel).unwrap().topic.clone();
            let payload = data.serialize();
            let qos = QoS::AtLeastOnce;

            self.client.publish(topic, qos, false, payload).await.unwrap();
        }
    }
}
