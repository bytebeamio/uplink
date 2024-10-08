use flume::Sender;
use rumqttc::{Publish, QoS, Request};

use crate::{
    base::serializer::{MqttClient, MqttError},
    config::StreamConfig,
    Package, Payload, Stream,
};

#[derive(Clone)]
pub struct MockClient {
    pub net_tx: Sender<Request>,
}

#[async_trait::async_trait]
impl MqttClient for MockClient {
    async fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), MqttError>
    where
        S: Into<String> + Send,
        V: Into<Vec<u8>> + Send,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        self.net_tx.send_async(publish).await.map_err(|e| MqttError::Send(e.into_inner()))?;
        Ok(())
    }

    fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), MqttError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let publish = Request::Publish(publish);
        self.net_tx.try_send(publish).map_err(|e| MqttError::TrySend(e.into_inner()))?;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Stream error {0}")]
    Base(#[from] crate::base::bridge::stream::Error),
}

pub struct MockCollector {
    stream: Stream<Payload>,
}

impl MockCollector {
    pub fn new(
        stream_name: &str,
        stream_config: StreamConfig,
        data_tx: Sender<Box<dyn Package>>,
    ) -> MockCollector {
        MockCollector { stream: Stream::new(stream_name, stream_config, data_tx) }
    }

    pub async fn send(&mut self, i: u32) -> Result<(), Error> {
        let payload = Payload {
            stream: Default::default(),
            sequence: i,
            timestamp: 0,
            payload: serde_json::from_str("{\"msg\": \"Hello, World!\"}")?,
        };
        self.stream.fill(payload).await?;

        Ok(())
    }
}
