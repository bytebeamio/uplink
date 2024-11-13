use std::io;
use std::sync::Arc;

use flume::{Receiver, RecvError};
use rumqttc::{AsyncClient, ClientError, QoS};
use tokio::select;

use crate::Config;

use super::bridge::StreamMetrics;
use super::mqtt::MqttMetrics;
use super::serializer::SerializerMetrics;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Collector recv error {0}")]
    Collector(#[from] RecvError),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Mqtt client error {0}")]
    Client(#[from] ClientError),
}

/// Interface implementing MQTT protocol to communicate with a broker, handling metrics publishing.
pub struct Monitor {
    /// Shared configuration for the uplink
    config: Arc<Config>,
    /// Client handle for publishing to MQTT
    client: AsyncClient,
    /// Receiver for stream metrics
    stream_metrics_rx: Receiver<StreamMetrics>,
    /// Receiver for serializer metrics
    serializer_metrics_rx: Receiver<SerializerMetrics>,
    /// Receiver for MQTT-related metrics
    mqtt_metrics_rx: Receiver<MqttMetrics>,
}

impl Monitor {
    /// Creates a new `Monitor` with given configuration, client, and receivers for various metrics.
    pub fn new(
        config: Arc<Config>,
        client: AsyncClient,
        stream_metrics_rx: Receiver<StreamMetrics>,
        serializer_metrics_rx: Receiver<SerializerMetrics>,
        mqtt_metrics_rx: Receiver<MqttMetrics>,
    ) -> Monitor {
        Monitor { config, client, stream_metrics_rx, serializer_metrics_rx, mqtt_metrics_rx }
    }

    /// Starts the `Monitor` to listen for metrics and publish them to MQTT topics.
    pub async fn start(&self) -> Result<(), Error> {
        let stream_metrics_config = self.config.stream_metrics.clone();
        let bridge_stream_metrics_topic = stream_metrics_config.bridge_topic;
        let serializer_stream_metrics_topic = stream_metrics_config.serializer_topic;

        let serializer_metrics_config = self.config.serializer_metrics.clone();
        let serializer_metrics_topic = serializer_metrics_config.topic;

        let mqtt_metrics_config = self.config.mqtt_metrics.clone();
        let mqtt_metrics_topic = mqtt_metrics_config.topic;

        // Initialize empty buffers for metrics
        let mut bridge_stream_metrics = Vec::with_capacity(10);
        let mut serializer_stream_metrics = Vec::with_capacity(10);
        let mut serializer_metrics = Vec::with_capacity(10);
        let mut mqtt_metrics = Vec::with_capacity(10);

        loop {
            select! {
                // Process stream metrics and publish if not blacklisted
                stream_metric = self.stream_metrics_rx.recv_async() => {
                    let metric = stream_metric?;
                    if stream_metrics_config.blacklist.contains(metric.stream()) {
                        continue;
                    }
                    bridge_stream_metrics.push(metric);
                    self.publish_metrics(&bridge_stream_metrics, &bridge_stream_metrics_topic).await?;
                    bridge_stream_metrics.clear();
                }

                // Process serializer metrics
                serializer_metric = self.serializer_metrics_rx.recv_async() => {
                    match serializer_metric? {
                        SerializerMetrics::Main(main_metric) => {
                            serializer_metrics.push(main_metric);
                            self.publish_metrics(&serializer_metrics, &serializer_metrics_topic).await?;
                            serializer_metrics.clear();
                        }
                        SerializerMetrics::Stream(stream_metric) => {
                            if stream_metrics_config.blacklist.contains(&stream_metric.stream) {
                                continue;
                            }
                            serializer_stream_metrics.push(stream_metric);
                            self.publish_metrics(&serializer_stream_metrics, &serializer_stream_metrics_topic).await?;
                            serializer_stream_metrics.clear();
                        }
                    }
                }

                // Process MQTT metrics
                mqtt_metric = self.mqtt_metrics_rx.recv_async() => {
                    let metric = mqtt_metric?;
                    mqtt_metrics.push(metric);
                    self.publish_metrics(&mqtt_metrics, &mqtt_metrics_topic).await?;
                    mqtt_metrics.clear();
                }
            }
        }
    }

    /// Helper function to serialize and publish metrics to a given MQTT topic.
    async fn publish_metrics<T: serde::Serialize>(
        &self,
        metrics: &[T],
        topic: &str,
    ) -> Result<(), Error> {
        let serialized = serde_json::to_string(metrics)?;
        self.client.publish(topic, QoS::AtLeastOnce, false, serialized).await?;
        Ok(())
    }
}
