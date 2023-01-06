use std::{collections::HashMap, sync::Arc};

use flume::{Receiver, Selector};
use rumqttc::AsyncClient;

use crate::base::MetricsConfig;
use crate::Config;

use self::serializer::{SerializerMetrics, SerializerMetricsHandler};
use self::stream::{StreamMetrics, StreamMetricsHandler};

mod serializer;
mod stream;

/// Interface implementing MQTT protocol to communicate with broker
pub struct Monitor {
    /// Uplink config
    config: Arc<Config>,
    /// Client handle
    client: AsyncClient,
    /// Stream metrics handler
    stream_metrics_handler: Option<StreamMetricsHandler>,
    /// Serializer metrics handler
    serializer_metrics_handler: Option<SerializerMetricsHandler>,
    /// Stream metrics receiver
    stream_metrics_rx: Receiver<StreamMetrics>,
    /// Serializer metrics receiver
    serializer_metrics_rx: Receiver<SerializerMetrics>,
}

impl Monitor {
    pub fn new(
        config: Arc<Config>,
        client: AsyncClient,
        stream_metrics_rx: Receiver<StreamMetrics>,
        serializer_metrics_rx: Receiver<SerializerMetrics>,
    ) -> Monitor {
        let stream_metrics_handler = config.stream_metrics.enabled.then(|| {
            let topic = String::from("/tenants/")
                + &config.project_id
                + "/devices/"
                + &config.device_id
                + "/events/uplink_stream_metrics/jsonarray";

            StreamMetricsHandler::new(topic, config.stream_metrics.blacklist.clone())
        });

        let serializer_metrics_handler = config.serializer_metrics.enabled.then(|| {
            let topic = String::from("/tenants/")
                + &config.project_id
                + "/devices/"
                + &config.device_id
                + "/events/uplink_serializer_metrics/jsonarray";

            SerializerMetricsHandler::new(topic)
        });

        Monitor {
            config,
            client,
            stream_metrics_handler,
            serializer_metrics_handler,
            stream_metrics_rx,
            serializer_metrics_rx,
        }
    }

    pub fn start(mut self) {
        loop {
            flume::Selector::new()
                .recv(&self.serializer_metrics_rx, |b| println!("Received {:?}", b))
                .recv(&self.stream_metrics_rx, |n| println!("Received {:?}", n))
                .wait();
        }
    }
}
