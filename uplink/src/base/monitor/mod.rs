use std::io;
use std::sync::Arc;

use flume::{Receiver, RecvError};
use rumqttc::{AsyncClient, ClientError, Request};
use tokio::select;

use crate::collector::stream::StreamMetrics;
use crate::Config;

use super::serializer::SerializerMetrics;

mod stream;

/// Interface implementing MQTT protocol to communicate with broker
pub struct Monitor {
    /// Uplink config
    config: Arc<Config>,
    /// Client handle
    client: AsyncClient,
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
        // let stream_metrics_handler = config.stream_metrics.enabled.then(|| {
        //     let topic = String::from("/tenants/")
        //         + &config.project_id
        //         + "/devices/"
        //         + &config.device_id
        //         + "/events/uplink_stream_metrics/jsonarray";
        //
        //     StreamMetricsHandler::new(topic, config.stream_metrics.blacklist.clone())
        // });
        //
        // let serializer_metrics_handler = config.serializer_metrics.enabled.then(|| {
        //     let topic = String::from("/tenants/")
        //         + &config.project_id
        //         + "/devices/"
        //         + &config.device_id
        //         + "/events/uplink_serializer_metrics/jsonarray";
        //
        //     SerializerMetricsHandler::new(topic)
        // });

        Monitor { config, client, stream_metrics_rx, serializer_metrics_rx }
    }

    pub async fn start(&self) -> Result<(), Error> {
        loop {
            select! {
                o = self.serializer_metrics_rx.recv_async() => {
                    let o = o?;
                    println!("Received {:?}", o);
                }
                o = self.serializer_metrics_rx.recv_async() => {
                    let o = o?;
                    println!("Received {:?}", o);
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MqttError {
    #[error("SendError(..)")]
    Send(Request),
    #[error("TrySendError(..)")]
    TrySend(Request),
}

impl From<ClientError> for MqttError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Request(r) => MqttError::Send(r),
            ClientError::TryRequest(r) => MqttError::TrySend(r),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Collector recv error {0}")]
    Collector(#[from] RecvError),
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Mqtt client error {0}")]
    Client(#[from] MqttError),
}
