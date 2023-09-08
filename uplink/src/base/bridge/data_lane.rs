use std::{sync::Arc, time::Duration};

use flume::{bounded, Receiver, RecvError, Sender};
use log::{debug, error};
use tokio::{select, time::interval};

use crate::Config;

use super::{streams::Streams, Package, Payload, StreamMetrics};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Receiver error {0}")]
    Recv(#[from] RecvError),
}

pub struct DataBridge {
    /// All configuration
    config: Arc<Config>,
    /// Tx handle to give to apps
    data_tx: Sender<Payload>,
    /// Rx to receive data from apps
    data_rx: Receiver<Payload>,
    /// Handle to send data over streams
    streams: Streams,
}

impl DataBridge {
    pub fn new(
        config: Arc<Config>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        let (data_tx, data_rx) = bounded(10);
        let streams = Streams::new(config.clone(), package_tx, metrics_tx);

        Self { data_tx, data_rx, config, streams }
    }

    pub fn tx(&self) -> DataBridgeTx {
        DataBridgeTx { data_tx: self.data_tx.clone() }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut metrics_timeout = interval(Duration::from_secs(self.config.stream_metrics.timeout));

        loop {
            select! {
                data = self.data_rx.recv_async() => {
                    let data = data?;
                    self.streams.forward(data).await;
                }
                // Flush streams that timeout
                Some(timedout_stream) = self.streams.stream_timeouts.next(), if self.streams.stream_timeouts.has_pending() => {
                    debug!("Flushing stream = {}", timedout_stream);
                    if let Err(e) = self.streams.flush_stream(&timedout_stream).await {
                        error!("Failed to flush stream = {}. Error = {}", timedout_stream, e);
                    }
                }
                // Flush all metrics when timed out
                _ = metrics_timeout.tick() => {
                    if let Err(e) = self.streams.check_and_flush_metrics() {
                        debug!("Failed to flush stream metrics. Error = {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataBridgeTx {
    // Handle for apps to send action status to bridge
    pub(crate) data_tx: Sender<Payload>,
}

impl DataBridgeTx {
    pub async fn send_payload(&self, payload: Payload) {
        self.data_tx.send_async(payload).await.unwrap()
    }

    pub fn send_payload_sync(&self, payload: Payload) {
        self.data_tx.send(payload).unwrap()
    }
}
