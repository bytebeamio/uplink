use std::sync::Arc;

use flume::{bounded, Receiver, RecvError, Sender};
use log::{debug, error};
use tokio::{select, time::interval};

use crate::config::{Config, DeviceConfig};

use super::{streams::Streams, DataBridgeShutdown, Package, Payload, StreamMetrics};

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
    streams: Streams<Payload>,
    ctrl_rx: Receiver<DataBridgeShutdown>,
    ctrl_tx: Sender<DataBridgeShutdown>,
}

impl DataBridge {
    pub fn new(
        config: Arc<Config>,
        device_config: Arc<DeviceConfig>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        let (data_tx, data_rx) = bounded(10);
        let (ctrl_tx, ctrl_rx) = bounded(1);

        let mut streams =
            Streams::new(config.max_stream_count, device_config, package_tx, metrics_tx);
        streams.config_streams(config.streams.clone());

        Self { data_tx, data_rx, config, streams, ctrl_rx, ctrl_tx }
    }

    /// Handle to send data points from source application
    pub fn data_tx(&self) -> DataTx {
        DataTx { inner: self.data_tx.clone() }
    }

    /// Handle to send data lane control message
    pub fn ctrl_tx(&self) -> CtrlTx {
        CtrlTx { inner: self.ctrl_tx.clone() }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut metrics_timeout = interval(self.config.stream_metrics.timeout);

        loop {
            select! {
                data = self.data_rx.recv_async() => {
                    let data = data?;
                    self.streams.forward(data).await;
                }
                // Flush streams that timeout
                Some(timedout_stream) = self.streams.stream_timeouts.next(), if self.streams.stream_timeouts.has_pending() => {
                    debug!("Flushing stream = {timedout_stream}");
                    if let Err(e) = self.streams.flush_stream(&timedout_stream).await {
                        error!("Failed to flush stream = {timedout_stream}. Error = {e}");
                    }
                }
                // Flush all metrics when timed out
                _ = metrics_timeout.tick() => {
                    if let Err(e) = self.streams.check_and_flush_metrics() {
                        debug!("Failed to flush stream metrics. Error = {e}");
                    }
                }
                // Handle a shutdown signal
                _ = self.ctrl_rx.recv_async() => {
                    self.streams.flush_all().await;

                    return Ok(())
                }
            }
        }
    }
}

/// Handle for apps to send action status to bridge
#[derive(Debug, Clone)]
pub struct DataTx {
    pub inner: Sender<Payload>,
}

impl DataTx {
    pub async fn send_payload(&self, payload: Payload) {
        self.inner.send_async(payload).await.unwrap()
    }

    pub fn send_payload_sync(&self, payload: Payload) {
        self.inner.send(payload).unwrap()
    }
}

/// Handle to send control messages to data lane
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub(crate) inner: Sender<DataBridgeShutdown>,
}

impl CtrlTx {
    /// Triggers shutdown of `bridge::data_lane`
    pub async fn trigger_shutdown(&self) {
        let _ = self.inner.send_async(DataBridgeShutdown).await;
    }
}
