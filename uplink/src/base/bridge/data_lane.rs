use std::sync::Arc;

use flume::{bounded, Receiver, RecvError, Sender};
use log::{debug, error};
use tokio::{select, time::interval};

use super::{streams::Streams, DataBridgeShutdown, Package, Payload, StreamMetrics};
use crate::config::{Config, DeviceConfig};

/// Custom error type for `DataBridge`
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("DataBridge receive error: {0}")]
    Recv(#[from] RecvError),
}

/// A bridge to manage data transmission from applications to data streams with metrics and control signals.
pub struct DataBridge {
    /// Shared configuration
    config: Arc<Config>,
    /// Sender for data payloads from applications
    data_tx: Sender<Payload>,
    /// Receiver for data payloads
    data_rx: Receiver<Payload>,
    /// Handler for managing data streams
    streams: Streams<Payload>,
    /// Receiver for control signals to shutdown or control the bridge
    ctrl_rx: Receiver<DataBridgeShutdown>,
    /// Sender for control signals to shutdown or control the bridge
    ctrl_tx: Sender<DataBridgeShutdown>,
}

impl DataBridge {
    /// Creates a new `DataBridge` with the specified configuration, device config, and channels for metrics and packages.
    pub fn new(
        config: Arc<Config>,
        device_config: Arc<DeviceConfig>,
        package_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        // Channels for data payload and control messages
        let (data_tx, data_rx) = bounded(10);
        let (ctrl_tx, ctrl_rx) = bounded(1);

        // Initialize stream handler with configuration
        let streams = Streams::new(
            config.max_stream_count,
            device_config,
            config.streams.clone(),
            package_tx,
            metrics_tx,
        );

        Self { config, data_tx, data_rx, streams, ctrl_rx, ctrl_tx }
    }

    /// Returns a handle for sending data payloads.
    pub fn data_tx(&self) -> DataTx {
        DataTx { inner: self.data_tx.clone() }
    }

    /// Returns a handle for sending control messages to the data bridge.
    pub fn ctrl_tx(&self) -> CtrlTx {
        CtrlTx { inner: self.ctrl_tx.clone() }
    }

    /// Starts the main loop for the `DataBridge`, handling data reception, stream timeouts, metrics flushing, and shutdown.
    pub async fn start(&mut self) -> Result<(), Error> {
        let mut metrics_timeout = interval(self.config.stream_metrics.timeout);

        loop {
            select! {
                // Process incoming data payloads
                data = self.data_rx.recv_async() => {
                    let data = data?;
                    self.streams.forward(data).await;
                }

                // Handle stream timeouts and flush expired streams
                Some(timedout_stream) = self.streams.stream_timeouts.next(), if self.streams.stream_timeouts.has_pending() => {
                    debug!("Flushing timed-out stream: {timedout_stream}");
                    if let Err(e) = self.streams.flush_stream(&timedout_stream).await {
                        error!("Failed to flush stream: {timedout_stream}. Error: {e}");
                    }
                }

                // Periodically flush metrics for all streams
                _ = metrics_timeout.tick() => {
                    if let Err(e) = self.streams.check_and_flush_metrics() {
                        debug!("Failed to flush stream metrics. Error: {e}");
                    }
                }

                // Handle shutdown signal and flush all remaining data
                _ = self.ctrl_rx.recv_async() => {
                    self.streams.flush_all().await;
                    return Ok(());
                }
            }
        }
    }
}

/// A handle for applications to send data payloads to the `DataBridge`.
#[derive(Debug, Clone)]
pub struct DataTx {
    pub inner: Sender<Payload>,
}

impl DataTx {
    /// Sends a payload asynchronously.
    pub async fn send_payload(&self, payload: Payload) {
        if let Err(e) = self.inner.send_async(payload).await {
            error!("Failed to send payload asynchronously: {e}");
        }
    }

    /// Sends a payload synchronously.
    pub fn send_payload_sync(&self, payload: Payload) {
        if let Err(e) = self.inner.send(payload) {
            error!("Failed to send payload synchronously: {e}");
        }
    }
}

/// A handle to control the `DataBridge`, allowing shutdown signals to be sent.
#[derive(Debug, Clone)]
pub struct CtrlTx {
    inner: Sender<DataBridgeShutdown>,
}

impl CtrlTx {
    /// Triggers an asynchronous shutdown of the `DataBridge`.
    pub async fn trigger_shutdown(&self) {
        if let Err(e) = self.inner.send_async(DataBridgeShutdown).await {
            error!("Failed to send shutdown signal: {e}");
        }
    }
}
