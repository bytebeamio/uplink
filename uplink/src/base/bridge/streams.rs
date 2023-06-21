use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flume::Sender;
use log::{error, info, trace};
use tokio::time::{interval, Interval};

use super::stream::{self, StreamStatus, MAX_BUFFER_SIZE};
use super::StreamMetrics;
use crate::{Config, Package, Payload, Stream};

use super::delaymap::DelayMap;

pub struct Streams {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    metrics_tx: Sender<StreamMetrics>,
    map: HashMap<String, Stream<Payload>>,
    pub stream_timeouts: DelayMap<String>,
    pub metrics_timeouts: DelayMap<String>,
    pub metrics_timeout: Interval,
}

impl Streams {
    pub async fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
    ) -> Self {
        let mut map = HashMap::new();
        for (name, stream) in &config.streams {
            let stream = Stream::with_config(name, stream, data_tx.clone());
            map.insert(name.to_owned(), stream);
        }

        let metrics_timeout = interval(Duration::from_secs(config.stream_metrics.timeout));
        Self {
            config,
            data_tx,
            metrics_tx,
            map,
            stream_timeouts: DelayMap::new(),
            metrics_timeouts: DelayMap::new(),
            metrics_timeout,
        }
    }

    pub async fn forward(&mut self, data: Payload) {
        let stream_name = &data.stream;
        let (stream_id, device_id) = match &data.device_id {
            Some(device_id) => (stream_name.to_owned() + "/" + device_id, device_id.to_owned()),
            _ => (stream_name.to_owned(), self.config.device_id.to_owned()),
        };

        let stream = match self.map.get_mut(&stream_id) {
            Some(partition) => partition,
            None => {
                if self.config.simulator.is_none() && self.map.keys().len() > 20 {
                    error!("Failed to create {:?} stream. More than max 20 streams", stream_id);
                    return;
                }

                let stream = Stream::dynamic(
                    stream_name,
                    &self.config.project_id,
                    &device_id,
                    MAX_BUFFER_SIZE,
                    self.data_tx.clone(),
                );

                self.map.entry(stream_id.to_owned()).or_insert(stream)
            }
        };

        let max_stream_size = stream.max_buffer_size;
        let state = match stream.fill(data).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to send data. Error = {:?}", e.to_string());
                return;
            }
        };

        // Remove timeout from flush_handler for selected stream if stream state is flushed,
        // do nothing if stream state is partial. Insert a new timeout if initial fill.
        // Warn in case stream flushed stream was not in the queue.
        if max_stream_size > 1 {
            match state {
                StreamStatus::Flushed => self.stream_timeouts.remove(&stream_id),
                StreamStatus::Init(flush_period) => {
                    trace!("Initialized stream buffer for {stream_id}");
                    self.stream_timeouts.insert(&stream_id, flush_period);
                }
                StreamStatus::Partial(_l) => {}
            }
        }
    }

    /// Flush all streams, use on bridge shutdown
    pub async fn flush_all(&mut self) {
        for (stream_name, stream) in self.map.iter_mut() {
            match stream.flush().await {
                Err(e) => error!("Couldn't flush stream = {stream_name}; Error = {e}"),
                _ => info!("Flushed stream = {stream_name}"),
            }
        }
    }

    // Flush stream/partitions that timeout
    pub async fn flush_stream(&mut self, stream: &str) -> Result<(), stream::Error> {
        let stream = self.map.get_mut(stream).unwrap();
        stream.flush().await?;
        Ok(())
    }

    // Enable actual metrics timers when there is data. This method is called every minute by the bridge
    pub fn check_and_flush_metrics(
        &mut self,
    ) -> Result<(), Box<flume::TrySendError<StreamMetrics>>> {
        for (buffer_name, data) in self.map.iter_mut() {
            let metrics = data.metrics.clone();

            // Initialize metrics timeouts when force flush sees data counts
            if metrics.points() > 0 {
                info!(
                    "{:>20}: points = {:<5} batches = {:<5} latency = {}",
                    buffer_name, metrics.points, metrics.batches, metrics.average_batch_latency
                );
                self.metrics_tx.try_send(metrics)?;
                data.metrics.prepare_next();
            }
        }

        Ok(())
    }
}
