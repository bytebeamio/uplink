use std::collections::HashMap;
use std::sync::Arc;

use flume::Sender;
use log::{error, info, trace};

use super::stream::{self, StreamStatus, MAX_BUFFER_SIZE};
use super::{Point, StreamMetrics};
use crate::base::StreamConfig;
use crate::{Config, Package, Stream};

use super::delaymap::DelayMap;

pub struct Streams<T> {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    metrics_tx: Sender<StreamMetrics>,
    map: HashMap<String, Stream<T>>,
    pub stream_timeouts: DelayMap<String>,
    pub max_buf_size: usize,
    topic_template: String,
}

impl<T: Point> Streams<T> {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
        topic_template: String,
    ) -> Self {
        Self {
            config,
            data_tx,
            metrics_tx,
            map: HashMap::new(),
            stream_timeouts: DelayMap::new(),
            topic_template,
            max_buf_size: MAX_BUFFER_SIZE,
        }
    }

    pub fn config_streams(&mut self, streams_config: HashMap<String, StreamConfig>) {
        for (name, stream) in streams_config {
            let stream = Stream::with_config(&name, &stream, self.data_tx.clone());
            self.map.insert(name.to_owned(), stream);
        }
    }

    pub async fn forward(&mut self, data: T) {
        let stream_name = data.stream_name().to_string();

        let stream = match self.map.get_mut(&stream_name) {
            Some(partition) => partition,
            None => {
                if self.config.simulator.is_none() && self.map.keys().len() > 20 {
                    error!("Failed to create {:?} stream. More than max 20 streams", stream_name);
                    return;
                }

                let stream = Stream::dynamic(
                    &stream_name,
                    &self.config.project_id,
                    &self.config.device_id,
                    self.max_buf_size,
                    self.data_tx.clone(),
                    &self.topic_template,
                );

                self.map.entry(stream_name.to_owned()).or_insert(stream)
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
                StreamStatus::Flushed => self.stream_timeouts.remove(&stream_name),
                StreamStatus::Init(flush_period) => {
                    trace!("Initialized stream buffer for {stream_name}");
                    self.stream_timeouts.insert(&stream_name, flush_period);
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
