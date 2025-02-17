use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use flume::{Receiver, RecvError, Sender};
use log::{debug, error, info, warn};
use tokio::{select, time::interval};
use crate::base::bridge::delaymap::DelayMap;
use crate::uplink_config::UplinkConfig;

use super::{Payload, StreamMetrics};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Receiver error {0}")]
    Recv(#[from] RecvError),
}

pub struct DataBridge {
    /// All configuration
    config: Arc<UplinkConfig>,
    /// channels on which messages can be sent to be queued for upload
    /// this task will stop if the first channel closes, but will ignore closure of weak channel
    pub messages_rx: Receiver<Payload>,
    pub weak_messages_rx: Receiver<Payload>,
    /// channel to serializer
    buffers_tx: Sender<Vec<Payload>>,
    streams: HashMap<String, (Vec<Payload>, StreamMetrics)>,
    stream_timeouts: DelayMap<String>,
}

impl DataBridge {
    pub fn new(
        config: Arc<UplinkConfig>,
        messages_rx: Receiver<Payload>,
        weak_messages_rx: Receiver<Payload>,
        buffers_tx: Sender<Vec<Payload>>,
    ) -> Self {
        Self {
            config,
            messages_rx,
            weak_messages_rx,
            buffers_tx,
            streams: HashMap::new(),
            stream_timeouts: DelayMap::new(),
        }
    }

    pub async fn start(&mut self) {
        let mut metrics_timeout = interval(self.config.app_config.stream_metrics.timeout);

        loop {
            select! {
                result = self.messages_rx.recv_async() => {
                    match result {
                        Ok(message) => {
                            self.add(message).await;
                        }
                        Err(_) => {
                            warn!("stopping message buffer task");
                            self.flush_everything().await;
                            break;
                        }
                    }
                }
                result = self.weak_messages_rx.recv_async() => {
                    if let Ok(message) = result {
                        self.add(message).await;
                    }
                }
                Some(timedout_stream) = self.stream_timeouts.next(), if !self.stream_timeouts.is_empty() => {
                    if let Some((buffer, metrics)) = self.streams.get_mut(&timedout_stream) {
                        let cap = buffer.capacity();
                        if buffer.len() > 0 {
                            let buffer_to_send = std::mem::replace(buffer, Vec::with_capacity(cap));
                            let _ = self.buffers_tx.send(buffer_to_send);
                            metrics.add_batch();
                        }
                    } else {
                        error!("stream({timedout_stream}) has a timeout but doesn't have a stream buffer!");
                    }
                }
                _ = metrics_timeout.tick() => {
                    self.generate_metrics().await;
                }
            }
        }
    }

    async fn flush_everything(&mut self) {
        for (_, (buffer, _)) in self.streams.drain() {
            if buffer.len() > 0 {
                let _ = self.buffers_tx.send(buffer);
            }
        }
    }

    async fn add(&mut self, message: Payload) {
        let sk = self.config.app_config.streams.get(&message.stream);
        let batch_size = sk
            .map(|sk| sk.batch_size)
            .unwrap_or(128);
        let flush_period = sk
            .map(|sk| sk.flush_period)
            .unwrap_or(Duration::from_secs(60));
        if !self.streams.contains_key(&message.stream) {
            if self.streams.len() >= self.config.app_config.max_stream_count {
                error!("too many streams! limit is set to {}, cannot process data for stream({})", self.config.app_config.max_stream_count, message.stream);
                return;
            }
            self.streams.insert(
                message.stream.to_owned(),
                (Vec::with_capacity(batch_size), StreamMetrics::new(message.stream.to_owned(), batch_size))
            );
        }

        let (buffer, metrics) = self.streams.get_mut(&message.stream).unwrap();
        metrics.add_point();
        if buffer.is_empty() && batch_size != 1 {
            if !self.stream_timeouts.contains(&message.stream) {
                self.stream_timeouts.insert(message.stream.to_owned(), flush_period);
            }
        }
        buffer.push(message);
        // TODO: ensure that batch_size is always greater than 0
        if buffer.len() >= batch_size {
            let buffer_to_send = std::mem::replace(buffer, Vec::with_capacity(buffer.capacity()));
            let _ = self.buffers_tx.send(buffer_to_send);
        }
    }

    async fn generate_metrics(&mut self) {
        let too_many_streams = self.streams.len() > 10;
        for (stream_name, (_, metrics)) in self.streams.iter_mut() {
            if metrics.points > 0 {
                let message = format!("{stream_name:>20}: points = {:<5} batches = {:<5} latency = {}", metrics.points, metrics.batches, metrics.average_batch_latency);
                if too_many_streams {
                    debug!("{}", message);
                } else {
                    info!("{}", message);
                }
                if !STREAMS_EXCEMPT_FROM_METRICS.contains(&stream_name.as_str()) {
                    self.add(metrics.to_payload()).await;
                }
                metrics.prepare_next();
            }
        }
    }
}

const STREAMS_EXCEMPT_FROM_METRICS: [&'static str; 6] = [
    "uplink_disk_stats",
    "uplink_network_stats",
    "uplink_processor_stats",
    "uplink_process_stats",
    "uplink_component_stats",
    "uplink_system_stats",
];