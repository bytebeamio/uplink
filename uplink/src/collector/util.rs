use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use flume::Sender;
use tokio_stream::StreamExt;
use tokio_util::time::{delay_queue::Key, DelayQueue};
use tracing::{error, info, warn};

use crate::base::StreamStatus;
use crate::{Config, Package, Payload, Stream};

/// A map to store and retrieve delays from a DelayQueue.
pub struct DelayMap<T> {
    queue: DelayQueue<T>,
    map: HashMap<T, Key>,
}

impl<T: Eq + Hash + Clone + Display> DelayMap<T> {
    pub fn new() -> Self {
        Self { queue: DelayQueue::new(), map: HashMap::new() }
    }

    // Removes timeout if it exists, else returns false.
    pub fn remove(&mut self, item: &T) {
        match self.map.remove(item) {
            Some(key) => {
                self.queue.remove(&key);
            }
            None => warn!("Timeout couldn't be removed from DelayMap: {}", item),
        }
    }

    // Insert new timeout.
    pub fn insert(&mut self, item: &T, period: Duration) {
        let key = self.queue.insert(item.clone(), period);
        if self.map.insert(item.to_owned(), key).is_some() {
            warn!("Timeout might have already been in DelayMap: {}", item);
        }
    }

    // Remove a key from map if it has timedout.
    pub async fn next(&mut self) -> Option<T> {
        if let Some(item) = self.queue.next().await {
            self.map.remove(item.get_ref());
            return Some(item.into_inner());
        }

        None
    }

    // Check if queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

pub struct Streams {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    map: HashMap<String, Stream<Payload>>,
    flush_handler: DelayMap<String>,
}

impl Streams {
    pub fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>) -> Self {
        let mut map = HashMap::new();
        for (name, stream) in &config.streams {
            let stream = Stream::with_config(
                name,
                &config.project_id,
                &config.device_id,
                stream,
                data_tx.clone(),
            );
            map.insert(name.to_owned(), stream);
        }

        let flush_handler = DelayMap::new();

        Self { config, data_tx, map, flush_handler }
    }

    pub async fn forward(&mut self, data: Payload) {
        let stream = match self.map.get_mut(&data.stream) {
            Some(partition) => partition,
            None => {
                if self.map.keys().len() > 20 {
                    error!("Failed to create {:?} stream. More than max 20 streams", data.stream);
                    return;
                }

                let stream = Stream::dynamic(
                    &data.stream,
                    &self.config.project_id,
                    &self.config.device_id,
                    self.data_tx.clone(),
                );
                self.map.entry(data.stream.to_owned()).or_insert(stream)
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
                StreamStatus::Flushed(name) => self.flush_handler.remove(name),
                StreamStatus::Init(name, flush_period) => {
                    self.flush_handler.insert(name, flush_period)
                }
                StreamStatus::Partial(l) => {
                    info!("Flushing stream {} with {} elements", stream.name, l);
                }
            }
        }
    }

    pub fn is_flushable(&self) -> bool {
        !self.flush_handler.is_empty()
    }

    // Flush stream/partitions that timeout
    pub async fn flush(&mut self) {
        let stream = self.flush_handler.next().await.unwrap();
        let stream = self.map.get_mut(&stream).unwrap();
        if let Err(e) = stream.flush().await {
            error!("Failed to send data. Error = {:?}", e.to_string());
        }
    }
}
