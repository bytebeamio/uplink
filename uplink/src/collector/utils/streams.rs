use std::collections::HashMap;
use std::sync::Arc;

use flume::Sender;
use log::{error, info};

use crate::base::stream::{self, StreamStatus};
use crate::{Config, Package, Payload, Stream};

use super::delaymap::DelayMap;

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

    pub async fn data_timeout(&mut self) -> String {
        self.flush_handler.next().await.unwrap()
    }

    // Flush stream/partitions that timeout
    pub async fn flush(&mut self, stream: &str) -> Result<(), stream::Error> {
        let stream = self.map.get_mut(stream).unwrap();
        stream.flush().await?;
        Ok(())
    }
}
