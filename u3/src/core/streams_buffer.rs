use crate::config::StreamConfig;
use crate::utils::delaymap::DelayMap;
use crate::{CONFIG, DataRow, PublishItem};
use flume::{Receiver, Sender};
use log::error;
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;

pub struct StreamsBufferHandler {
    data_rx: Receiver<DataRow>,
    buffers_batch_tx: Sender<(String, Vec<PublishItem>)>,
    buffers: HashMap<String, (Vec<PublishItem>, StreamConfig)>,
    timeouts: DelayMap<String>,
}

impl StreamsBufferHandler {
    pub fn new(
        data_rx: Receiver<DataRow>,
        buffers_batch_tx: Sender<(String, Vec<PublishItem>)>,
    ) -> Self {
        Self { data_rx, buffers_batch_tx, buffers: HashMap::new(), timeouts: DelayMap::new() }
    }

    pub async fn run(mut self) {
        let max_dynamic_streams_count = CONFIG.with(|c| {
            for (name, cfg) in c.cfg.streams.iter() {
                self.buffers
                    .insert(name.clone(), (Vec::with_capacity(cfg.buffer_size), cfg.clone()));
            }
            c.cfg.max_dynamic_streams_count as usize
        });
        let declared_streams_count = self.buffers.len();
        loop {
            select! {
                Ok(row) = self.data_rx.recv_async() => {
                    match self.buffers.get_mut(&row.stream) {
                        Some((buf, cfg)) => {
                            buf.push(row.data);
                            if buf.len() >= cfg.buffer_size {
                                let _ = self.buffers_batch_tx.send_async((row.stream, buf.drain(..).collect())).await;
                            } else if buf.len() == 1 {
                                self.timeouts.insert(&row.stream, Duration::from_secs(cfg.flush_interval));
                            }
                        }
                        None => {
                            if self.buffers.len() - declared_streams_count >= max_dynamic_streams_count {
                                error!("too many dynamic streams, ignoring data for stream({})", row.stream);
                                continue;
                            } else {
                                let cfg = StreamConfig::default();
                                let mut buf = Vec::with_capacity(cfg.buffer_size);
                                buf.push(row.data);
                                self.timeouts.insert(&row.stream, Duration::from_secs(cfg.flush_interval));
                                self.buffers.insert(row.stream.clone(), (buf, cfg));
                            }
                        }
                    }
                }
                Some(stream_name) = self.timeouts.next(), if self.timeouts.has_pending() => {
                    let data = self.buffers.get_mut(&stream_name).unwrap().0.drain(..).collect();
                    let _ = self.buffers_batch_tx.send_async((stream_name, data)).await;
                }
                else => break
            }
        }
    }
}

impl Drop for StreamsBufferHandler {
    fn drop(&mut self) {
        for (stream_name, (buf, _)) in self.buffers.drain() {
            if self.buffers_batch_tx.send((stream_name, buf)).is_err() {
                error!("couldn't flush stream buffers during shutdown");
            }
        }
    }
}
