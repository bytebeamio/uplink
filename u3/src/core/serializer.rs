use crate::config::StreamConfig;
use crate::core::storage;
use crate::core::storage::{Storage, StorageEnum, StorageWriteError};
use crate::{DataRow, PublishItem, AppContext};
use flume::{Receiver, SendError, Sender};
use log::error;
use lz4_flex::frame::FrameEncoder;
use replace_with::replace_with_or_abort;
use rumqttc::{AsyncClient, Publish, QoS, Request};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use tokio::select;

pub struct SerializerStorageHandler {
    context: Arc<AppContext>,
    data_tx: Sender<DataRow>,
    buffers_batch_rx: Receiver<(String, Vec<PublishItem>)>,
    topic_prefix: String,
    mqtt_client: Sender<Publish>,
    storages: HashMap<String, StorageState>,
    live_data_clock: usize,
    current_publish: Option<(String, Publish)>,
}

struct StorageState {
    storage: StorageEnum,
    stream_config: StreamConfig,
    live_data: Option<Publish>,
    live_data_pushed_at: usize,
}

impl SerializerStorageHandler {
    pub fn new(
        context: Arc<AppContext>,
        data_tx: Sender<DataRow>,
        buffers_batch_rx: Receiver<(String, Vec<PublishItem>)>,
        mqtt_client: Sender<Publish>,
    ) -> Self {
        let (topic_prefix, storages) = {
            let topic_prefix =
                format!("/tenants/{}/devices/{}", context.auth.project_id, context.auth.device_id);
            let storages = context
                .cfg
                .streams
                .iter()
                .map(|(name, cfg)| {
                    (
                        name.clone(),
                        StorageState {
                            storage: create_storage_for_stream(&context, name, cfg),
                            stream_config: cfg.clone(),
                            live_data: None,
                            live_data_pushed_at: 0,
                        },
                    )
                })
                .collect();
            (topic_prefix, storages)
        };
        Self {
            context: context,
            data_tx,
            buffers_batch_rx,
            topic_prefix,
            mqtt_client,
            storages,
            live_data_clock: 0,
            current_publish: None,
        }
    }

    pub async fn run(mut self) {
        let mqtt_client = self.mqtt_client.clone();
        self.current_publish = self.get_next_publish();
        let mut current_publish_task = self
            .current_publish
            .clone()
            .map(|(_, publish)| mqtt_client.send_async(publish));
        loop {
            select! {
                Ok(buf) = self.buffers_batch_rx.recv_async() => {
                    self.write_buffer_to_storage(buf);
                    if current_publish_task.is_none() {
                        self.current_publish = self.get_next_publish();
                        current_publish_task = self.current_publish.clone()
                            .map(|(_, publish)| mqtt_client.send_async(publish));
                    }
                }
                res = async { current_publish_task.as_mut().unwrap().await }, if current_publish_task.is_some() => {
                    match res {
                        Ok(_) => {
                            self.current_publish = self.get_next_publish();
                            current_publish_task = self.current_publish.clone()
                                .map(|(_, publish)| mqtt_client.send_async(publish));
                        }
                        Err(_) => {
                            current_publish_task = self.current_publish.clone()
                                .map(|(_, publish)| mqtt_client.send_async(publish));
                        }
                    }
                }
                else => break
            }
        }
    }

    fn write_buffer_to_storage(&mut self, (stream_name, data): (String, Vec<PublishItem>)) {
        let compress = self
            .storages
            .get(&stream_name)
            .map(|storage| storage.stream_config.compress)
            .unwrap_or(false);
        let publish = create_publish(&stream_name, &data, &self.topic_prefix, compress);
        self.write_publish_to_storage(stream_name, publish);
    }

    fn write_publish_to_storage(&mut self, stream_name: String, publish: Publish) {
        if !self.storages.contains_key(&stream_name) {
            let stream_config = StreamConfig::default();
            self.storages.insert(
                stream_name.to_owned(),
                StorageState {
                    storage: create_storage_for_stream(&self.context, &stream_name, &stream_config),
                    stream_config,
                    live_data: None,
                    live_data_pushed_at: self.live_data_clock,
                },
            );
        }
        let state = self.storages.get_mut(&stream_name).unwrap();
        let mut publish_to_write = Some(publish);
        std::mem::swap(&mut publish_to_write, &mut state.live_data);
        if let Some(publish) = publish_to_write {
            match state.storage.write_packet(publish) {
                Ok(_) => {}
                Err(StorageWriteError::FileSystemError(e)) => {
                    log::error!(
                        "Encountered file system error when reading packet for stream({}): {e}, falling back to in memory persistence",
                        state.storage.name()
                    );
                    replace_with_or_abort(&mut state.storage, |s| s.to_in_memory());
                }
                Err(StorageWriteError::InvalidPacket(e)) => {
                    log::error!(
                        "Found invalid packet when writing to storage for stream({stream_name}): {e:?}"
                    );
                }
            };
        }
    }

    fn get_next_publish(&mut self) -> Option<(String, Publish)> {
        if let Some((name, state)) = self
            .storages
            .iter_mut()
            .filter(|(_, state)| state.live_data.is_some())
            .min_by_key(|(_, state)| state.live_data_pushed_at)
        {
            self.live_data_clock += 1;
            state.live_data_pushed_at = self.live_data_clock;
            return Some((name.clone(), state.live_data.take().unwrap()));
        }
        for (name, storage) in self.storages.iter_mut() {
            let storage = &mut storage.storage;
            match storage.read_packet() {
                Ok(packet) => {
                    if packet.topic.starts_with(&self.topic_prefix) {
                        return Some((name.clone(), packet));
                    } else {
                        log::warn!("found data for wrong tenant in persistence!: {}", packet.topic);
                        continue;
                    }
                }
                Err(storage::StorageReadError::Empty) => {
                    continue;
                }
                Err(storage::StorageReadError::FileSystemError(e)) => {
                    log::error!(
                        "Encountered file system error when reading packet for stream({}): {e}, falling back to in memory persistence",
                        storage.name()
                    );
                    replace_with_or_abort(storage, |s| s.to_in_memory());
                }
                Err(storage::StorageReadError::InvalidPacket(e)) => {
                    log::error!(
                        "Found invalid packet when reading from storage for stream({}): {e:?}",
                        storage.name()
                    );
                }
                Err(storage::StorageReadError::UnsupportedPacketType) => {
                    log::error!(
                        "Found unsupported packet type when reading from storage for stream({})",
                        storage.name()
                    );
                }
            }
        }
        None
    }
}

impl Drop for SerializerStorageHandler {
    fn drop(&mut self) {
        if let Some((name, publish)) = self.current_publish.take() {
            self.write_publish_to_storage(name, publish);
        }
        while let Ok(buf) = self.buffers_batch_rx.recv() {
            self.write_buffer_to_storage(buf);
        }
        for (name, storage) in self.storages.iter_mut() {
            if let Some(publish) = storage.live_data.take() {
                let _ = storage.storage.write_packet(publish);
            }
            if let Err(e) = storage.storage.flush() {
                error!("couldn't flush storage for stream({name:?}) : {e:?}");
            }
        }
    }
}

fn create_storage_for_stream(ctx: &AppContext, name: &str, config: &StreamConfig) -> StorageEnum {
    let (max_packet_size, directory) = (ctx.cfg.mqtt.max_packet_size, ctx.cfg.persistence_path.join(name));
    if config.persistence.max_file_count == 0 {
        StorageEnum::InMemory(storage::InMemoryStorage::new(
            name,
            config.persistence.max_file_size,
            max_packet_size,
        ))
    } else {
        match storage::DirectoryStorage::new(
            directory,
            config.persistence.max_file_size,
            config.persistence.max_file_count,
            max_packet_size,
        ) {
            Ok(s) => StorageEnum::Directory(s),
            Err(e) => {
                log::error!(
                    "Failed to initialize disk backed storage for {name} : {e}, falling back to in memory persistence"
                );
                StorageEnum::InMemory(storage::InMemoryStorage::new(
                    name,
                    config.persistence.max_file_size,
                    max_packet_size,
                ))
            }
        }
    }
}

// TODO: we should probably move it off of tokio context because it'll do compression
fn create_publish(
    stream_name: &str,
    data: &[PublishItem],
    topic_prefix: &str,
    compress: bool,
) -> Publish {
    let point_count = data.len();
    log::trace!("Data received on stream: {stream_name}; message count = {point_count}");

    let topic = if stream_name == "action_status" {
        format!("{topic_prefix}/action/status")
    } else {
        format!(
            "{topic_prefix}/events/{stream_name}/jsonarray{}",
            if compress { "/lz4" } else { "" }
        )
    };

    let serialization_start = Instant::now();
    let mut payload = serde_json::to_vec(data).unwrap();
    let serialization_time = serialization_start.elapsed();
    // metrics.add_serialization_time(serialization_time);

    let data_size = payload.len();
    let mut compressed_data_size = None;

    if compress {
        let compression_start = Instant::now();
        lz4_compress(&mut payload);
        let compression_time = compression_start.elapsed();
        // metrics.add_compression_time(compression_time);

        compressed_data_size = Some(payload.len());
    }

    // metrics.add_serialized_sizes(data_size, compressed_data_size);

    let mut result = Publish::new(topic, QoS::AtLeastOnce, payload);
    result.pkid = 1;
    result
}

fn lz4_compress(payload: &mut Vec<u8>) {
    let mut compressor = FrameEncoder::new(vec![]);
    // Below functions fail in case of IO errors
    // so these unwraps are safe because we are doing in memory compression
    compressor.write_all(payload).unwrap();
    *payload = compressor.finish().unwrap();
}
