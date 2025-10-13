use crate::config::StreamConfig;
use crate::core::storage;
use crate::core::storage::{Storage, StorageEnum, StorageWriteError};
use crate::utils::delaymap::DelayMap;
use crate::{AppContext, DataRow, PublishItem};
use flume::r#async::SendFut;
use flume::{Receiver, SendError, Sender};
use log::{debug, error, info};
use lz4_flex::frame::FrameEncoder;
use replace_with::replace_with_or_abort;
use rumqttc::{AsyncClient, Publish, QoS, Request};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;

pub struct SerializerStorageHandler {
    context: SerializerConfig,

    // handles stream buffers and timeouts along with action_status
    data_rx: Receiver<DataRow>,
    buffers: HashMap<String, BufferState>,
    timeouts: DelayMap<String>,
    dynamic_streams_count: usize,

    // handler storage and persistence
    mqtt_client: Sender<Publish>,
    storages: HashMap<String, StorageState>,
    live_data_clock: usize,
    current_publish: Option<(String, Publish)>,
}

struct BufferState {
    data: Vec<PublishItem>,
    stream_config: StreamConfig,
    json_payload_size: usize,
}

pub struct SerializerConfig {
    pub streams: HashMap<String, StreamConfig>,
    pub mqtt_max_packet_size: usize,
    pub max_dynamic_streams_count: usize,
    pub persistence_path: Option<PathBuf>,
}

struct StorageState {
    storage: StorageEnum,
    stream_config: StreamConfig,
    live_data: Option<Publish>,
    live_data_pushed_at: usize,
}

impl SerializerStorageHandler {
    /// * Receive data from `data_rx` and `metrics_rx`
    /// * Buffer and timeout as per the stream config
    /// * Save the buffers to disk as per the persistence config
    /// * Upload to cloud using mqtt_client
    /// * Flush in memory data to disk on shutdown
    ///
    /// `metrics_rx` is lower priority than `data_rx`
    /// Data from `metrics_rx` isn't saved on shutdown
    pub fn new(
        context: SerializerConfig,
        data_rx: Receiver<DataRow>,
        mqtt_client: Sender<Publish>,
    ) -> Self {
        let mut buffers = HashMap::new();
        for (name, cfg) in context.streams.iter() {
            buffers.insert(name.clone(), BufferState {
                data: Vec::with_capacity(cfg.buffer_size),
                stream_config: cfg.clone(),
                json_payload_size: 2,
            });
        }

        let storages = context
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
        Self {
            context,

            data_rx,
            buffers,
            timeouts: DelayMap::new(),
            dynamic_streams_count: 0,

            mqtt_client,
            storages,
            live_data_clock: 0,
            current_publish: None,
        }
    }

    pub async fn run(mut self) {
        let mqtt_client = self.mqtt_client.clone();
        let mut current_publish_task = None;
        macro_rules! retry_current_publish {
            () => {{
                current_publish_task = self
                    .current_publish
                    .clone()
                    .map(|(_, publish)| Box::pin(mqtt_client.send_async(publish)));
            }};
        }
        macro_rules! queue_next_publish {
            () => {{
                self.current_publish = self.get_next_publish();
                retry_current_publish!();
            }};
        }
        queue_next_publish!();

        loop {
            select! {
                // first two tasks read data points, and move them to storage according to stream buffer size and timeout config
                Some(row) = async {
                    select! {
                        Ok(row) = self.data_rx.recv_async() => Some(row),
                        else => None
                    }
                } => if let Some(filled_buffer) = self.buffer_row(row) {
                    debug!("flushing {}", &filled_buffer.0);
                    self.write_buffer_to_storage(filled_buffer);
                    if current_publish_task.is_none() {
                        queue_next_publish!();
                    }
                },
                Some(stream_name) = self.timeouts.next(), if self.timeouts.has_pending() => {
                    debug!("flushing {stream_name} because of timeout");
                    let stream_state = self.buffers.get_mut(&stream_name).unwrap();
                    let data = std::mem::take(&mut stream_state.data);
                    stream_state.json_payload_size = 2;
                    self.write_buffer_to_storage((stream_name, data));
                    if current_publish_task.is_none() {
                        queue_next_publish!();
                    }
                }

                // moves data from storage to mqtt client
                res = async { current_publish_task.as_mut().unwrap().await }, if current_publish_task.is_some() => {
                    match res {
                        Ok(_) => queue_next_publish!(),
                        Err(_) => retry_current_publish!(),
                    }
                }
                else => break
            }
        }
    }

    fn buffer_row(&mut self, row: DataRow) -> Option<(String, Vec<PublishItem>)> {
        match self.buffers.get_mut(&row.stream) {
            Some(BufferState { data, stream_config, json_payload_size }) => {
                let row_size = serde_json::to_string(&row.data).unwrap().len() + if data.len() == 0 { 0 } else { 1 };
                let new_size = *json_payload_size + row_size;
                let mqtt_max_packet_size = if stream_config.compress {
                    self.context.mqtt_max_packet_size
                } else {
                    self.context.mqtt_max_packet_size * 5 / 2
                };
                if new_size > mqtt_max_packet_size {
                    self.timeouts.remove(&row.stream);
                    let data = std::mem::replace(data, Vec::with_capacity(stream_config.buffer_size));
                    *json_payload_size = 2;
                    return Some((row.stream, data));
                } else {
                    *json_payload_size = new_size;
                }

                data.push(row.data);
                if data.len() > stream_config.buffer_size {
                    self.timeouts.remove(&row.stream);
                    let data = std::mem::replace(data, Vec::with_capacity(stream_config.buffer_size));
                    *json_payload_size = 0;
                    return Some((row.stream, data));
                } else if data.len() == 1 {
                    self.timeouts.insert(&row.stream, Duration::from_secs(stream_config.flush_interval));
                }
            }
            None => {
                if self.dynamic_streams_count >= self.context.max_dynamic_streams_count {
                    error!("too many dynamic streams, ignoring data for stream({})", row.stream);
                } else {
                    let stream_config = StreamConfig::default();
                    let mut data = Vec::with_capacity(stream_config.buffer_size);
                    data.push(row.data);
                    self.timeouts.insert(&row.stream, Duration::from_secs(stream_config.flush_interval));
                    self.buffers.insert(row.stream.clone(), BufferState { data, stream_config, json_payload_size: 2 });
                    self.dynamic_streams_count += 1;
                }
            }
        }
        None
    }

    // TODO: heavy functions
    // called synchronously in the serializer main loop
    // might be slow for big batch size, needs to be benchmarked
    // does compression and disk io
    // if these block on disk, serializer will stop reading data points
    fn write_buffer_to_storage(&mut self, (stream_name, data): (String, Vec<PublishItem>)) {
        let compress = self
            .storages
            .get(&stream_name)
            .map(|storage| storage.stream_config.compress)
            .unwrap_or(false);
        let publish = create_publish(&stream_name, &data, compress);
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
                    return Some((name.clone(), packet));
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
        // read all inflight data from all collectors and save them
        while let Ok(row) = self.data_rx.recv_timeout(Duration::from_millis(500)) {
            if let Some(buffer) = self.buffer_row(row) {
                self.write_buffer_to_storage(buffer);
            }
        }
        // write any unflushed buffers to storage
        for (stream_name, buf) in std::mem::take(&mut self.buffers) {
            if !buf.data.is_empty() {
                self.write_buffer_to_storage((stream_name, buf.data));
            }
        }
        // write inflight publish to storage
        if let Some((name, publish)) = self.current_publish.take() {
            self.write_publish_to_storage(name, publish);
        }
        // flush all the storages to disk
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

fn create_storage_for_stream(
    ctx: &SerializerConfig,
    name: &str,
    config: &StreamConfig,
) -> StorageEnum {
    if config.persistence.max_file_count == 0 {
        StorageEnum::InMemory(storage::InMemoryStorage::new(
            name,
            config.persistence.max_file_size,
            usize::MAX,
        ))
    } else {
        match storage::DirectoryStorage::new(
            ctx.persistence_path.as_ref().unwrap().join(name),
            config.persistence.max_file_size,
            config.persistence.max_file_count,
            usize::MAX,
        ) {
            Ok(s) => StorageEnum::Directory(s),
            Err(e) => {
                log::error!(
                    "Failed to initialize disk backed storage for {name} : {e}, falling back to in memory persistence"
                );
                StorageEnum::InMemory(storage::InMemoryStorage::new(
                    name,
                    config.persistence.max_file_size,
                    usize::MAX,
                ))
            }
        }
    }
}

fn create_publish(stream_name: &str, data: &[PublishItem], compress: bool) -> Publish {
    let point_count = data.len();
    log::trace!("Data received on stream: {stream_name}; message count = {point_count}");

    let topic = if stream_name == "action_status" {
        "/action/status".to_owned()
    } else {
        format!("/events/{stream_name}/jsonarray{}", if compress { "/lz4" } else { "" })
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
    compressor.write_all(payload).unwrap();
    *payload = compressor.finish().unwrap();
}
