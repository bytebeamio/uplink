use crate::config::{HttpCreds, StreamConfig};
use crate::core::storage;
use crate::core::storage::{DiskQueue, Publish};
use crate::utils::array_map::ArrayMap;
use crate::utils::delaymap::DelayMap;
use crate::{AppContext, DataRow, PublishItem};
use flume::r#async::SendFut;
use flume::{Receiver, SendError, Sender};
use log::{debug, error, info};
use lz4_flex::frame::FrameEncoder;
use replace_with::replace_with_or_abort;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use futures::stream::{FlatMapUnordered, FuturesUnordered, StreamExt};
use reqwest::{Client, Error, Response};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use tokio::select;
use crate::core::actions::Action;
use crate::utils::ac::AC;

pub struct SerializerStorageHandler {
    context: SerializerConfig,

    // handles stream buffers and timeouts along with action_status
    data_rx: Receiver<DataRow>,
    buffers: HashMap<String, BufferState>,
    timeouts: DelayMap<String>,
    dynamic_streams_count: usize,

    // handler storage and persistence
    storages: ArrayMap<String, StorageState>,
    /// incremented whenever we do a publish,
    live_data_clock: usize,
    active_publishes: HashMap<u32, (String, Publish)>,
}

struct BufferState {
    data: Vec<PublishItem>,
    stream_config: StreamConfig,
    json_payload_size: usize,
}

pub struct SerializerConfig {
    pub connection_manager: Arc<ConnectionManager>,
    pub streams: HashMap<String, StreamConfig>,
    pub max_packet_size: usize,
    pub max_dynamic_streams_count: usize,
    pub persistence_path: Option<PathBuf>,
}

struct StorageState {
    storage: DiskQueue,
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
    pub fn new(context: SerializerConfig, data_rx: Receiver<DataRow>) -> Self {
        let mut buffers = HashMap::new();
        for (name, cfg) in context.streams.iter() {
            buffers.insert(
                name.clone(),
                BufferState {
                    data: Vec::with_capacity(cfg.buffer_size),
                    stream_config: cfg.clone(),
                    json_payload_size: 2,
                },
            );
        }

        let mut storages = ArrayMap::<String, StorageState>::new(|a, b| {
            b.stream_config.priority.cmp(&a.stream_config.priority)
        });
        for (name, cfg) in context.streams.iter() {
            storages.insert(
                name.clone(),
                StorageState {
                    storage: create_storage_for_stream(&context, name, cfg),
                    stream_config: cfg.clone(),
                    live_data: None,
                    live_data_pushed_at: 0,
                },
            );
        }
        Self {
            context,

            data_rx,
            buffers,
            timeouts: DelayMap::new(),
            dynamic_streams_count: 0,

            storages,
            live_data_clock: 0,
            active_publishes: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut current_publish_tasks = FuturesUnordered::<Pin<Box<dyn Future<Output=(u32, bool)> + Send>>>::new();
        macro_rules! try_publish_with_id {
            ($id:expr) => {{
                let cm = self.context.connection_manager.clone();
                let (stream_name, publish) = self.active_publishes.get(&$id).unwrap().clone();
                current_publish_tasks.push(Box::pin(async move {
                    let success = cm.upload(&stream_name, publish).await;
                    if !success {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                    ($id, success)
                }));
            }};
        }
        macro_rules! queue_next_publish {
            () => {{
                let id = rand::random::<u32>();
                if let Some(next_publish) = self.get_next_publish() {
                    self.active_publishes.insert(id, next_publish.clone());
                    try_publish_with_id!(id);
                }
            }};
        }
        queue_next_publish!();

        let mut metrics_timer = tokio::time::interval(Duration::from_secs(10));
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
                    if current_publish_tasks.len() < 4 {
                        queue_next_publish!();
                    }
                },
                Some(stream_name) = self.timeouts.next(), if self.timeouts.has_pending() => {
                    debug!("flushing {stream_name} because of timeout");
                    let stream_state = self.buffers.get_mut(&stream_name).unwrap();
                    let data = std::mem::take(&mut stream_state.data);
                    stream_state.json_payload_size = 2;
                    self.write_buffer_to_storage((stream_name, data));
                    if current_publish_tasks.len() < 4 {
                        queue_next_publish!();
                    }
                }

                Some((id, ok)) = current_publish_tasks.next(), if !current_publish_tasks.is_empty() => {
                    if ok {
                        self.active_publishes.remove(&id);
                        queue_next_publish!();
                    } else {
                        try_publish_with_id!(id);
                    }
                }
                _ = metrics_timer.tick() => {
                    for (name, storage) in self.storages.iter_mut() {
                        let m = storage.storage.metrics();
                    }
                    // serializer metrics:
                    // * memory usage
                    // * disk usage
                    // * disk percentage
                    // * live messages pushed
                    // * storage messages pushed
                    // * net compression time
                    // * net serialization time
                    // stream metrics (for each stream):
                    // * number of messages
                    // * serialization/compression time/size
                    // *
                    // for individual streams, collect number of messages, serialization time/size, compression size/time, and push as stream metrics
                }
            }
        }
    }

    fn buffer_row(&mut self, row: DataRow) -> Option<(String, Vec<PublishItem>)> {
        match self.buffers.get_mut(&row.stream) {
            Some(BufferState { data, stream_config, json_payload_size }) => {
                let row_size = serde_json::to_string(&row.data).unwrap().len()
                    + if data.len() == 0 { 0 } else { 1 };
                let new_size = *json_payload_size + row_size;
                let mqtt_max_packet_size = if stream_config.compress {
                    self.context.max_packet_size
                } else {
                    self.context.max_packet_size * 12 / 5
                };
                if new_size > mqtt_max_packet_size {
                    self.timeouts.remove(&row.stream);
                    let data =
                        std::mem::replace(data, Vec::with_capacity(stream_config.buffer_size));
                    *json_payload_size = 2;
                    return Some((row.stream, data));
                } else {
                    *json_payload_size = new_size;
                }

                data.push(row.data);
                if data.len() >= stream_config.buffer_size {
                    self.timeouts.remove(&row.stream);
                    let data =
                        std::mem::replace(data, Vec::with_capacity(stream_config.buffer_size));
                    *json_payload_size = 0;
                    return Some((row.stream, data));
                } else if data.len() == 1 {
                    self.timeouts
                        .insert(&row.stream, Duration::from_secs(stream_config.flush_interval));
                }
            }
            None => {
                if self.dynamic_streams_count >= self.context.max_dynamic_streams_count {
                    error!("too many dynamic streams, ignoring data for stream({})", row.stream);
                } else {
                    let stream_config = StreamConfig::default();
                    let mut data = Vec::with_capacity(stream_config.buffer_size);
                    data.push(row.data);
                    self.timeouts
                        .insert(&row.stream, Duration::from_secs(stream_config.flush_interval));
                    self.buffers.insert(
                        row.stream.clone(),
                        BufferState { data, stream_config, json_payload_size: 2 },
                    );
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
        let publish = create_publish(&data, compress);
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
            state.storage.write_packet(publish)
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
                Err(storage::StorageReadError::InvalidPacket(e)) => {
                    log::error!(
                        "Found invalid packet when reading from storage for stream({name}): {e}"
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
        // write inflight publishes to storage
        for (_, (name, publish)) in std::mem::take(&mut self.active_publishes) {
            self.write_publish_to_storage(name, publish);
        }
        // flush all the storages to disk
        for (_, storage) in self.storages.iter_mut() {
            if let Some(publish) = storage.live_data.take() {
                let _ = storage.storage.write_packet(publish);
            }
            storage.storage.flush();
        }
    }
}

fn create_storage_for_stream(
    ctx: &SerializerConfig,
    name: &str,
    config: &StreamConfig,
) -> DiskQueue {
    DiskQueue::new(ctx.persistence_path.as_ref().unwrap().join(name), config.persistence.clone())
}

pub fn create_publish(data: &[PublishItem], compressed: bool) -> Publish {
    let mut payload = serde_json::to_vec(data).unwrap();
    if compressed {
        lz4_compress(&mut payload);
    }
    Publish { payload, compressed }
}

fn lz4_compress(payload: &mut Vec<u8>) {
    let mut compressor = FrameEncoder::new(vec![]);
    compressor.write_all(payload).unwrap();
    *payload = compressor.finish().unwrap();
}

enum ErrorKind {
    NetworkError(String),
    ServerError(String),

    DnsError
}

pub struct ConnectionManager {
    pub state: Mutex<CMState>,
}

struct CMState {
    pub creds: HttpCreds,
    pub client: Option<Client>,
    pub connected: Option<bool>,
    pub abort_trigger: tokio::sync::broadcast::Sender<()>,
}

impl ConnectionManager {
    pub fn new(creds: HttpCreds) -> Self {
        Self {
            state: Mutex::new(CMState {
                creds,
                client: None,
                connected: None,
                abort_trigger: tokio::sync::broadcast::Sender::new(1)
            })
        }
    }

    pub async fn upload(&self, stream: &str, data: Publish) -> bool {
        self.process_result(self.upload_impl(stream, data).await)
            .is_some()
    }

    pub async fn upload_message(&self, stream: &str, message: PublishItem) {
        loop {
            match self.upload_impl(stream, create_publish(&[message.clone()], false)).await {
                Ok(_) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    pub async fn await_action(&self, ty: &str) -> Action {
        loop {
            match self.process_result(self.await_action_impl(ty).await) {
                Some(Some(r)) => {
                    info!("received action({ty}) : {r:?}");
                    return r
                }
                _ => {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    pub fn create_copy(&self) -> Self {
        Self::new(self.state.lock().unwrap().creds.clone())
    }

    async fn await_action_impl(&self, ty: &str) -> Result<Option<Action>, ErrorKind> {
        let (client, api_url, mut abort_trigger) = {
            let mut state = self.state.lock().unwrap();
            if state.client.is_none() {
                let mut headers = HeaderMap::new();
                headers.insert("content-type", HeaderValue::from_str("application/json").unwrap());
                headers.insert("x-bytebeam-device-identity", HeaderValue::from_str(&state.creds.api_key).unwrap());
                state.client = reqwest::ClientBuilder::new()
                    .use_rustls_tls()
                    .default_headers(headers)
                    .pool_max_idle_per_host(1)
                    .build()
                    .ok();
            }
            let client = {
                match state.client.clone() {
                    Some(c) => c,
                    None => return Err(ErrorKind::DnsError),
                }
            };
            let abort_trigger = state.abort_trigger.subscribe();
            (client, state.creds.api_url.clone(), abort_trigger)
        };
        async move {
            select! {
                r = Self::await_action_standalone(client, api_url, ty.to_owned()) => r,
                _ = abort_trigger.recv() => Ok(None)
            }
        }.await
    }

    async fn await_action_standalone(client: Client, api_url: String, ty: String) -> Result<Option<Action>, ErrorKind> {
        let resp = match client
            .get(format!("{api_url}/v1/available-action/{ty}/await"))
            .send().await {
            Ok(r) => {
                r
            }
            Err(e) => {
                return Err(ErrorKind::NetworkError(format!("{e:?}")));
            }
        };
        let status = resp.status();
        let resp_text = match resp.text().await {
            Ok(r) => r,
            Err(e) => {
                return Err(ErrorKind::NetworkError(format!("{e:?}")));
            }
        };
        #[derive(Deserialize)]
        struct ActionResponse {
            action: Option<Action>,
        }
        if status.is_success() {
            match serde_json::from_str::<ActionResponse>(&resp_text) {
                Ok(r) => return Ok(r.action),
                Err(e) => {
                    return Err(ErrorKind::ServerError(format!("server returned an unexpected response: {resp_text}, {e:?}")));
                }
            }
        } else {
            if status.is_server_error() {
                return Err(ErrorKind::ServerError(format!("server error({status}): {resp_text}")));
            } else {
                log::error!("unexpected error when fetching action({ty}) : ({status}) : ({resp_text})");
                return Ok(None)
            }
        }
    }

    pub fn update_credentials(&self, creds: HttpCreds) {
        let mut state = self.state.lock().unwrap();
        state.creds = creds;
        state.client = None;
        state.connected = None;
        let _ = state.abort_trigger.send(());
    }

    async fn upload_impl(&self, stream: &str, data: Publish) -> Result<(), ErrorKind> {
        let (client, api_url) = {
            let mut state = self.state.lock().unwrap();
            if state.client.is_none() {
                let mut headers = HeaderMap::new();
                headers.insert("content-type", HeaderValue::from_str("application/json").unwrap());
                headers.insert("x-bytebeam-device-identity", HeaderValue::from_str(&state.creds.api_key).unwrap());
                state.client = reqwest::ClientBuilder::new()
                    .use_rustls_tls()
                    .default_headers(headers)
                    .pool_max_idle_per_host(1)
                    .build()
                    .ok();
            }
            let client = {
                match state.client.clone() {
                    Some(c) => c,
                    None => return Err(ErrorKind::DnsError),
                }
            };
            let api_url = state.creds.api_url.clone();
            (client, api_url)
        };
        Self::upload_impl_standalone(client, api_url, stream.to_owned(), data).await
    }

    async fn upload_impl_standalone(client: Client, api_url: String, stream: String, data: Publish) -> Result<(), ErrorKind> {
        let mut req = client.post(format!("{api_url}/v1/streams/{stream}/submit"))
            .body(data.payload);
        if data.compressed {
            req = req.header("content-encoding", "lz4")
        }
        let resp = req.send().await
            .map_err(|e| ErrorKind::NetworkError(format!("{e:?}")))?;
        let status = resp.status();
        if !status.is_success() {
            let message = resp.text().await.unwrap_or(String::new());
            if status.is_server_error() {
                return Err(ErrorKind::ServerError(format!("server error({status}) : {message}")));
            }
            log::error!("server responded with an error when uploading data for stream({stream})!\nresponse:\n{message}");
        }
        Ok(())
    }

    fn process_result<T>(&self, r: Result<T, ErrorKind>) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        match r {
            Ok(r) => {
                if state.connected != Some(true) {
                    state.connected = Some(true);
                    info!("connected to server!");
                }
                Some(r)
            }
            Err(e) => {
                let mut state = self.state.lock().unwrap();
                if state.connected != Some(false) {
                    state.connected = Some(false);
                    match e {
                        ErrorKind::NetworkError(msg) => error!("network error: {msg}"),
                        ErrorKind::ServerError(msg) => error!("server error: {msg}"),
                        ErrorKind::DnsError => error!("dns error!"),
                    }
                }
                None
            }
        }
    }
}