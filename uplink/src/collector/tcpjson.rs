use flume::{Receiver, RecvError, Sender};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, Instant};
use tokio::{select, time};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use tokio_util::time::{delay_queue::Key, DelayQueue};

use std::{collections::HashMap, io, sync::Arc};

use crate::base::actions::{Action, ActionResponse, Error as ActionsError};
use crate::base::{Buffer, Config, Package, Point, Stream};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Receiver error {0}")]
    Recv(#[from] RecvError),
    #[error("Stream done")]
    StreamDone,
    #[error("Lines codec error {0}")]
    Codec(#[from] LinesCodecError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("Download OTA error")]
    Actions(#[from] ActionsError),
    #[error("Couldn't fill stream")]
    Stream(#[from] crate::base::Error),
}

pub struct Bridge {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    actions_rx: Receiver<Action>,
    current_action: Option<String>,
    action_status: Stream<ActionResponse>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Bridge {
        Bridge { config, data_tx, actions_rx, current_action: None, action_status }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let mut action_status = self.action_status.clone();

        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;

            let (stream, addr) = loop {
                select! {
                    v = listener.accept() =>  {
                        match v {
                            Ok(s) => break s,
                            Err(e) => {
                                error!("Tcp connection accept error = {:?}", e);
                                continue;
                            }
                        }
                    }
                    action = self.actions_rx.recv_async() => {
                        let action = action?;
                        error!("Bridge down!! Action ID = {}", action.action_id);
                        let status = ActionResponse::failure(&action.action_id, "Bridge down");
                        if let Err(e) = action_status.fill(status).await {
                            error!("Failed to send busy status. Error = {:?}", e);
                        }
                    }
                }
            };

            info!("Accepted new connection from {:?}", addr);
            let framed = Framed::new(stream, LinesCodec::new());
            if let Err(e) = self.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
            }
        }
    }

    pub async fn collect(
        &mut self,
        mut framed: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        let flush_period = Duration::from_secs(self.config.flush_period.unwrap_or(10));

        let mut bridge_partitions = HashMap::new();
        for (stream, config) in self.config.streams.clone() {
            bridge_partitions.insert(
                stream.clone(),
                Stream::new(stream, config.topic, config.buf_size, self.data_tx.clone()),
            );
        }

        let mut action_status = self.action_status.clone();
        let action_timeout = time::sleep(Duration::from_secs(100));
        tokio::pin!(action_timeout);

        let mut flush_handler = DelayHandler::new(flush_period);

        loop {
            select! {
                frame = framed.next() => {
                    let frame = frame.ok_or(Error::StreamDone)??;
                    debug!("Received line = {:?}", frame);

                    let data: Payload = match serde_json::from_str(&frame) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    // If incoming data is a response for an action, drop it
                    // if timeout is already sent to cloud
                    if data.stream == "action_status" {
                        match self.current_action.take() {
                            Some(id) => debug!("Response for action = {:?}", id),
                            None => {
                                error!("Action timed out already");
                                continue
                            }
                        }
                    }

                    let data_stream = data.stream.clone();

                    let partition = match bridge_partitions.get_mut(&data_stream) {
                        Some(partition) => partition,
                        None => {
                            if bridge_partitions.keys().len() > 20 {
                                error!("Failed to create {:?} stream. More than max 20 streams", data_stream);
                                continue
                            }

                            let stream = Stream::dynamic(&data_stream, &self.config.project_id, &self.config.device_id, self.data_tx.clone());
                            bridge_partitions.entry(data_stream.clone()).or_insert(stream)
                        }
                    };

                    // Remove timeout from flush_handler for selected stream.
                    flush_handler.remove(&data_stream);

                    match partition.fill(data).await {
                        Ok(f) => if !f {
                            // Add new timeout to flush_handler if not flushed,
                            // effectively reseting timeout.
                            flush_handler.insert(data_stream)
                        },
                        Err(e) => {
                            error!("Failed to send data. Error = {:?}", e.to_string());
                            continue
                        }
                    }
                }

                action = self.actions_rx.recv_async() => {
                    let action = action?;
                    self.current_action = Some(action.action_id.to_owned());

                    action_timeout.as_mut().reset(Instant::now() + Duration::from_secs(10));
                    let data = match serde_json::to_vec(&action) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    };

                    framed.get_mut().write_all(&data).await?;
                    framed.get_mut().write_all(b"\n").await?;
                }

                _ = &mut action_timeout, if self.current_action.is_some() => {
                    let action = self.current_action.take().unwrap();
                    error!("Timeout waiting for action response. Action ID = {}", action);

                    // Send failure response to cloud
                    let status = ActionResponse::failure(&action, "Action timed out");
                    if let Err(e) = action_status.fill(status).await {
                        error!("Failed to fill. Error = {:?}", e);
                    }
                }

                // Flush stream/partitions that timeout
                stream = flush_handler.next(), if !flush_handler.is_empty() => {
                    let stream = stream.unwrap();
                    bridge_partitions.get_mut(&stream).unwrap().flush().await?;
                }

            }
        }
    }
}

/// An internal structure to manage flushing Streams by setting timers
struct DelayHandler {
    queue: DelayQueue<String>,
    map: HashMap<String, Key>,
    period: Duration,
}

impl DelayHandler {
    pub fn new(period: Duration) -> Self {
        Self { queue: DelayQueue::new(), map: HashMap::new(), period }
    }

    // Removes timeout if it exists, else do nothing.
    pub fn remove(&mut self, stream: &str) {
        if let Some(flush_key) = self.map.remove(stream) {
            self.queue.remove(&flush_key);
        }
    }

    // Insert new timeout.
    pub fn insert(&mut self, stream: String) {
        let key = self.queue.insert(stream.clone(), self.period);
        self.map.insert(stream, key);
    }

    // Remove a key from map if it has timedout
    pub async fn next(&mut self) -> Option<String> {
        if let Some(stream) = self.queue.next().await {
            self.map.remove_entry(stream.get_ref());
            return Some(stream.get_ref().to_owned());
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is in turn a json
// TODO which cloud will double deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    #[serde(skip_serializing)]
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

impl Payload {
    pub fn from_string<S: Into<String>>(input: S) -> Result<Self, Error> {
        Ok(serde_json::from_str(&input.into())?)
    }
}

impl Point for Payload {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Payload> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.buffer)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}
