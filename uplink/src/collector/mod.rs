pub mod simulator;
pub mod systemstats;
pub mod tcpjson;
mod util;

use std::{collections::HashMap, sync::Arc, time::Duration};

use flume::{Receiver, Sender};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{select, time::Instant};

use crate::{
    base::{Buffer, StreamStatus},
    collector::util::DelayMap,
    Action, ActionResponse, Config, Package, Point, Stream,
};

#[derive(thiserror::Error, Debug)]
pub enum CollectorError {
    #[error("Lines codec error {0}")]
    Codec(#[from] tokio_util::codec::LinesCodecError),
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("Receiver error {0}")]
    Recv(#[from] flume::RecvError),
    #[error("Couldn't fill stream")]
    Stream(#[from] crate::base::Error),
    #[error("Stream done")]
    StreamDone,
}

pub struct Collector {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    actions_rx: Receiver<Action>,
    current_action: Option<String>,
    action_status: Stream<ActionResponse>,
}

impl Collector {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Self {
        Self { config, data_tx, actions_rx, current_action: None, action_status }
    }

    pub async fn collect(
        &mut self,
        mut interface: impl CollectorInterface,
    ) -> Result<(), CollectorError> {
        let mut bridge_partitions = HashMap::new();
        for (name, config) in &self.config.streams {
            let stream = Stream::with_config(name, config, self.data_tx.clone());
            bridge_partitions.insert(name.to_owned(), stream);
        }

        let mut action_status = self.action_status.clone();
        let action_timeout = tokio::time::sleep(Duration::from_secs(100));
        tokio::pin!(action_timeout);

        let mut flush_handler = DelayMap::new();

        loop {
            select! {
                data = interface.recv() => {
                    let data = data?;
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

                    let stream = match bridge_partitions.get_mut(&data.stream) {
                        Some(partition) => partition,
                        None => {
                            if bridge_partitions.keys().len() > 20 {
                                error!("Failed to create {:?} stream. More than max 20 streams", data.stream);
                                continue
                            }

                            let stream = Stream::dynamic(&data.stream, &self.config.project_id, &self.config.device_id, self.data_tx.clone());
                            bridge_partitions.entry(data.stream.clone()).or_insert(stream)
                        }
                    };

                    let state = match stream.fill(data).await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to send data. Error = {:?}", e.to_string());
                            continue
                        }
                    };

                    // Remove timeout from flush_handler for selected stream if stream state is flushed,
                    // do nothing if stream state is partial. Insert a new timeout if initial fill.
                    // Warn in case stream flushed stream was not in the queue.
                    match state {
                        StreamStatus::Flushed(name) => flush_handler.remove(name),
                        StreamStatus::Init(name, flush_period) => flush_handler.insert(name, flush_period),
                        StreamStatus::Partial(l) => {
                            debug!("Stream contains {} elements", l);
                        }
                    }
                }

                action = self.actions_rx.recv_async() => {
                    let action = action?;
                    self.current_action = Some(action.action_id.to_owned());

                    action_timeout.as_mut().reset(Instant::now() + Duration::from_secs(10));

                    match interface.send(action).await {
                        Ok(d) => d,
                        Err(CollectorError::Json(e)) => {
                            error!("Deserialization error = {:?}", e);
                            continue;
                        }
                        e => e?
                    }
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
                    let stream = bridge_partitions.get_mut(&stream).unwrap();
                    stream.flush().await?;
                }

            }
        }
    }
}

#[async_trait::async_trait]
pub trait CollectorInterface {
    async fn send(&mut self, action: Action) -> Result<(), CollectorError>;
    async fn recv(&mut self) -> Result<Payload, CollectorError>;
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
