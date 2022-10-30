use flume::{Receiver, RecvError, Sender};
use futures_util::SinkExt;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, Sleep};
use tokio::{select, time};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::{collections::HashMap, io, sync::Arc};
use std::pin::Pin;

use super::util::DelayMap;
use crate::base::actions::{Action, ActionResponse, Error as ActionsError};
use crate::base::{Buffer, Config, Package, Point, Stream, StreamStatus};

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
    action_status: Stream<ActionResponse>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Bridge {
        Bridge { config, data_tx, actions_rx, action_status }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
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
                        if let Err(e) = self.action_status.fill(status).await {
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
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        let mut bridge_partitions = HashMap::new();
        for (name, config) in &self.config.streams {
            let stream = Stream::with_config(
                name,
                &self.config.project_id,
                &self.config.device_id,
                config,
                self.data_tx.clone(),
            );
            bridge_partitions.insert(name.to_owned(), stream);
        }

        let mut end = Box::pin(time::sleep(Duration::from_secs(u64::MAX)));
        struct CurrentAction {
            id: String,
            timeout: Pin<Box<Sleep>>,
        }
        // - set to None when
        // -- timeout ends
        // -- A response with status "Completed" is received
        // - set to a value when
        // -- it is currently None and a new action is received
        // - timeout is updated
        // -- when a non "Completed" action is received
        let mut current_action_: Option<CurrentAction> = None;

        let mut flush_handler = DelayMap::new();

        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    info!("Received line = {:?}", line);

                    let data: Payload = match serde_json::from_str(&line) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    // If incoming data is a response for an action, drop it
                    // if timeout is already sent to cloud
                    if data.stream == "action_status" {
                        if current_action_.is_some() {
                            if let Some(response_id) = data.payload.as_object()
                                .and_then(|payload| payload.get("action_id"))
                                .and_then(|id| id.as_str()) {
                                let action_id = current_action_.as_ref().unwrap().id.as_str();
                                if action_id == response_id {
                                    if let Some("Completed") = data.payload.as_object().unwrap().get("state")
                                        .and_then(|s| s.as_str()) {
                                        current_action_ = None;
                                    } else {
                                        current_action_.as_mut().unwrap().timeout = Box::pin(time::sleep(Duration::from_secs(10)));
                                    }
                                } else {
                                    error!("action_id in action_status({response_id}) does not match that of active action ({action_id})");
                                    continue;
                                }
                            } else {
                                error!("No valid action_id in action_status stream payload");
                                continue;
                            }
                        } else {
                            error!("Action timed out already, ignoring response: {:?}", data);
                            continue;
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

                    let max_stream_size = stream.max_buffer_size;
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
                    if max_stream_size > 1 {
                        match state {
                            StreamStatus::Flushed(name) => flush_handler.remove(name),
                            StreamStatus::Init(name, flush_period) => flush_handler.insert(name, flush_period),
                            StreamStatus::Partial(l) => {
                                debug!("Stream contains {} elements", l);
                            }
                        }
                    }
                }

                action = self.actions_rx.recv_async(), if current_action_.is_none() => {
                    let action = action?;
                    info!("Received action: {action}")

                    match serde_json::to_string(&action) {
                        Ok(data) => {
                            current_action_ = Some(CurrentAction {
                                id: action.action_id.clone(),
                                timeout: Box::pin(time::sleep(Duration::from_secs(10))),
                            });
                            client.send(data).await?;
                        },
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    };
                }

                _ = &mut current_action_.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                    let action = current_action_.take().unwrap();
                    error!("Timeout waiting for action response. Action ID = {}", action.id);

                    // Send failure response to cloud
                    let status = ActionResponse::failure(&action.id, "Action timed out");
                    if let Err(e) = self.action_status.fill(status).await {
                        error!("Failed to fill. Error = {:?}", e);
                    }
                }

                // Flush stream/partitions that timeout
                Some(stream) = flush_handler.next(), if !flush_handler.is_empty() => {
                    let stream = bridge_partitions.get_mut(&stream).unwrap();
                    stream.flush().await?;
                }

            }
        }
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
