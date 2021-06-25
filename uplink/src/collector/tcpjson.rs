use async_channel::{Receiver, RecvError, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{select, time};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tokio_util::codec::{LinesCodec, LinesCodecError};

use std::io;

use crate::base::actions::{Action, ActionResponse};
use crate::base::{Buffer, Config, Package, Point, Stream};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

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
}

pub struct Bridge {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    actions_rx: Receiver<Action>,
    current_action: Option<String>,
}

impl Bridge {
    pub fn new(config: Arc<Config>, data_tx: Sender<Box<dyn Package>>, actions_rx: Receiver<Action>) -> Bridge {
        Bridge { config, data_tx, actions_rx, current_action: None }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;
            let mut status_stream = Stream::new("action_status", 1, self.data_tx.clone());

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
                    action = self.actions_rx.recv() => {
                        let action = action.unwrap();
                        error!("Bridge down!! Action ID = {}", action.id);
                        let status = ActionResponse::failure(&action.id, "Bridge down");
                        if let Err(e) = status_stream.fill(status).await {
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

    pub async fn collect(&mut self, mut framed: Framed<TcpStream, LinesCodec>) -> Result<(), Error> {
        let mut bridge_partitions = HashMap::new();
        for (stream, config) in self.config.streams.clone() {
            bridge_partitions.insert(stream.clone(), Stream::new(stream, config.buf_size, self.data_tx.clone()));
        }

        let mut status_stream = Stream::new("action_status", 1, self.data_tx.clone());
        let action_timeout = time::sleep(Duration::from_secs(10));

        tokio::pin!(action_timeout);
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

                    match bridge_partitions.get_mut(&data.stream) {
                        Some(partition) => if let Err(e) = partition.fill(data).await {
                            error!("Failed to send data. Error = {:?}", e.to_string());
                        }
                        None => {
                            error!("Data on invalid stream = {:?}", data.stream);
                        }
                    }
                }
                action = self.actions_rx.recv() => {
                    let action = action?;
                    self.current_action = Some(action.id.to_owned());

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
                    if let Err(e) = status_stream.fill(status).await {
                        error!("Failed to fill. Error = {:?}", e);
                    }
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
    pub(crate) stream: String,
    pub(crate) sequence: u32,
    pub(crate) timestamp: u64,
    #[serde(flatten)]
    pub(crate) payload: Value,
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
    fn stream(&self) -> String {
        return self.stream.clone();
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> (String, usize) {
        self.anomalies()
    }
}
