use flume::{Receiver, RecvError, Sender};
use futures_util::SinkExt;
use log::{error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{channel, Receiver as BRx, Sender as BTx};
use tokio::time::{Duration, Sleep};
use tokio::{select, time};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::pin::Pin;
use std::{io, sync::Arc};

use super::util::Streams;
use crate::base::middleware::Error as ActionsError;
use crate::{Action, ActionResponse, Config, Package, Payload, Stream};

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
    #[error("Broadcast receiver error {0}")]
    BRecv(#[from] tokio::sync::broadcast::error::RecvError),
}

pub struct Bridge {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    actions_rx: Receiver<Action>,
    actions_tx: BTx<Action>,
    action_status: Stream<ActionResponse>,
}

const ACTION_TIMEOUT: Duration = Duration::from_secs(30);

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Bridge {
        let (actions_tx, _) = channel(10);
        Bridge { action_status: action_status.clone(), data_tx, config, actions_rx, actions_tx }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;

            loop {
                select! {
                    v = listener.accept() =>  {
                        match v {
                            Ok((stream, addr)) => {
                                info!("Accepted new connection from {:?}", addr);
                                self.spawn_collector(stream).await
                            },
                            Err(e) => {
                                error!("Tcp connection accept error = {:?}", e);
                                continue;
                            }
                        }
                    }
                    action = self.actions_rx.recv_async() => {
                        let action = action?;
                        let action_id = action.action_id.clone();
                        if self.actions_tx.send(action).is_err() {
                            error!("Bridge down!! Action ID = {}", action_id);
                            let status = ActionResponse::failure(&action_id, "Bridge down");
                            if let Err(e) = self.action_status.fill(status).await {
                                error!("Failed to send busy status. Error = {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn spawn_collector(&self, stream: TcpStream) {
        let framed = Framed::new(stream, LinesCodec::new());
        let mut tcp_json = TcpJson {
            action_status: self.action_status.clone(),
            streams: Streams::new(self.config.clone(), self.data_tx.clone()),
            actions_rx: self.actions_tx.subscribe(),
        };
        tokio::task::spawn(async move {
            if let Err(e) = tcp_json.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
            }
        });
    }
}

struct TcpJson {
    streams: Streams,
    action_status: Stream<ActionResponse>,
    actions_rx: BRx<Action>,
}

impl TcpJson {
    /// NOTE: We only expect one action to be processed over uplink's bridge at a time,
    /// which is what current_action achieves
    pub async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
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

        loop {
            select! {
                line = client.next() => {
                    // Update action_status for current action
                    if line.is_none() && current_action_.is_some() {
                        self.action_status
                            .fill(ActionResponse::failure(
                                current_action_.take().unwrap().id.as_str(),
                                "bridge disconnected",
                            ))
                            .await?;
                    }
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
                        let (action_id, timeout) = match &mut current_action_ {
                            Some(CurrentAction { id, timeout }) => (id, timeout),
                            None => {
                                error!("Action timed out already, ignoring response: {:?}", data);
                                continue;
                            }
                        };

                        let response = match ActionResponse::from_payload(&data) {
                            Ok(response) => response,
                            Err(e) => {
                                error!("Couldn't parse payload as an action response: {e:?}");
                                continue;
                            }
                        };

                        if *action_id != response.action_id {
                            error!("action_id in action_status({}) does not match that of active action ({})", response.action_id, action_id);
                            continue;
                        }

                        if &response.state == "Completed" || &response.state == "Failed" {
                            current_action_ = None;
                        } else {
                            *timeout = Box::pin(time::sleep(ACTION_TIMEOUT));
                        }
                        self.action_status.fill(response).await?;
                    } else {
                        self.streams.forward(data).await
                    }
                }

                action = self.actions_rx.recv(), if current_action_.is_none() => {
                    let action = action?;
                    info!("Received action: {:?}", action);

                    self.action_status.fill(ActionResponse::progress(&action.action_id, "Received", 0)).await?;

                    match serde_json::to_string(&action) {
                        Ok(data) => {
                            current_action_ = Some(CurrentAction {
                                id: action.action_id.clone(),
                                timeout: Box::pin(time::sleep(ACTION_TIMEOUT)),
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
                _ = self.streams.flush(), if self.streams.is_flushable() => {}

            }
        }
    }
}
