use flume::{bounded, Receiver, RecvError, SendError, Sender};
use futures_util::SinkExt;
use log::{debug, error, info};
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
    #[error("Sender error {0}")]
    Send(#[from] SendError<ActionResponse>),
    #[error("Sender error {0}")]
    SendUnit(#[from] SendError<()>),
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

    /// Actions incoming from backend
    actions_rx: Receiver<Action>,
    /// Action responses going to backend
    action_status: Stream<ActionResponse>,

    /// Used to broadcast actions to all client connections
    actions_tx: BTx<Action>,
    /// Given to client connections for sending action responses
    status_tx: Sender<ActionResponse>,
    /// Process action responses coming in from client connections
    status_rx: Receiver<ActionResponse>,
    /// Used by clients to signal when they disconnect
    client_disconnect_tx: Sender<()>,
    client_disconnect_rx: Receiver<()>,
    /// Keeps track of the number of clients connected
    clients_count: u32,
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
        let (status_tx, status_rx) = bounded(10);
        let (client_disconnect_tx, client_disconnect_rx) = bounded(1);

        Bridge {
            action_status,
            data_tx,
            config,
            actions_rx,
            actions_tx,
            status_tx,
            status_rx,
            client_disconnect_tx,
            client_disconnect_rx,
            clients_count: 0
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;

            let mut end = Box::pin(time::sleep(Duration::from_secs(u64::MAX)));

            /// NOTE: We only expect one action to be processed over uplink's bridge at a time
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
            let mut current_action: Option<CurrentAction> = None;

            info!("Listening for new connection on {:?}", addr);
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

                    action = self.actions_rx.recv_async(), if current_action.is_none() => {
                        let action = action?;
                        info!("Received action: {:?}", action);
                        let action_id = action.action_id.clone();

                        if self.actions_tx.send(action).is_ok() {
                            current_action = Some(CurrentAction {
                                id: action_id.clone(),
                                timeout: Box::pin(time::sleep(ACTION_TIMEOUT)),
                            });
                            self.action_status.fill(ActionResponse::progress(&action_id, "Received", 0)).await?;
                        } else if self.config.ignore_actions_if_no_clients {
                            error!("No clients connected, ignoring action = {:?}", action_id);
                        } else {
                            error!("Bridge down!! Action ID = {}", action_id);
                            let status = ActionResponse::failure(&action_id, "Bridge down");
                            if let Err(e) = self.action_status.fill(status).await {
                                error!("Failed to send busy status. Error = {:?}", e);
                            }
                        }
                    }

                    response = self.status_rx.recv_async() => {
                        let response = response?;
                        let (action_id, timeout) = match &mut current_action {
                            Some(CurrentAction { id, timeout }) => (id, timeout),
                            None => {
                                error!("Action timed out already, ignoring response: {:?}", response);
                                continue;
                            }
                        };

                        if *action_id != response.action_id {
                            error!("action_id in action_status({}) does not match that of active action ({})", response.action_id, action_id);
                            continue;
                        }

                        if &response.state == "Completed" || &response.state == "Failed" {
                            current_action = None;
                        } else {
                            *timeout = Box::pin(time::sleep(ACTION_TIMEOUT));
                        }
                        if let Err(e) = self.action_status.fill(response).await {
                            error!("Failed to fill. Error = {:?}", e);
                        }
                    }

                    _ = self.client_disconnect_rx.recv_async() => {
                        self.clients_count -= 1;
                        if self.clients_count == 0 && current_action.is_some() {
                            error!("All bridge clients have disconnected, terminating current action");
                            let status = ActionResponse::failure(current_action.take().unwrap().id.as_str(), "Bridge apps disconnected");
                            if let Err(e) = self.action_status.fill(status).await {
                                error!("Failed to send failure response. Error = {:?}", e);
                            }
                        }
                    }

                    _ = &mut current_action.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                        let action = current_action.take().unwrap();
                        error!("Timeout waiting for action response. Action ID = {}", action.id);

                        // Send failure response to cloud
                        let status = ActionResponse::failure(&action.id, "Action timed out");
                        if let Err(e) = self.action_status.fill(status).await {
                            error!("Failed to fill. Error = {:?}", e);
                        }
                    }
                }
            }
        }
    }

    async fn spawn_collector(&mut self, stream: TcpStream) {
        let framed = Framed::new(stream, LinesCodec::new());
        let mut tcp_json = ClientConnection {
            status_tx: self.status_tx.clone(),
            streams: Streams::new(self.config.clone(), self.data_tx.clone()),
            actions_rx: self.actions_tx.subscribe(),
        };
        let close_tx = self.client_disconnect_tx.clone();
        self.clients_count += 1;
        tokio::task::spawn(async move {
            if let Err(e) = tcp_json.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
            }
            if let Err(e) = close_tx.send(()) {
                error!("Failed to send client disconnect. Error = {:?}", e);
            }
        });
    }
}

struct ClientConnection {
    streams: Streams,
    status_tx: Sender<ActionResponse>,
    actions_rx: BRx<Action>,
}

impl ClientConnection {
    pub async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    debug!("Received line = {:?}", line);

                    let data = match serde_json::from_str::<Payload>(&line) {
                        Ok(d) => d.set_collection_timestamp(),
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    // If incoming data is a response for an action, set in_execution and forward
                    if data.stream == "action_status" {
                        let response = match ActionResponse::from_payload(&data) {
                            Ok(response) => response,
                            Err(e) => {
                                error!("Couldn't parse payload as an action response: {e:?}");
                                continue;
                            }
                        };

                        self.status_tx.send_async(response).await?;
                    } else {
                        self.streams.forward(data).await
                    }
                }

                action = self.actions_rx.recv() => {
                    let action = action?;
                    match serde_json::to_string(&action) {
                        Ok(data) => client.send(data).await?,
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    }
                }

                // Flush stream/partitions that timeout
                _ = self.streams.flush(), if self.streams.is_flushable() => {}

            }
        }
    }
}
