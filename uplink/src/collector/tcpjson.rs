use flume::{bounded, Receiver, RecvError, SendError, Sender};
use futures_util::SinkExt;
use log::{error, info};
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{channel, Receiver as BRx, Sender as BTx};
use tokio::time::{Duration, Sleep};
use tokio::{select, time};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
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
    status_tx: Sender<ActionResponse>,
    status_rx: Receiver<ActionResponse>,
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

        Bridge { action_status, data_tx, config, actions_rx, actions_tx, status_tx, status_rx }
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
            let mut current_action_: Option<CurrentAction> = None;

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

                    action = self.actions_rx.recv_async(), if current_action_.is_none() => {
                        let action = action?;
                        let action_id = action.action_id.clone();

                        info!("Received action: {:?}", action);
                        self.action_status.fill(ActionResponse::progress(&action.action_id, "Received", 0)).await?;

                        current_action_ = Some(CurrentAction {
                            id: action.action_id.clone(),
                            timeout: Box::pin(time::sleep(ACTION_TIMEOUT)),
                        });

                        if self.actions_tx.send(action).is_err() {
                            error!("Bridge down!! Action ID = {}", action_id);
                            let status = ActionResponse::failure(&action_id, "Bridge down");
                            if let Err(e) = self.action_status.fill(status).await {
                                error!("Failed to send busy status. Error = {:?}", e);
                            }
                        }
                    }

                    response = self.status_rx.recv_async() => {
                        let response = response?;
                        let (action_id, timeout) = match &mut current_action_ {
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
                            current_action_ = None;
                        } else {
                            *timeout = Box::pin(time::sleep(ACTION_TIMEOUT));
                        }
                        if let Err(e) = self.action_status.fill(response).await {
                            error!("Failed to fill. Error = {:?}", e);
                        }
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
                }
            }
        }
    }

    async fn spawn_collector(&self, stream: TcpStream) {
        let framed = Framed::new(stream, LinesCodec::new());
        let mut tcp_json = TcpJson {
            status_tx: self.status_tx.clone(),
            streams: Streams::new(self.config.clone(), self.data_tx.clone()),
            actions_rx: self.actions_tx.subscribe(),
            app_stats: AppStats::new(),
        };
        tokio::task::spawn(async move {
            if let Err(e) = tcp_json.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
                tcp_json.app_stats.disconnect();
                tcp_json.update_app_stats().await;
            }
        });
    }
}

struct TcpJson {
    streams: Streams,
    status_tx: Sender<ActionResponse>,
    actions_rx: BRx<Action>,
    app_stats: AppStats,
}

impl TcpJson {
    pub async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        loop {
            select! {
                line = client.next() => {
                    let line = match line.ok_or(Error::StreamDone)? {
                        Ok(l) => l,
                        Err(e) => {
                            error!("Couldn't error = {:?}", e);
                            continue
                        }
                    };
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
                        let response = match ActionResponse::from_payload(&data) {
                            Ok(response) => response,
                            Err(e) => {
                                error!("Couldn't parse payload as an action response: {e:?}");
                                continue;
                            }
                        };
                        self.app_stats.capture_response(&response);
                        self.status_tx.send_async(response).await?;
                        self.update_app_stats().await;
                    } else {
                        self.streams.forward(data).await
                    }
                }

                action = self.actions_rx.recv() => {
                    let action = action?;
                    self.app_stats.last_action = Some(action.action_id.to_owned());
                    match serde_json::to_string(&action) {
                        Ok(data) => client.send(data).await?,
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    }
                    self.app_stats.capture_action(&action);
                }

                // Flush stream/partitions that timeout
                _ = self.streams.flush(), if self.streams.is_flushable() => {}

            }
        }
    }

    /// Update platform when a connected app sends a response or disconnects.
    /// This can help us to keep track of an app that consumes an action and disconnects
    /// before being able to send a response, thus triggering an action timeout.
    async fn update_app_stats(&mut self) {
        let payload = self.app_stats.capture_stats();
        self.streams.forward(payload).await;
    }
}

#[derive(Serialize, Clone, Default)]
struct AppStats {
    sequence: u32,
    timestamp: u64,
    connection_timestamp: u64,
    actions_received: u32,
    last_action: Option<String>,
    actions_responded: u32,
    last_response: Option<ActionResponse>,
    connected: bool,
}

impl AppStats {
    fn new() -> Self {
        Self {
            connection_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
                as u64,
            connected: true,
            ..Default::default()
        }
    }

    fn capture_response(&mut self, response: &ActionResponse) {
        self.last_response = Some(response.clone());
        self.actions_responded += 1;
    }

    fn capture_action(&mut self, action: &Action) {
        self.last_action = Some(action.action_id.clone());
        self.actions_received += 1;
    }

    fn capture_stats(&mut self) -> Payload {
        self.sequence += 1;
        self.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        Payload::from(self.clone())
    }

    fn disconnect(&mut self) {
        self.connected = false;
    }
}

impl From<AppStats> for Payload {
    fn from(stats: AppStats) -> Self {
        let AppStats {
            sequence,
            timestamp,
            connection_timestamp,
            actions_received,
            last_action,
            actions_responded,
            last_response,
            connected,
        } = stats;
        Payload {
            stream: "uplink_app_stats".to_string(),
            sequence,
            timestamp,
            payload: json! ({
                "connection_timestamp": connection_timestamp,
                "connected": connected,
                "actions_received":actions_received,
                "last_action":last_action,
                "actions_responded":actions_responded,
                "last_response":last_response
            }),
        }
    }
}
