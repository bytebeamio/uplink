use flume::{bounded, Receiver, RecvError, SendError, Sender};
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

mod stats;

use self::stats::{AppStatsHandler, BridgeStatsHandler};

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

/// Keep track of actions in execution, ensure they are timed out when there is no
/// response within the designated time period.
/// NOTE: We only expect one action to be processed over uplink's bridge at a time
struct CurrentAction {
    id: String,
    timeout: Pin<Box<Sleep>>,
}

pub struct Bridge {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    actions_rx: Receiver<Action>,
    actions_tx: BTx<Action>,
    action_status: Stream<ActionResponse>,
    status_tx: Sender<ActionResponse>,
    status_rx: Receiver<ActionResponse>,
    bridge_stats: BridgeStatsHandler,
    // - set to None when
    // -- timeout ends
    // -- A response with status "Completed" is received
    // - set to a value when
    // -- it is currently None and a new action is received
    // - timeout is updated
    // -- when a non "Completed" action is received
    current_action: Option<CurrentAction>,
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
        let bridge_stats = BridgeStatsHandler::new(config.clone(), data_tx.clone());

        Bridge {
            action_status,
            data_tx,
            config,
            actions_rx,
            actions_tx,
            status_tx,
            status_rx,
            bridge_stats,
            current_action: None,
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;

            let mut end = Box::pin(time::sleep(Duration::from_secs(u64::MAX)));

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

                    action = self.actions_rx.recv_async(), if self.current_action.is_none() => {
                        let action = action?;
                        let action_id = action.action_id.clone();

                        info!("Received action: {:?}", action);
                        self.action_status.fill(ActionResponse::progress(&action.action_id, "Received", 0)).await?;

                        self.capture_action(&action);

                        if self.actions_tx.send(action).is_err() {
                            let error_str = format!("Bridge down!! Action ID = {action_id}");
                            error!("{error_str}");
                            self.bridge_stats.capture_error(error_str);
                            self.reset_action();

                            let status = ActionResponse::failure(&action_id, "Bridge down");
                            if let Err(e) = self.action_status.fill(status).await {
                                error!("Failed to send busy status. Error = {:?}", e);
                            }
                        }

                        self.update_bridge_stats().await;
                        self.bridge_stats.reset_error();
                    }

                    response = self.status_rx.recv_async() => {
                        let response = response?;
                        let (action_id, timeout) = match &mut self.current_action {
                            Some(CurrentAction { id, timeout }) => (id, timeout),
                            None => {
                                let error_str = format!("Action timed out already, ignoring response: {response:?}");
                                error!("{error_str}");
                                self.bridge_stats.capture_error(error_str);
                                self.update_bridge_stats().await;
                                self.bridge_stats.reset_error();
                                continue;
                            }
                        };

                        if *action_id != response.action_id {
                            error!("action_id in action_status({}) does not match that of active action ({})", response.action_id, action_id);
                            continue;
                        }

                        if &response.state == "Completed" || &response.state == "Failed" {
                            self.reset_action();
                        } else {
                            *timeout = Box::pin(time::sleep(ACTION_TIMEOUT));
                        }
                        if let Err(e) = self.action_status.fill(response).await {
                            error!("Failed to fill. Error = {:?}", e);
                        }
                    }

                    _ = &mut self.current_action.as_mut().map(|a| &mut a.timeout).unwrap_or(&mut end) => {
                        let action = self.reset_action();
                        let action_id = action.id;
                        let error_str = format!("Timeout waiting for action response. Action ID = {action_id}");
                        error!("{error_str}");

                        // Send failure response to cloud
                        let status = ActionResponse::failure(&action_id, "Action timed out");
                        if let Err(e) = self.action_status.fill(status).await {
                            error!("Failed to fill. Error = {:?}", e);
                        }

                        self.bridge_stats.capture_error(error_str);
                        self.update_bridge_stats().await;
                        self.bridge_stats.reset_error();
                    }
                }
            }
        }
    }

    async fn spawn_collector(&mut self, stream: TcpStream) {
        let framed = Framed::new(stream, LinesCodec::new());
        let id = self.bridge_stats.accept();
        let mut tcp_json = TcpJson::new(
            id,
            self.config.clone(),
            self.data_tx.clone(),
            self.status_tx.clone(),
            self.actions_tx.subscribe(),
        );

        tokio::task::spawn(async move {
            if let Err(e) = tcp_json.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
                tcp_json.app_stats.disconnect(e.to_string());
                tcp_json.update_app_stats().await;
            }
        });
    }

    fn capture_action(&mut self, action: &Action) {
        self.bridge_stats.capture_action(action.action_id.clone());
        self.current_action = Some(CurrentAction {
            id: action.action_id.clone(),
            timeout: Box::pin(time::sleep(ACTION_TIMEOUT)),
        });
    }

    fn reset_action(&mut self) -> CurrentAction {
        self.bridge_stats.reset_action();
        self.current_action.take().unwrap()
    }

    /// Update platform when an action is accepted, timeout is triggered or an error occurs.
    /// This can help us to keep track of the status of  that consumes an action and disconnects
    /// before being able to send a response, thus triggering an action timeout.
    async fn update_bridge_stats(&mut self) {
        let app_count = self.actions_tx.receiver_count();
        self.bridge_stats.update(app_count).await;
    }
}

struct TcpJson {
    streams: Streams,
    status_tx: Sender<ActionResponse>,
    actions_rx: BRx<Action>,
    app_stats: AppStatsHandler,
}

impl TcpJson {
    fn new(
        id: usize,
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        status_tx: Sender<ActionResponse>,
        actions_rx: BRx<Action>,
    ) -> Self {
        let app_stats = AppStatsHandler::new(id, config.clone(), data_tx.clone());

        Self { status_tx, streams: Streams::new(config, data_tx), actions_rx, app_stats }
    }

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
                            self.app_stats.capture_error(e.to_string());
                            self.update_app_stats().await;
                            continue
                        }
                    };
                    info!("Received line = {:?}", line);

                    let data: Payload = match serde_json::from_str(&line) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            self.app_stats.capture_error(e.to_string());
                            self.update_app_stats().await;
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
                                self.app_stats.capture_error(e.to_string());
                                self.update_app_stats().await;
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
                    self.app_stats.capture_action(&action);
                    match serde_json::to_string(&action) {
                        Ok(data) => client.send(data).await?,
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            self.app_stats.capture_error(e.to_string());
                            self.update_app_stats().await;
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
        self.app_stats.update().await;
        self.app_stats.reset();
    }
}
