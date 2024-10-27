use flume::{Receiver, RecvError};
use futures_util::SinkExt;
use log::{debug, error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task::{spawn, JoinHandle};
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::io;
use anyhow::Context;
use crate::base::bridge::BridgeTx;
use crate::config::{AppConfig};
use crate::{Action, ActionResponse, Payload};

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

#[derive(Debug, Clone)]
pub struct TcpJson {
    name: String,
    config: AppConfig,
    /// Bridge handle to register apps
    bridge: BridgeTx,
    /// Action receiver
    actions_rx: Option<Receiver<Action>>,
}

impl TcpJson {
    pub fn new(
        name: String,
        config: AppConfig,
        actions_rx: Option<Receiver<Action>>,
        bridge: BridgeTx,
    ) -> TcpJson {
        // Note: We can register `TcpJson` itself as an app to direct actions to it
        TcpJson { name, config, bridge, actions_rx }
    }

    pub async fn start(self) {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = loop {
            match TcpListener::bind(&addr).await {
                Ok(s) => break s,
                Err(e) => {
                    error!(
                        "Couldn't bind to port: {}; Error = {e}; retrying in 5s",
                        self.config.port
                    );
                    sleep(Duration::from_secs(5)).await;
                }
            }
        };
        let mut existing_connection: Option<JoinHandle<()>> = None;

        info!("Waiting for app = {} to connect on {addr}", self.name);
        loop {
            let tcp_connection = match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted connection from app = {} on {addr}", self.name);
                    Framed::new(stream, LinesCodec::new())
                }
                Err(e) => {
                    error!("Tcp connection accept error = {e}, app = {}", self.name);
                    continue;
                }
            };

            if let Some(existing_connection) = existing_connection.take() {
                existing_connection.abort();
                let _ = existing_connection.await;
                error!("Dropping previous connection to tcpapp({}) because another connection was initiated", self.name);
            }

            {
                let supports_cancellation = self.config.actions.iter().find(|c| c.cancellable).is_some();
                let mut client_connection = ClientConnection {
                    app_name: self.name.clone(),
                    supports_cancellation,
                    actions_rx: self.actions_rx.clone(),
                    bridge_tx: self.bridge.clone(),
                };
                existing_connection = Some(spawn(async move {
                    if let Err(e) = client_connection.start(tcp_connection).await {
                        error!("TcpJson failed. app = {}, Error = {e}", client_connection.app_name);
                    }
                }));
            }
        }
    }
}

struct ClientConnection {
    app_name: String,
    supports_cancellation: bool,
    actions_rx: Option<Receiver<Action>>,
    bridge_tx: BridgeTx,
}

impl ClientConnection {
    pub async fn start(&mut self, mut client: Framed<TcpStream, LinesCodec>) -> anyhow::Result<()> {
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    if let Err(e) = self.handle_incoming_line(line).await {
                        error!("Error handling incoming line = {e}, app = {}", self.app_name);
                    }
                }
                action = self.actions_rx.as_ref().unwrap().recv_async(), if self.actions_rx.is_some() => {
                    let action = action?;
                    if action.name == "cancel_action" {
                        if !self.supports_cancellation {
                            self.bridge_tx.send_action_response(ActionResponse::failure(&action.action_id, "This tcp port isn't configured to support cancellation")).await;
                        } else {
                            client.send(serde_json::to_string(&action).unwrap()).await
                                .context("failed to route action to client")?;
                        }
                    } else {
                        client.send(serde_json::to_string(&action).unwrap()).await
                            .context("failed to route action to client")?;
                    }
                }
            }
        }
    }
    async fn handle_incoming_line(&self, line: String) -> Result<(), Error> {
        debug!("{}: Received line = {line:?}", self.app_name);
        let data = serde_json::from_str::<Payload>(&line)?;

        if data.stream == "action_status" {
            let response = ActionResponse::from_payload(&data)?;
            self.bridge_tx.send_action_response(response).await;
        } else {
            self.bridge_tx.send_payload(data).await;
        }

        Ok(())
    }
}