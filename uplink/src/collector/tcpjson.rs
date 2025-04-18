use flume::{Receiver, RecvError};
use futures_util::SinkExt;
use log::{debug, error, info, warn};
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
use crate::uplink_config::{AppConfig};
use crate::{Action, Payload};

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
                warn!("Dropping previous connection to tcpapp({}) because another connection was initiated", self.name);
            }

            {
                let mut client_connection = ClientConnection {
                    app_name: self.name.clone(),
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
    actions_rx: Option<Receiver<Action>>,
    bridge_tx: BridgeTx,
}

impl ClientConnection {
    pub async fn start(&mut self, mut client: Framed<TcpStream, LinesCodec>) -> anyhow::Result<()> {
        let (_dummy_action_tx, dummy_action_rx) = flume::bounded(0);
        let actions_rx = self.actions_rx.as_ref().unwrap_or(&dummy_action_rx);
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    if let Err(e) = self.handle_incoming_line(line).await {
                        error!("Error handling incoming line = {e}, app = {}", self.app_name);
                    }
                }
                action = actions_rx.recv_async() => {
                    let action = action?;
                    client.send(serde_json::to_string(&action).unwrap()).await
                        .context("failed to route action to client")?;
                }
            }
        }
    }
    async fn handle_incoming_line(&self, line: String) -> Result<(), Error> {
        debug!("{}: Received line = {line:?}", self.app_name);
        let data = serde_json::from_str::<Payload>(&line)?;

        self.bridge_tx.send_payload(data).await;

        Ok(())
    }
}