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

use crate::base::bridge::BridgeTx;
use crate::config::AppConfig;
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
        let mut handle: Option<JoinHandle<()>> = None;

        info!("Waiting for app = {} to connect on {addr}", self.name);
        loop {
            let framed = match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted connection from app = {} on {addr}", self.name);
                    Framed::new(stream, LinesCodec::new())
                }
                Err(e) => {
                    error!("Tcp connection accept error = {e}, app = {}", self.name);
                    continue;
                }
            };

            if let Some(handle) = handle {
                handle.abort();
            }

            let tcpjson = self.clone();
            handle = Some(spawn(async move {
                if let Err(e) = tcpjson.collect(framed).await {
                    error!("TcpJson failed. app = {}, Error = {e}", tcpjson.name);
                }
            }));
        }
    }

    async fn collect(&self, mut client: Framed<TcpStream, LinesCodec>) -> Result<(), Error> {
        if let Some(actions_rx) = self.actions_rx.as_ref() {
            loop {
                select! {
                    line = client.next() => {
                        let line = line.ok_or(Error::StreamDone)??;
                        if let Err(e) = self.handle_incoming_line(line).await {
                            error!("Error handling incoming line = {e}, app = {}", self.name);
                        }
                    }
                    action = actions_rx.recv_async() => {
                        let action = action?;
                        match serde_json::to_string(&action) {
                            Ok(data) => client.send(data).await?,
                            Err(e) => {
                                error!("Serialization error = {e}, app = {}", self.name);
                                continue
                            }
                        }
                    }
                }
            }
        } else {
            loop {
                let line = client.next().await;
                let line = line.ok_or(Error::StreamDone)??;
                if let Err(e) = self.handle_incoming_line(line).await {
                    error!("Error handling incoming line = {e}, app = {}", self.name);
                }
            }
        }
    }

    async fn handle_incoming_line(&self, line: String) -> Result<(), Error> {
        debug!("{}: Received line = {line:?}", self.name);
        let data = serde_json::from_str::<Payload>(&line)?;

        if data.stream == "action_status" {
            let response = ActionResponse::from_payload(&data)?;
            self.bridge.send_action_response(response).await;

            return Ok(());
        }

        self.bridge.send_payload(data).await;

        Ok(())
    }
}
