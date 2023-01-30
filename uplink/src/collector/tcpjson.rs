use flume::{Receiver, RecvError, SendError};
use futures_util::SinkExt;
use log::{error, info, trace};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::io;

use crate::base::bridge::BridgeTx;
use crate::base::AppConfig;
use crate::{Action, ActionResponse, Payload};

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
    #[error("Couldn't fill stream")]
    Stream(#[from] crate::base::bridge::Error),
}

pub struct TcpJson {
    name: String,
    config: AppConfig,
    /// Bridge handle to register apps
    bridge: BridgeTx,
    actions_rx: Receiver<Action>,
}

impl TcpJson {
    pub fn new(
        name: String,
        config: AppConfig,
        bridge: BridgeTx,
        actions_rx: Receiver<Action>,
    ) -> TcpJson {
        // Note: We can register `TcpJson` itself as an app to direct actions to it
        TcpJson { name, config, bridge, actions_rx }
    }

    pub async fn start(mut self) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        info!("{}: Listening for new connection on {:?}", self.name, addr);
        loop {
            let framed = match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("{}: Accepted new connection from {:?}", self.name, addr);
                    Framed::new(stream, LinesCodec::new())
                }
                Err(e) => {
                    error!("Tcp connection accept error = {:?}", e);
                    continue;
                }
            };

            if let Err(e) = self.collect(framed).await {
                error!("TcpJson failed . Error = {:?}", e);
            }
        }
    }

    async fn collect(&mut self, mut client: Framed<TcpStream, LinesCodec>) -> Result<(), Error> {
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    if let Err(e) = self.handle_incoming_line(line).await {
                        error!("Error handling incoming line = {e}");
                    }
                }
                action = self.actions_rx.recv_async() => {
                    let action = action?;
                    match serde_json::to_string(&action) {
                        Ok(data) => client.send(data).await?,
                        Err(e) => {
                            error!("Serialization error = {e}");
                            continue
                        }
                    }
                }
            }
        }
    }

    async fn handle_incoming_line(&self, line: String) -> Result<(), Error> {
        trace!("{}: Received line = {:?}", self.name, line);
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
