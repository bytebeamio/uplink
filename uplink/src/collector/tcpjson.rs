use futures_util::SinkExt;
use log::{debug, error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::{spawn, JoinHandle};
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
    #[error("Receiver error")]
    Recv,
    #[error("Stream done")]
    StreamDone,
    #[error("Lines codec error {0}")]
    Codec(#[from] LinesCodecError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

#[derive(Debug)]
pub struct TcpJson {
    name: String,
    config: AppConfig,
    /// Bridge handle to register apps
    bridge: BridgeTx,
    /// Action receiver
    actions_rx: Option<Receiver<Action>>,
}

impl Clone for TcpJson {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            config: self.config.clone(),
            bridge: self.bridge.clone(),
            actions_rx: None,
        }
    }
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

    pub async fn start(mut self) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        let mut handle: Option<JoinHandle<()>> = None;
        let (mut actions_tx, _) = channel(1);

        info!("Waiting for app = {} to connect on {:?}", self.name, addr);
        loop {
            select! {
                accepted = listener.accept() => {
                    let (tx,actions_rx) = channel(0);
                    actions_tx = tx;
                    let framed = match accepted {
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

                    let mut tcpjson = self.clone();
                    handle = Some(spawn(async move {
                        if let Err(e) = tcpjson.collect(framed, actions_rx).await {
                            error!("TcpJson failed. app = {}, Error = {e}", tcpjson.name);
                        }
                    }));
                }

                // Accepts action only if tx is not closed
                o = self.fwd_actions(&actions_tx), if !actions_tx.is_closed() => {
                    o?
                }
            }
        }
    }

    async fn fwd_actions(&mut self, actions_tx: &Sender<Action>) -> Result<(), Error> {
        let Some(actions_rx) = self.actions_rx.as_mut() else {
            return Ok(());
        };

        let action = actions_rx.recv().await.ok_or(Error::Recv)?;
        actions_tx.send(action).await.unwrap();

        Ok(())
    }

    async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
        mut actions_rx: Receiver<Action>,
    ) -> Result<(), Error> {
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    if let Err(e) = self.handle_incoming_line(line).await {
                        error!("Error handling incoming line = {e}, app = {}", self.name);
                    }
                }
                action = actions_rx.recv() => {
                    let action = action.ok_or(Error::Recv)?;
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
    }

    async fn handle_incoming_line(&self, line: String) -> Result<(), Error> {
        debug!("{}: Received line = {:?}", self.name, line);
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
