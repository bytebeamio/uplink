use flume::{bounded, Receiver, RecvError, SendError};
use futures_util::SinkExt;
use log::{error, info, trace};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task::{spawn, JoinHandle};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use futures_util::future::{Future, ready, Ready, select_all, SelectAll};
use futures_util::stream::{FuturesUnordered, Stream};

use std::io;
use std::time::Duration;
use tokio::time::{Sleep, Timeout, timeout};

use crate::base::bridge::BridgeTx;
use crate::base::AppConfig;
use crate::{Action, ActionResponse, Payload};

#[allow(clippy::large_enum_variant)]
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


#[derive(Debug)]
pub struct TcpJson {
    name: String,
    config: AppConfig,
    /// Bridge handle to register apps
    bridge: BridgeTx,
    /// Action receiver
    actions_rx: Option<Receiver<Action>>,
    buffered_action: Option<(Action, Sleep)>,
    connected_client: Option<Framed<TcpStream, LinesCodec>>,
}

impl TcpJson {
    pub async fn new(name: String, config: AppConfig, bridge: BridgeTx) -> TcpJson {
        let actions_rx = if !config.actions.is_empty() {
            // TODO: Return Option<Receiver> while registering multiple actions
            let actions_rx = bridge.register_action_routes(&config.actions).await;
            Some(actions_rx)
        } else {
            None
        };

        // Note: We can register `TcpJson` itself as an app to direct actions to it
        TcpJson {
            name,
            config,
            bridge,
            actions_rx,
            buffered_action: None,
            connected_client: None,
        }
    }

    pub async fn start(mut self) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        let mut handle: Option<JoinHandle<()>> = None;
        let (_, mut mock_action_receiver) = bounded(0);
        info!("Waiting for app = {} to connect on {:?}", self.name, addr);
        loop {
            select! {
                framed = listener.accept(), if self.connected_client.is_none() => {
                    match framed {
                        Ok((stream, addr)) => {
                            info!("Accepted connection from app = {} on {addr}", self.name);
                            let mut client = Framed::new(stream, LinesCodec::new());
                            if let Some((action, _)) = self.buffered_action.take() {
                                client.send(serde_json::to_string(&action)?).await?;
                            }
                            self.connected_client = Some(client);
                        }
                        Err(e) => {
                            error!("Tcp connection accept error = {e}, app = {}", self.name);
                        }
                    }
                }
                // line = cc, if self.connected_client.is_some() => {
                //     match line {
                //         Some(Ok(line)) => {
                //             if let Err(e) = self.handle_incoming_line(line).await {
                //                 error!("Couldn't process incoming line: {e:?}");
                //             }
                //         },
                //         _ => {
                //             self.connected_client = None;
                //         }
                //     }
                // }
                action = self.actions_rx.as_mut().unwrap_or(&mut mock_action_receiver).recv_async(), if self.buffered_action.is_none() => {
                    let action = action?;
                    if self.connected_client.is_some() {
                        match serde_json::to_string(&action) {
                            Ok(data) => self.connected_client.as_mut().unwrap().send(data).await?,
                            Err(e) => {
                                error!("Serialization error = {e}, app = {}", self.name);
                                continue
                            }
                        }
                    } else {
                        self.buffered_action = Some((action, tokio::time::sleep(Duration::from_secs(5))));
                    }
                }
                // _ = &mut self.buffered_action.as_mut().map(|(_, timer)| timer).unwrap_or(&mut end), if self.buffered_action.is_some() && self.connected_client.is_none() => {
                //
                // }
            }
            // let framed = match listener.accept().await {
            //     Ok((stream, addr)) => {
            //         info!("Accepted connection from app = {} on {addr}", self.name);
            //         Framed::new(stream, LinesCodec::new())
            //     }
            //     Err(e) => {
            //         error!("Tcp connection accept error = {e}, app = {}", self.name);
            //         continue;
            //     }
            // };
            //
            // if let Some(handle) = handle {
            //     handle.abort();
            // }
            //
            // let tcpjson = self.clone();
            // handle = Some(spawn(async move {
            //     if let Err(e) = tcpjson.collect(framed).await {
            //         error!("TcpJson failed. app = {}, Error = {e}", tcpjson.name);
            //     }
            // }));
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
