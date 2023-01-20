use flume::{RecvError, SendError};
use futures_util::SinkExt;
use log::{error, info, trace};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::{io, sync::Arc};

use crate::base::bridge::BridgeTx;
use crate::{ActionResponse, Config, Payload};

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
    config: Arc<Config>,
    /// Bridge handle to register apps
    bridge: BridgeTx,
}

impl TcpJson {
    pub fn new(config: Arc<Config>, bridge: BridgeTx) -> TcpJson {
        // Note: We can register `TcpJson` itself as an app to direct actions to it
        TcpJson { config, bridge }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;
            let mut count = 0;

            info!("Listening for new connection on {:?}", addr);
            loop {
                select! {
                    v = listener.accept() =>  {
                        match v {
                            Ok((stream, addr)) => {
                                info!("Accepted new connection from {:?}", addr);
                                count += 1;

                                let framed = Framed::new(stream, LinesCodec::new());
                                let app = format!("tcp-json-client-{}", count);
                                let mut tcp_json = ClientConnection::new(&app, self.bridge.clone());

                                tokio::task::spawn(async move {
                                    if let Err(e) = tcp_json.collect(framed).await {
                                        error!("Bridge failed. Error = {:?}", e);
                                    }

                                    // tcp_json.close().await;
                                });
                            },
                            Err(e) => {
                                error!("Tcp connection accept error = {:?}", e);
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
}

struct ClientConnection {
    name: String,
    bridge: BridgeTx,
}

impl ClientConnection {
    pub fn new(name: &str, bridge: BridgeTx) -> ClientConnection {
        ClientConnection { name: name.to_owned(), bridge }
    }

    pub async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        let actions_rx = self.bridge.register_action_route(&self.name).await;

        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    trace!("{}: Received line = {:?}", self.name, line);
                    let data = match serde_json::from_str::<Payload>(&line) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    if data.stream == "action_status" {
                        let response = match ActionResponse::from_payload(&data) {
                            Ok(response) => response,
                            Err(e) => {
                                error!("Couldn't parse payload as an action response: {e:?}");
                                continue;
                            }
                        };

                        self.bridge.send_action_response(response).await;
                        continue
                    }

                    self.bridge.send_payload(data).await;
                }

                action = actions_rx.recv_async() => {
                    let action = action?;
                    match serde_json::to_string(&action) {
                        Ok(data) => client.send(data).await?,
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    }
                }
            }
        }
    }

    // TODO: Implement close in bridge
    // async fn close(&mut self) {
    //     if let Some(action_id) = self.in_execution.take() {
    //         let status = ActionResponse::failure(&action_id, "Bridge disconnected");
    //         if let Err(e) = self.status_tx.send_async(status).await {
    //             error!("Failed to send failure response. Error = {:?}", e);
    //         }
    //     }
    // }
}
