use flume::{Receiver, RecvError, Sender};
use futures_util::SinkExt;
use log::{error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::{io, sync::Arc};

use crate::base::middleware::{Action, ActionResponse};
use crate::base::{Config, Payload};

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
    #[error("Couldn't fill stream")]
    Stream(#[from] crate::base::Error),
}

pub struct Bridge {
    config: Arc<Config>,
    data_tx: Sender<Payload>,
    actions_rx: Receiver<Action>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Payload>,
        actions_rx: Receiver<Action>,
    ) -> Bridge {
        Bridge { config, data_tx, actions_rx }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;

            let (stream, addr) = loop {
                select! {
                    v = listener.accept() =>  {
                        match v {
                            Ok(s) => break s,
                            Err(e) => {
                                error!("Tcp connection accept error = {:?}", e);
                                continue;
                            }
                        }
                    }
                    action = self.actions_rx.recv_async() => {
                        let action = action?;
                        error!("Bridge down!! Action ID = {}", action.action_id);
                        let status = ActionResponse::failure(&action.action_id, "Bridge down");
                        if let Err(e) = self.data_tx.send_async(status.as_payload()).await {
                            error!("Failed to send busy status. Error = {:?}", e);
                        }
                    }
                }
            };

            info!("Accepted new connection from {:?}", addr);
            let framed = Framed::new(stream, LinesCodec::new());
            if let Err(e) = self.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
            }
        }
    }

    pub async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    info!("Received line = {:?}", line);

                    let data: Payload = match serde_json::from_str(&line) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    if let Err(e) = self.data_tx.send_async(data).await {
                        error!("Send error = {:?}", e);
                    }
                }

                action = self.actions_rx.recv_async() => {
                    let action = action?;
                    info!("Received action: {:?}", action);

                    match serde_json::to_string(&action) {
                        Ok(action) => {
                            client.send(action).await?;
                        },
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    };
                }
            }
        }
    }
}
