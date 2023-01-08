use flume::{bounded, Receiver, RecvError, SendError, Sender};
use futures_util::SinkExt;
use log::{debug, error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::{io, sync::Arc};

use crate::base::bridge::StreamMetrics;
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
    Stream(#[from] crate::base::bridge::Error),
    #[error("Broadcast receiver error {0}")]
    BRecv(#[from] tokio::sync::broadcast::error::RecvError),
}

pub struct Bridge {
    config: Arc<Config>,
    data_tx: Sender<Box<dyn Package>>,
    metrics_tx: Sender<StreamMetrics>,
    /// Actions incoming from backend
    actions_rx: Receiver<Action>,
    /// Action responses going to backend
    action_status: Stream<ActionResponse>,
    /// Used to broadcast actions to all client connections
    actions_tx: Sender<Action>,
    /// Given to client connections for sending action responses
    status_tx: Sender<Payload>,
    /// Process action responses coming in from client connections
    status_rx: Receiver<Payload>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        metrics_tx: Sender<StreamMetrics>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Bridge {
        let (actions_tx, _) = bounded(10);
        let (status_tx, status_rx) = bounded(10);

        Bridge {
            action_status,
            data_tx,
            metrics_tx,
            config,
            actions_rx,
            actions_tx,
            status_tx,
            status_rx,
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let addr = format!("0.0.0.0:{}", self.config.bridge_port);
            let listener = TcpListener::bind(&addr).await?;

            info!("Listening for new connection on {:?}", addr);
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
                }
            }
        }
    }

    async fn spawn_collector(&self, stream: TcpStream) {
        let framed = Framed::new(stream, LinesCodec::new());
        let mut tcp_json = ClientConnection {
            status_tx: self.status_tx.clone(),
            actions_rx: self.actions_rx.clone(),
        };

        tokio::task::spawn(async move {
            if let Err(e) = tcp_json.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
            }

            // tcp_json.close().await;
        });
    }
}

struct ClientConnection {
    status_tx: Sender<Payload>,
    actions_rx: Receiver<Action>,
}

impl ClientConnection {
    pub async fn collect(
        &mut self,
        mut client: Framed<TcpStream, LinesCodec>,
    ) -> Result<(), Error> {
        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    debug!("Received line = {:?}", line);

                    let data = match serde_json::from_str::<Payload>(&line) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    if let Err(e) = self.status_tx.send_async(data).await {
                        error!("Failed to send status to bridge!! Error = {:?}", e);
                    }
                }

                action = self.actions_rx.recv_async() => {
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
