use flume::{Receiver, Sender};
use log::{debug, error, info};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use std::sync::Arc;

use super::{Collector, CollectorError, CollectorInterface, Payload};
use crate::base::actions::{Action, ActionResponse};
use crate::base::{Config, Package, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Receiver error {0}")]
    Recv(#[from] flume::RecvError),
}

struct TcpCollector {
    inner: Framed<TcpStream, LinesCodec>,
}

#[async_trait::async_trait]
impl CollectorInterface for TcpCollector {
    async fn write(&mut self, action: Action) -> Result<(), CollectorError> {
        let data = match serde_json::to_vec(&action) {
            Ok(d) => d,
            Err(e) => {
                error!("Serialization error = {:?}", e);
                return Ok(());
            }
        };

        self.inner.get_mut().write_all(&data).await?;
        self.inner.get_mut().write_all(b"\n").await?;

        Ok(())
    }

    async fn read(&mut self) -> Result<Payload, CollectorError> {
        let frame = self.inner.next().await.ok_or(CollectorError::StreamDone)??;
        debug!("Received line = {:?}", frame);

        let data: Payload = serde_json::from_str(&frame)?;

        Ok(data)
    }
}

pub struct Bridge {
    config: Arc<Config>,
    collector: Collector,
    actions_rx: Receiver<Action>,
    action_status: Stream<ActionResponse>,
}

impl Bridge {
    pub fn new(
        config: Arc<Config>,
        data_tx: Sender<Box<dyn Package>>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Self {
        Self {
            collector: Collector::new(
                config.clone(),
                data_tx,
                actions_rx.clone(),
                action_status.clone(),
            ),
            config,
            actions_rx,
            action_status,
        }
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
                        if let Err(e) = self.action_status.fill(status).await {
                            error!("Failed to send busy status. Error = {:?}", e);
                        }
                    }
                }
            };

            info!("Accepted new connection from {:?}", addr);
            let framed = TcpCollector { inner: Framed::new(stream, LinesCodec::new()) };
            if let Err(e) = self.collector.collect(framed).await {
                error!("Bridge failed. Error = {:?}", e);
            }
        }
    }
}
