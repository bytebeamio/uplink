use futures_util::SinkExt;
use log::{error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::{io::AsyncWriteExt, sync::RwLock};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::{io, sync::Arc};

use crate::base::serializer::Status;
use crate::Config;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Stream done")]
    StreamDone,
    #[error("Lines codec error {0}")]
    Codec(#[from] LinesCodecError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

pub struct TcpStatus {
    config: Arc<Config>,
    status: Arc<RwLock<Status>>,
}

impl TcpStatus {
    pub fn new(config: Arc<Config>, status: Arc<RwLock<Status>>) -> Self {
        Self { config, status }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        let status_port = match self.config.status_port {
            Some(p) => p,
            _ => return Ok(()),
        };
        loop {
            let addr = format!("0.0.0.0:{}", status_port);
            let listener = TcpListener::bind(&addr).await?;

            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("Accepted new connection from {:?}", addr);
                        self.spawn_to_respond(stream).await
                    }
                    Err(e) => {
                        error!("Tcp connection accept error = {:?}", e);
                        continue;
                    }
                }
            }
        }
    }

    async fn spawn_to_respond(&self, stream: TcpStream) {
        let mut framed = Framed::new(stream, LinesCodec::new());
        let status = self.status.clone();
        tokio::task::spawn(async move {
            let status = status.read().await.to_string();
            if let Err(e) = framed.send(status).await {
                error!("Error sending line = {:?}", e);
            }
            if let Err(e) = framed.into_inner().shutdown().await {
                error!("Error shutting down TCP stream = {:?}", e);
            }
        });
    }
}
