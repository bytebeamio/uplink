use futures_util::SinkExt;
use log::{error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::{io, sync::Arc};

use crate::{Config, SerializerState};

#[derive(thiserror::Error, Debug)]
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
    serializer_state: Arc<RwLock<SerializerState>>,
}

impl TcpStatus {
    pub fn new(config: Arc<Config>, serializer_state: Arc<RwLock<SerializerState>>) -> Self {
        Self { config, serializer_state }
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
        let framed = Framed::new(stream, LinesCodec::new());
        let serializer_state = self.serializer_state.clone();

        tokio::task::spawn(async move {
            if let Err(e) = write_status(serializer_state, framed).await {
                error!("Error writing status = {:?}", e);
            }
        });
    }
}

async fn write_status(
    serializer_state: Arc<RwLock<SerializerState>>,
    mut framed: Framed<TcpStream, LinesCodec>,
) -> Result<(), Error> {
    let serializer_state = &*serializer_state.read().await;
    let status = serde_json::to_string(serializer_state)?;
    framed.send(status).await?;

    Ok(())
}
