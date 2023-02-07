use flume::RecvError;
use log::{error, info, trace, warn};
use serde_json::json;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task::{spawn, JoinHandle};
use tokio::time::interval;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::io;
use std::time::Duration;

use crate::base::bridge::{BridgeTx, Payload};
use crate::base::DeviceShadowConfig;

use super::utils::clock;

const STREAM: &str = "device_shadow";

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
    #[error("Couldn't fill stream")]
    Stream(#[from] crate::base::bridge::Error),
}

#[derive(Debug, Clone)]
pub struct DeviceShadow {
    config: DeviceShadowConfig,
    /// Bridge handle to register apps
    bridge: BridgeTx,
    /// Device shadow payload value
    payload: Payload,
}

impl DeviceShadow {
    pub async fn new(config: DeviceShadowConfig, bridge: BridgeTx) -> Self {
        Self {
            config,
            bridge,
            payload: Payload {
                stream: STREAM.to_owned(),
                device_id: None,
                sequence: 0,
                timestamp: 0,
                payload: json!({}),
            },
        }
    }

    pub async fn start(mut self) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        let mut handle: Option<JoinHandle<()>> = None;

        let mut timeout = interval(Duration::from_secs(self.config.timeout));

        info!("Device Shadow: Waiting to connect on {:?}", addr);
        loop {
            select! {
               v = listener.accept() => {
                    let framed = match v {
                        Ok((stream, addr)) => {
                            info!("Device Shadow: Accepted connection on {addr}");
                            Framed::new(stream, LinesCodec::new())
                        }
                        Err(e) => {
                            error!("Device Shadow: Tcp connection accept error = {e}");
                            continue;
                        }
                    };

                    if let Some(handle) = handle {
                        handle.abort();
                    }

                    let mut tcpjson = self.clone();
                    handle = Some(spawn(async move {
                        if let Err(e) = tcpjson.collect(framed).await {
                            error!("Device Shadow: TcpJson failed, Error = {e}");
                        }
                    }));
                }

                _ = timeout.tick() => {
                    self.payload.timestamp = clock() as u64;
                    self.payload.sequence += 1;

                    self.bridge.send_payload(self.payload.clone()).await
                }

            }
        }
    }

    async fn collect(&mut self, mut client: Framed<TcpStream, LinesCodec>) -> Result<(), Error> {
        loop {
            let line = client.next().await;
            let line = line.ok_or(Error::StreamDone)??;
            trace!("Device Shadow:  Received line = {:?}", line);
            // Store incoming points into payload
            let payload: Payload = serde_json::from_str(&line)?;
            if payload.stream != STREAM {
                warn!("Device Shadow: unexpected data sent to port");
                return Ok(());
            }

            self.payload = payload;
        }
    }
}
