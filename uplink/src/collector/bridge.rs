use tokio::net::{TcpStream, TcpListener};
use tokio::stream::StreamExt;
use tokio_util::codec::{LinesCodec, LinesCodecError};
use tokio_util::codec::Framed;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::{select, time};
use tokio::io::AsyncWriteExt;
use derive_more::From;
use serde::{Serialize, Deserialize};

use std::io;

use crate::base::{Buffer, Package, Partitions, Config};
use std::sync::Arc;
use toml::Value;
use crate::base::actions::{Action, ActionResponse};
use tokio::time::{Duration, Instant};

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    StreamDone,
    Codec(LinesCodecError),
    Json(serde_json::error::Error)
}

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is inturn a json
// TODO which cloud will doubel deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    channel: String,
    #[serde(flatten)]
    payload: Value
}

pub struct Bridge<'bridge> {
    config: Arc<Config>,
    data_rx: TcpStream,
    data_tx: &'bridge mut Sender<Box<dyn Package>>,
    actions: &'bridge mut Receiver<Action>,
    current_action: Option<String>,
}

impl<'bridge> Bridge<'bridge> {
    pub fn new(
        config: Arc<Config>,
        data_tx: &'bridge mut Sender<Box<dyn Package>>,
        data_rx: TcpStream,
        actions: &'bridge mut Receiver<Action>
    ) -> Bridge<'bridge> {
        Bridge {
            config,
            data_tx,
            data_rx,
            actions,
            current_action: None
        }
    }

    pub async fn collect(&mut self) -> Result<(), Error> {
        let channels = self.config.channels.iter().map(|(channel, config)| (channel.to_owned(), config.buf_size as usize)).collect();
        let mut partitions = Partitions::new(self.data_tx.clone(), channels);
        let mut framed = Framed::new(&mut self.data_rx, LinesCodec::new());
        let mut action_timeout = time::delay_for(Duration::from_secs(10));

        loop {
            select! {
                frame = framed.next() => {
                    let frame = frame.ok_or(Error::StreamDone)??;
                    info!("Received line = {}", frame);

                    match self.current_action.take() {
                        Some(id) => debug!("Response for action = {:?}", id),
                        None => {
                            error!("Action timed out already");
                            continue
                        }
                    }

                    let data: Payload = match serde_json::from_str(&frame) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Deserialization error = {:?}", e);
                            continue
                        }
                    };

                    // TODO remove channel clone
                    if let Err(e) = partitions.fill(&data.channel.clone(), data).await {
                        error!("Failed to send data. Error = {:?}", e);
                    }
                }
                action = self.actions.next() => {
                    let action = action.ok_or(Error::StreamDone)?;
                    self.current_action = Some(action.id.to_owned());
                    action_timeout.reset(Instant::now() + Duration::from_secs(10));
                    let data = match serde_json::to_vec(&action) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Serialization error = {:?}", e);
                            continue
                        }
                    };

                    framed.get_mut().write_all(&data).await?;
                    framed.get_mut().write_all(b"\n").await?;
                }
                _ = &mut action_timeout, if self.current_action.is_some() => {
                    let action = self.current_action.take().unwrap();
                    let mut status = ActionResponse::new(&action, "Failed");
                    status.add_error(format!("Action timed out"));
                    if let Err(e) = self.data_tx.send(Box::new(status)).await {
                        error!("Failed to send status. Error = {:?}", e);
                    }
                }
            }
        }
    }
}

pub async fn start(
    config: Arc<Config>,
    mut data_tx: Sender<Box<dyn Package>>,
    mut actions_rx: Receiver<Action>
) -> Result<(), Error> {
    let addr = format!("0.0.0.0:{}", config.bridge_port);
    let mut listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!("Tcp connection error = {:?}", e);
                continue;
            }
        };

        info!("Accepted new connection from {:?}", addr);
        let mut bridge = Bridge::new(config.clone(), &mut data_tx, stream, &mut actions_rx);
        if let Err(e) = bridge.collect().await {
            error!("Bridge failed. Error = {:?}", e);
        }
    }
}

impl Package for Buffer<Payload> {
    fn channel(&self) -> String {
        return self.channel.clone();
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }
}
