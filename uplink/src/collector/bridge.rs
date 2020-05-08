use tokio::net::{TcpStream, TcpListener};
use tokio::stream::StreamExt;
use tokio_util::codec::{LinesCodec, LinesCodecError};
use tokio_util::codec::Framed;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::select;
use tokio::io::AsyncWriteExt;
use derive_more::From;
use serde::{Serialize, Deserialize};

use std::io;

use crate::base::{Buffer, Package, Partitions, Config};
use std::sync::Arc;

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    StreamDone,
    Codec(LinesCodecError),
    Json(serde_json::error::Error)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    channel: String,
    #[serde(flatten)]
    payload: Vec<u8>
}

pub struct Bridge<'bridge> {
    config: Arc<Config>,
    data_rx: TcpStream,
    data_tx: &'bridge mut Sender<Box<dyn Package>>,
    actions: &'bridge mut Receiver<Vec<u8>>,
}

impl<'bridge> Bridge<'bridge> {
    pub fn new(
        config: Arc<Config>,
        data_tx: &'bridge mut Sender<Box<dyn Package>>,
        data_rx: TcpStream,
        actions: &'bridge mut Receiver<Vec<u8>>
    ) -> Bridge<'bridge> {
        Bridge {
            config,
            data_tx,
            data_rx,
            actions
        }
    }

    pub async fn collect(&mut self) -> Result<(), Error> {
        let channels = self.config.channels.iter().map(|(channel, config)| (channel.to_owned(), config.buf_size as usize)).collect();
        let mut partitions = Partitions::new(self.data_tx.clone(), channels);
        let mut framed = Framed::new(&mut self.data_rx, LinesCodec::new());

        loop {
            select! {
                frame = framed.next() => {
                    let frame = frame.ok_or(Error::StreamDone)??;
                    info!("Received line = {}", frame);
                    let data: Payload = serde_json::from_str(&frame)?;
                    // TODO remove channel clone
                    if let Err(e) = partitions.fill(&data.channel.clone(), data).await {
                        error!("Failed to send data. Error = {:?}", e);
                    }
                }
                action = self.actions.next() => {
                    let action = action.ok_or(Error::StreamDone)?;
                    framed.get_mut().write_all(&action).await?;
                    framed.get_mut().write_all(b"\n").await?;
                }
            }
        }
    }
}

pub async fn start(
    config: Arc<Config>,
    mut data_tx: Sender<Box<dyn Package>>,
    mut actions_rx: Receiver<Vec<u8>>
) -> Result<(), Error> {
    let mut listener = TcpListener::bind("0.0.0.0:5555").await?;
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
