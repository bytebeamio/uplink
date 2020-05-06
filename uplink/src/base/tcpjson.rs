use tokio::task;
use tokio::net::{TcpStream, TcpListener};
use tokio::stream::StreamExt;
use tokio_util::codec::LinesCodec;
use tokio_util::codec::Framed;
use tokio::sync::mpsc::Sender;
use derive_more::From;
use serde::{Serialize, Deserialize};
use serde_json::Value;

use std::io;

use crate::base::{Buffer, Package, Partitions};

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error)
}

pub async fn start(tx: Sender<Box<dyn Package>>) -> Result<(), Error> {
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
        let tx = tx.clone();
        task::spawn(async move {
            collect(stream, tx.clone()).await;
        });
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Required {
    channel: String,
    timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    #[serde(flatten)]
    required: Required,
    #[serde(flatten)]
    payload: Value 
}

pub async fn collect(stream: TcpStream, tx: Sender<Box<dyn Package>>) {
    // TODO replace with correct channels
    let channels = vec![("battery_general_info".to_owned(), 1)];
    let mut partitions = Partitions::new(tx, channels);
    let mut framed = Framed::new(stream, LinesCodec::new());

    loop {
        let frame = match framed.next().await {
            Some(frame) => frame,
            None => {
                info!("Done with the connection!!");
                return
            }
        };

        let frame = match frame {
            Ok(frame) => frame,
            Err(e) => {
                error!("Codec error = {:?}", e);
                return
            }
        };

        info!("Received line = {}", frame);
        let data: Payload = match serde_json::from_str(&frame) {
            Ok(data) => data,
            Err(e) => {
                error!("Invalid json. Error = {:?}", e);
                continue;
            }
        };

        if let Err(e) = partitions.fill(&data.required.channel.clone(), data).await {
            error!("Failed to send data. Error = {:?}", e);
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
