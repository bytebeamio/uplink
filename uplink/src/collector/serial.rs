use std::{io, time::Duration};

use tokio_serial::SerialStream;

use crate::base::bridge::BridgeTx;

#[derive(Debug, Clone)]
pub struct Serial {
    name: String,
    /// Bridge handle to register apps
    bridge: BridgeTx,
}

impl Serial {
    pub fn new(name: String, bridge: BridgeTx) -> Serial {
        // let actions_rx = bridge.register_action_routes(&config.actions).await;

        // Note: We can register `Serial` itself as an app to direct actions to it
        Serial { name, bridge }
    }

    pub async fn start(self) -> Result<(), Error> {
        dbg!();
        let s = tokio_serial::new("/dev/ttyS1", 9600);
        dbg!();
        let mut s = SerialStream::open(&s).unwrap();
        dbg!();

        let mut buf = vec![0; 1024];
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            dbg!();
            s.readable().await.unwrap();
            dbg!();
            let n = s.try_read(buf.as_mut_slice()).unwrap();
            println!("Read {} bytes", n);
            println!("{:?}", buf);
            buf.clear();
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Stream done")]
    StreamDone,
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
    #[error("Couldn't fill stream")]
    Stream(#[from] crate::base::bridge::Error),
    #[error("Serial error")]
    Serial(#[from] tokio_serial::Error),
}
