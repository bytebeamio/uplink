use std::{io, thread, time::Duration};

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

    pub fn start(self) -> Result<(), Error> {
        // initialize a serial port using serialport crate
        //
        //
        dbg!();
        let mut s = serialport::new("/dev/ttyS1", 115_200)
            .timeout(Duration::from_millis(10000))
            .open()
            .expect("Failed to open port");

        s.set_timeout(Duration::from_secs(1000)).unwrap();
        let mut buf = vec![0; 1024];
        loop {
            thread::sleep(Duration::from_millis(100));
            dbg!();
            let n = s.read(buf.as_mut_slice()).unwrap();
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
