use flume::{Receiver, RecvError};
use futures_util::SinkExt;
use log::{debug, error, info};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::task::{spawn, JoinHandle};
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use std::io;

use crate::base::bridge::BridgeTx;
use crate::config::AppConfig;
use crate::{Action, ActionResponse, Payload};

/// Custom error type to manage various errors that can occur within the TcpJson server
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
}

/// TcpJson server responsible for managing JSON-encoded messages over a TCP connection
#[derive(Debug, Clone)]
pub struct TcpJson {
    // Name identifier for the server instance
    name: String,
    // Application-specific configuration
    config: AppConfig,
    // Bridge for sending/receiving actions and payloads
    bridge: BridgeTx,
    // Optional receiver for incoming `Action`s
    actions_rx: Option<Receiver<Action>>,
}

impl TcpJson {
    /// Creates a new TcpJson server instance with the specified configuration and bridge
    pub fn new(
        name: String,
        config: AppConfig,
        actions_rx: Option<Receiver<Action>>,
        bridge: BridgeTx,
    ) -> TcpJson {
        TcpJson { name, config, bridge, actions_rx }
    }

    /// Starts the TcpJson server and listens for incoming TCP connections
    pub async fn start(self) {
        let addr = format!("0.0.0.0:{}", self.config.port);
        // Try binding the TCP listener, retrying every 5 seconds on failure
        let listener = loop {
            match TcpListener::bind(&addr).await {
                Ok(s) => break s,
                Err(e) => {
                    error!(
                        "Couldn't bind to port: {}; Error = {e}; retrying in 5s",
                        self.config.port
                    );
                    sleep(Duration::from_secs(5)).await;
                }
            }
        };

        let mut handle: Option<JoinHandle<()>> = None;

        info!("Waiting for app = {} to connect on {addr}", self.name);

        // Main loop: waits for incoming connections and spawns a new task to handle each one
        loop {
            let framed = match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted connection from app = {} on {addr}", self.name);
                    Framed::new(stream, LinesCodec::new())
                }
                Err(e) => {
                    error!("Tcp connection accept error = {e}, app = {}", self.name);
                    continue;
                }
            };

            // Abort any previous task if a new connection is accepted
            if let Some(handle) = handle {
                handle.abort();
            }

            let tcpjson = self.clone();
            handle = Some(spawn(async move {
                if let Err(e) = tcpjson.collect(framed).await {
                    error!("TcpJson failed. app = {}, Error = {e}", tcpjson.name);
                }
            }));
        }
    }

    /// Collects data from the client, handling both incoming actions and received lines
    async fn collect(&self, mut client: Framed<TcpStream, LinesCodec>) -> Result<(), Error> {
        // If actions are enabled, handle them in a select loop
        let Some(actions_rx) = self.actions_rx.as_ref() else {
            // Simple loop if actions are disabled; handles incoming lines only
            while let Some(line) = client.next().await {
                let line = line?;
                if let Err(e) = self.handle_incoming_line(line).await {
                    error!("Error handling incoming line = {e}, app = {}", self.name);
                }
            }

            return Err(Error::StreamDone);
        };

        loop {
            select! {
                line = client.next() => {
                    let line = line.ok_or(Error::StreamDone)??;
                    if let Err(e) = self.handle_incoming_line(line).await {
                        error!("Error handling incoming line = {e}, app = {}", self.name);
                    }
                }
                action = actions_rx.recv_async() => {
                    let action = action?;
                    match serde_json::to_string(&action) {
                        Ok(data) => client.send(data).await?,
                        Err(e) => {
                            error!("Serialization error = {e}, app = {}", self.name);
                            continue
                        }
                    }
                }
            }
        }
    }

    /// Processes each incoming line of data from the client
    async fn handle_incoming_line(&self, line: String) -> Result<(), Error> {
        debug!("{}: Received line = {line:?}", self.name);

        // Deserialize the line into a `Payload`
        let data = serde_json::from_str::<Payload>(&line)?;

        // If the stream is for "action_status", treat it as an `ActionResponse`
        if data.stream == "action_status" {
            let response = ActionResponse::from_payload(&data)?;
            self.bridge.send_action_response(response).await;
            return Ok(());
        }

        // Otherwise, forward the payload data through the bridge
        self.bridge.send_payload(data).await;
        Ok(())
    }
}
