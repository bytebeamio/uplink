use std::sync::{Arc, Mutex};

use async_channel::{Receiver, Sender};
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::base::{
    actions::{ActionResponse, Package},
    Bucket,
};

pub struct Relay {
    host:     String,
    tls_port: u16,
    ws_port:  u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session:    String,
    encryption: String,
}

pub struct TunshellSession {
    relay:             Relay,
    echo_stdout:       bool,
    keys_rx:           Receiver<String>,
    status_bucket:     Bucket<ActionResponse>,
    last_process_done: Arc<Mutex<bool>>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Tunshell error {0}")]
    Tunshell(#[from] anyhow::Error),
    #[error("Already another tunshell session running")]
    Busy,
    #[error("Non-zero exit error {0}")]
    NonZeroExit(u8),
}

impl TunshellSession {
    pub fn new(
        relay: Relay,
        echo_stdout: bool,
        tunshell_keys_rx: Receiver<String>,
        collector_tx: Sender<Box<dyn Package>>,
    ) -> Self {
        Self {
            relay,
            echo_stdout,
            keys_rx: tunshell_keys_rx,
            status_bucket: Bucket::new(collector_tx, "tunshell_status", 1),
            last_process_done: Arc::new(Mutex::new(true)),
        }
    }

    fn config(&self, keys: Keys) -> Config {
        Config::new(
            ClientMode::Target,
            &keys.session,
            &self.relay.host,
            self.relay.tls_port,
            self.relay.ws_port,
            &keys.encryption,
            true,
            self.echo_stdout,
        )
    }

    pub async fn start(mut self) -> Result<(), Error> {
        while let Ok(keys) = self.keys_rx.recv().await {
            if *self.last_process_done.lock().unwrap() == false {
                if let Err(e) = self.status_bucket.fill(ActionResponse::failure("tunshell", Error::Busy.to_string())).await {
                    error!("Failed to send status, Error = {:?}", e);
                };
                continue;
            }

            let keys = serde_json::from_str(&keys)?;
            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());

            let last_process_done = self.last_process_done.clone();
            let mut status_tx = self.status_bucket.clone();

            tokio::spawn(async move {
                *last_process_done.lock().unwrap() = false;
                let res = client.start_session().compat().await;
                *last_process_done.lock().unwrap() = true;

                let send_status = match res {
                    Ok(status) => {
                        if status != 0 {
                            status_tx.fill(ActionResponse::success("tunshell")).await
                        } else {
                            status_tx.fill(ActionResponse::failure("tunshell", Error::NonZeroExit(status).to_string())).await
                        }
                    }
                    Err(e) => status_tx.fill(ActionResponse::failure("tunshell", e.to_string())).await,
                };

                if let Err(e) = send_status {
                    error!("Failed to send status. Error {:?}", e);
                }
            });
        }
        Ok(())
    }
}

impl Default for Relay {
    fn default() -> Self {
        Relay { host: "eu.relay.tunshell.com".to_string(), tls_port: 5000, ws_port: 443 }
    }
}
