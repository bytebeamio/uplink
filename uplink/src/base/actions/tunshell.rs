use std::sync::{Arc, Mutex};

use flume::Receiver;
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::base::{self, actions::ActionResponse, Stream};

pub struct Relay {
    host: String,
    tls_port: u16,
    ws_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    encryption: String,
}

pub struct TunshellSession {
    _config: Arc<base::Config>,
    relay: Relay,
    echo_stdout: bool,
    keys_rx: Receiver<String>,
    action_status: Stream<ActionResponse>,
    last_process_done: Arc<Mutex<bool>>,
}

impl TunshellSession {
    pub fn new(
        config: Arc<base::Config>,
        relay: Relay,
        echo_stdout: bool,
        tunshell_rx: Receiver<String>,
        action_status: Stream<ActionResponse>,
    ) -> Self {
        Self {
            _config: config,
            relay,
            echo_stdout,
            keys_rx: tunshell_rx,
            action_status,
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

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        while let Ok(keys) = self.keys_rx.recv_async().await {
            if !(*self.last_process_done.lock().unwrap()) {
                let status = ActionResponse::failure("tunshell", "busy".to_owned());
                if let Err(e) = self.action_status.fill(status).await {
                    error!("Failed to send status, Error = {:?}", e);
                };

                continue;
            }

            // println!("{:?}", keys);
            let keys = match serde_json::from_str(&keys) {
                Ok(k) => k,
                Err(e) => {
                    error!("Failed to deserialize keys. Error = {:?}", e);
                    let status = ActionResponse::failure("tunshell", "corruptkeys".to_owned());
                    if let Err(e) = self.action_status.fill(status).await {
                        error!("Failed to send status, Error = {:?}", e);
                    };

                    continue;
                }
            };

            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());
            let last_process_done = self.last_process_done.clone();
            let mut status_tx = self.action_status.clone();

            tokio::spawn(async move {
                *last_process_done.lock().unwrap() = false;

                let send_status = match client.start_session().compat().await {
                    Ok(status) => {
                        if status != 0 {
                            let response = ActionResponse::failure("tunshell", status.to_string());
                            status_tx.fill(response).await
                        } else {
                            status_tx.fill(ActionResponse::success("tunshell")).await
                        }
                    }
                    Err(e) => {
                        status_tx.fill(ActionResponse::failure("tunshell", e.to_string())).await
                    }
                };

                if let Err(e) = send_status {
                    error!("Failed to send status. Error {:?}", e);
                }

                *last_process_done.lock().unwrap() = true;
            });
        }
    }
}

impl Default for Relay {
    fn default() -> Self {
        Relay { host: "eu.relay.tunshell.com".to_string(), tls_port: 5000, ws_port: 443 }
    }
}
