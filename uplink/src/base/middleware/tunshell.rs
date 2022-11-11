use std::sync::{Arc, Mutex};

use flume::{Receiver, Sender};
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::{
    base::{self, middleware::ActionResponse},
    Action, Payload,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}

pub struct TunshellSession {
    _config: Arc<base::Config>,
    echo_stdout: bool,
    actions_rx: Receiver<Action>,
    data_tx: Sender<Payload>,
    last_process_done: Arc<Mutex<bool>>,
}

impl TunshellSession {
    pub fn new(
        config: Arc<base::Config>,
        echo_stdout: bool,
        tunshell_rx: Receiver<Action>,
        data_tx: Sender<Payload>,
    ) -> Self {
        Self {
            _config: config,
            echo_stdout,
            actions_rx: tunshell_rx,
            data_tx,
            last_process_done: Arc::new(Mutex::new(true)),
        }
    }

    fn config(&self, keys: Keys) -> Config {
        Config::new(
            ClientMode::Target,
            &keys.session,
            &keys.relay,
            5000,
            443,
            &keys.encryption,
            true,
            self.echo_stdout,
        )
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        while let Ok(action) = self.actions_rx.recv_async().await {
            let action_id = action.action_id.clone();
            if !(*self.last_process_done.lock().unwrap()) {
                let status = ActionResponse::failure(&action_id, "busy".to_owned());
                if let Err(e) = self.data_tx.send_async(status.as_payload()).await {
                    error!("Failed to send status, Error = {:?}", e);
                };

                continue;
            }

            // println!("{:?}", keys);
            let keys = match serde_json::from_str(&action.payload) {
                Ok(k) => k,
                Err(e) => {
                    error!("Failed to deserialize keys. Error = {:?}", e);
                    let status = ActionResponse::failure(&action_id, "corruptkeys".to_owned());
                    if let Err(e) = self.data_tx.send_async(status.as_payload()).await {
                        error!("Failed to send status, Error = {:?}", e);
                    };

                    continue;
                }
            };

            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());
            let last_process_done = self.last_process_done.clone();
            let status_tx = self.data_tx.clone();

            tokio::spawn(async move {
                *last_process_done.lock().unwrap() = false;
                let response = ActionResponse::progress(&action_id, "ShellSpawned", 100);
                if let Err(e) = status_tx.send_async(response.as_payload()).await {
                    error!("Failed to send status. Error {:?}", e);
                }

                let send_status = match client.start_session().compat().await {
                    Ok(status) => {
                        if status != 0 {
                            let response = ActionResponse::failure(&action_id, status.to_string());
                            status_tx.send_async(response.as_payload()).await
                        } else {
                            log::info!("tunshell exited with status: {}", status);
                            let response = ActionResponse::success(&action_id);
                            status_tx.send_async(response.as_payload()).await
                        }
                    }
                    Err(e) => {
                        log::warn!("tunshell client error: {}", e);
                        let response = ActionResponse::failure(&action_id, e.to_string());
                        status_tx.send_async(response.as_payload()).await
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
