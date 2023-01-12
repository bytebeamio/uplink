use std::sync::Arc;

use flume::Receiver;
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::{
    base::{self, Stream},
    Action, ActionResponse,
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
    action_status: Stream<ActionResponse>,
}

impl TunshellSession {
    pub fn new(
        config: Arc<base::Config>,
        echo_stdout: bool,
        tunshell_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Self {
        Self { _config: config, echo_stdout, actions_rx: tunshell_rx, action_status }
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
    #[tracing::instrument(name = "Tunshell", skip_all)]
    pub async fn start(mut self) {
        while let Ok(action) = self.actions_rx.recv_async().await {
            let action_id = action.action_id.clone();

            // println!("{:?}", keys);
            let keys = match serde_json::from_str(&action.payload) {
                Ok(k) => k,
                Err(e) => {
                    error!("Failed to deserialize keys. Error = {:?}", e);
                    let status = ActionResponse::failure(&action_id, "corruptkeys".to_owned());
                    if let Err(e) = self.action_status.fill(status).await {
                        error!("Failed to send status, Error = {:?}", e);
                    };

                    continue;
                }
            };

            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());
            let mut status_tx = self.action_status.clone();

            tokio::spawn(async move {
                let response = ActionResponse::progress(&action_id, "ShellSpawned", 100);
                if let Err(e) = status_tx.fill(response).await {
                    error!("Failed to send status. Error {:?}", e);
                }

                let send_status = match client.start_session().compat().await {
                    Ok(status) => {
                        if status != 0 {
                            let response = ActionResponse::failure(&action_id, status.to_string());
                            status_tx.fill(response).await
                        } else {
                            log::info!("tunshell exited with status: {}", status);
                            status_tx.fill(ActionResponse::success(&action_id)).await
                        }
                    }
                    Err(e) => {
                        log::warn!("tunshell client error: {}", e);
                        status_tx.fill(ActionResponse::failure(&action_id, e.to_string())).await
                    }
                };

                if let Err(e) = send_status {
                    error!("Failed to send status. Error {:?}", e);
                }
            });
        }
    }
}
