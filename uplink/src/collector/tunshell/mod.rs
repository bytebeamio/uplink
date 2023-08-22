use std::sync::Arc;

use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::{
    base::{self, bridge::BridgeTx, ActionRoute},
    ActionResponse,
};

mod metrics;

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}

pub struct TunshellSession {
    _config: Arc<base::Config>,
    bridge: BridgeTx,
}

impl TunshellSession {
    pub fn new(config: Arc<base::Config>, bridge: BridgeTx) -> Self {
        Self { _config: config, bridge }
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
            false,
        )
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        let route = ActionRoute { name: "launch_shell".to_owned(), timeout: 10 };
        let actions_rx = self.bridge.register_action_route(route).await;

        while let Ok(action) = actions_rx.recv_async().await {
            let action_id = action.action_id.clone();

            // println!("{:?}", keys);
            let keys = match serde_json::from_str(&action.payload) {
                Ok(k) => k,
                Err(e) => {
                    error!("Failed to deserialize keys. Error = {:?}", e);
                    let status = ActionResponse::failure(&action_id, "corruptkeys".to_owned());
                    self.bridge.send_action_response(status).await;
                    continue;
                }
            };

            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());
            let status_tx = self.bridge.clone();

            //TODO(RT): Findout why this is spawned. We want to send other action's with shell?
            tokio::spawn(async move {
                let response = ActionResponse::progress(&action_id, "ShellSpawned", 90);
                status_tx.send_action_response(response).await;

                match client.start_session().compat().await {
                    Ok(status) => {
                        if status != 0 {
                            let response = ActionResponse::failure(&action_id, status.to_string());
                            status_tx.send_action_response(response).await;
                        } else {
                            log::info!("tunshell exited with status: {}", status);
                            let response = ActionResponse::success(&action_id);
                            status_tx.send_action_response(response).await;
                        }
                    }
                    Err(e) => {
                        log::warn!("tunshell client error: {}", e);
                        let response = ActionResponse::failure(&action_id, e.to_string());
                        status_tx.send_action_response(response).await;
                    }
                };
            });
        }
    }
}
