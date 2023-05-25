use std::sync::Arc;

use flume::Receiver;
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::{base, Action, ActionResponse};

use super::bridge::stream::Stream;

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
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
    ) -> Self {
        Self { _config: config, echo_stdout, actions_rx, action_status }
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
    pub async fn start(mut self) {
        while let Ok(action) = self.actions_rx.recv_async().await {
            let action_id = action.action_id.clone();

            // println!("{:?}", keys);
            let keys = match serde_json::from_str(&action.payload) {
                Ok(k) => k,
                Err(e) => {
                    error!("Failed to deserialize keys. Error = {:?}", e);
                    let response = ActionResponse::failure(&action_id, "corruptkeys".to_owned());
                    if let Err(e) = self.action_status.fill(response).await {
                        error!("Couldn't send tunshell action status: {e}")
                    }
                    continue;
                }
            };

            let mut client = Client::new(self.config(keys), HostShell::new().unwrap());
            let mut action_status = self.action_status.clone();

            //TODO(RT): Findout why this is spawned. We want to send other action's with shell?
            tokio::spawn(async move {
                let response = ActionResponse::progress(&action_id, "ShellSpawned", 90);
                if let Err(e) = action_status.fill(response).await {
                    error!("Couldn't send tunshell action status: {e}")
                }

                match client.start_session().compat().await {
                    Ok(status) => {
                        if status != 0 {
                            let response = ActionResponse::failure(&action_id, status.to_string());
                            if let Err(e) = action_status.fill(response).await {
                                error!("Couldn't send tunshell action status: {e}")
                            }
                        } else {
                            log::info!("tunshell exited with status: {}", status);
                            let response = ActionResponse::success(&action_id);
                            if let Err(e) = action_status.fill(response).await {
                                error!("Couldn't send tunshell action status: {e}")
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("tunshell client error: {}", e);
                        let response = ActionResponse::failure(&action_id, e.to_string());
                        if let Err(e) = action_status.fill(response).await {
                            error!("Couldn't send tunshell action status: {e}")
                        }
                    }
                };
            });
        }
    }
}
