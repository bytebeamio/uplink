use flume::Receiver;
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::{base::bridge::BridgeTx, Action, ActionResponse};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to deserialize keys. Error = {0}")]
    Serde(#[from] serde_json::Error),
    #[error("TunshellClient client Error = {0}")]
    TunshellClient(#[from] anyhow::Error),
    #[error("TunshellClient exited with unexpected status: {0}")]
    UnexpectedStatus(u8),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}

#[derive(Debug, Clone)]
pub struct TunshellClient {
    actions_rx: Receiver<Action>,
    bridge: BridgeTx,
}

impl TunshellClient {
    pub fn new(actions_rx: Receiver<Action>, bridge: BridgeTx) -> Self {
        Self { actions_rx, bridge }
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
        while let Ok(action) = self.actions_rx.recv_async().await {
            let session = self.clone();
            //TODO(RT): Findout why this is spawned. We want to send other action's with shell?
            tokio::spawn(async move {
                if let Err(e) = session.session(&action).await {
                    error!("{}", e.to_string());
                    let status = ActionResponse::failure(&action.action_id, e.to_string());
                    session.bridge.send_action_response(status).await;
                }
            });
        }
    }

    async fn session(&self, action: &Action) -> Result<(), Error> {
        let action_id = action.action_id.clone();

        // println!("{:?}", keys);
        let keys = serde_json::from_str(&action.payload)?;
        let mut client = Client::new(self.config(keys), HostShell::new().unwrap());

        let response = ActionResponse::progress(&action_id, "ShellSpawned", 90);
        self.bridge.send_action_response(response).await;

        let status = client.start_session().compat().await?;
        if status != 0 {
            Err(Error::UnexpectedStatus(status))
        } else {
            log::info!("Tunshell session ended successfully");
            Ok(())
        }
    }
}
