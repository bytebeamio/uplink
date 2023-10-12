use flume::Receiver;
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt;
use tunshell_client::{Client, ClientMode, Config, HostShell};

use crate::{base::bridge::BridgeTx, Action, ActionResponse};

use super::ActionsLogWriter;

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
    actions_log: ActionsLogWriter,
}

impl TunshellClient {
    pub fn new(
        actions_rx: Receiver<Action>,
        bridge: BridgeTx,
        actions_log: ActionsLogWriter,
    ) -> Self {
        Self { actions_rx, bridge, actions_log }
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
            let mut session = self.clone();
            //TODO(RT): Findout why this is spawned. We want to send other action's with shell?
            tokio::spawn(async move {
                if let Err(e) = session.session(&action).await {
                    session.actions_log.update_stage("Failed");
                    let msg = e.to_string();
                    error!("{msg}");
                    session.actions_log.update_message(&msg);
                    session.actions_log.commit_entry();

                    let status = ActionResponse::failure(&action.action_id, msg);
                    session.bridge.send_action_response(status).await;
                }
            });
        }
    }

    async fn session(&mut self, action: &Action) -> Result<(), Error> {
        let action_id = action.action_id.clone();

        // println!("{:?}", keys);
        let keys = serde_json::from_str(&action.payload)?;
        let mut client = Client::new(self.config(keys), HostShell::new().unwrap());

        let stage = "ShellSpawned";
        let response = ActionResponse::progress(&action_id, stage, 90);
        self.actions_log.accept_action(&action_id, stage);
        self.actions_log.commit_entry();

        self.bridge.send_action_response(response).await;

        let status = client.start_session().compat().await?;
        if status != 0 {
            Err(Error::UnexpectedStatus(status))
        } else {
            self.actions_log.update_stage("Completed");
            let msg = "Tunshell session ended successfully";
            log::info!("{msg}");
            self.actions_log.update_message(msg);
            self.actions_log.commit_entry();
            Ok(())
        }
    }
}
