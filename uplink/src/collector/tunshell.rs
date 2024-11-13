use flume::Receiver;
use log::error;
use serde::{Deserialize, Serialize};
use tokio_compat_02::FutureExt; // Compatibility wrapper for async functionality
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

/// Holds session-specific keys for establishing a Tunshell session
#[derive(Debug, Serialize, Deserialize)]
pub struct Keys {
    session: String,
    relay: String,
    encryption: String,
}

/// TunshellClient is responsible for managing Tunshell sessions based on incoming actions
#[derive(Debug, Clone)]
pub struct TunshellClient {
    // Receives actions that may trigger a Tunshell session
    actions_rx: Receiver<Action>,
    // Used for sending action responses/status back to the bridge
    bridge: BridgeTx,
}

impl TunshellClient {
    /// Creates a new TunshellClient with the provided action receiver and bridge transmitter
    pub fn new(actions_rx: Receiver<Action>, bridge: BridgeTx) -> Self {
        Self { actions_rx, bridge }
    }

    /// Configures Tunshell client with session keys and connection parameters
    fn config(&self, keys: Keys) -> Config {
        Config::new(
            ClientMode::Target,
            &keys.session,    // Client key
            &keys.relay,      // Relay host
            5000,             // Relay port
            443,              // Websocket port
            &keys.encryption, // Encryption key
            true,             // Enable direct connection
            false,            // Disable echo to stdout
        )
    }

    /// Starts listening for actions that may trigger a Tunshell session
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(self) {
        while let Ok(action) = self.actions_rx.recv_async().await {
            let session = self.clone();
            // Spawn a separate task to handle each session
            tokio::spawn(async move {
                if let Err(e) = session.session(&action).await {
                    error!("{e}");
                    // Send a failure response if the session fails
                    let status = ActionResponse::failure(&action.action_id, e.to_string());
                    session.bridge.send_action_response(status).await;
                }
            });
        }
    }

    /// Manages the lifecycle of a Tunshell session, including configuration, start, and completion handling
    async fn session(&self, action: &Action) -> Result<(), Error> {
        let action_id = action.action_id.clone();

        // Deserialize keys from the action payload
        let keys: Keys = serde_json::from_str(&action.payload)?;
        let mut client = Client::new(self.config(keys), HostShell::new().unwrap());

        // Notify progress once the shell is spawned
        let response = ActionResponse::progress(&action_id, "ShellSpawned", 90);
        self.bridge.send_action_response(response).await;

        // Start the Tunshell session and await its completion
        match client.start_session().compat().await? {
            0 => log::info!("Tunshell session ended successfully"),
            // Return an error if the session ends with a non-zero status
            status => return Err(Error::UnexpectedStatus(status)),
        }

        Ok(())
    }
}
