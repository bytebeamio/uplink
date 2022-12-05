use flume::{Receiver, Sender, TrySendError};
use log::{debug, error};
use thiserror::Error;

use std::sync::Arc;

mod process;
pub mod tunshell;

use crate::{Action, ActionResponse, Config, Stream};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Process error {0}")]
    Process(#[from] process::Error),
    #[error("Error forwarding Action {0}")]
    Send(#[from] flume::SendError<Action>),
    #[error("Error forwarding Action {0}")]
    TrySend(#[from] flume::TrySendError<Action>),
    #[error("Invalid action")]
    InvalidActionKind(String),
    #[error("Another File is downloading")]
    Downloading,
}

pub struct Middleware {
    config: Arc<Config>,
    action_status: Stream<ActionResponse>,
    process: process::Process,
    actions_rx: Receiver<Action>,
    tunshell_tx: Sender<Action>,
    download_tx: Sender<Action>,
    log_tx: Sender<Action>,
    bridge_tx: Sender<Action>,
}

impl Middleware {
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        tunshell_tx: Sender<Action>,
        download_tx: Sender<Action>,
        log_tx: Sender<Action>,
        action_status: Stream<ActionResponse>,
        bridge_tx: Sender<Action>,
    ) -> Self {
        let process = process::Process::new(action_status.clone());
        Self {
            config,
            action_status,
            process,
            actions_rx,
            tunshell_tx,
            download_tx,
            log_tx,
            bridge_tx,
        }
    }

    /// Start receiving and processing [Action]s
    pub async fn start(mut self) {
        loop {
            let action = match self.actions_rx.recv_async().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Action stream receiver error = {:?}", e);
                    break;
                }
            };

            debug!("Action = {:?}", action);

            let action_id = action.action_id.clone();
            let action_name = action.name.clone();
            let error = match self.handle(action).await {
                Ok(_) => continue,
                Err(e) => e,
            };

            self.forward_action_error(&action_id, &action_name, error).await;
        }
    }

    /// Handle received actions
    async fn handle(&mut self, action: Action) -> Result<(), Error> {
        match action.name.as_ref() {
            "launch_shell" => {
                self.tunshell_tx.send_async(action).await?;
                return Ok(());
            }
            #[cfg(target_os = "linux")]
            "configure_journalctl" => {
                self.log_tx.send_async(action).await?;
                return Ok(());
            }
            #[cfg(target_os = "android")]
            "configure_logcat" => {
                self.log_tx.send_async(action).await?;
                return Ok(());
            }
            "update_firmware" | "send_file" if self.config.download_path.is_some() => {
                // if action can't be sent, Error out and notify cloud
                self.download_tx.try_send(action).map_err(|e| match e {
                    TrySendError::Full(_) => Error::Downloading,
                    e => Error::TrySend(e),
                })?;
                return Ok(());
            }
            _ => (),
        }

        // Bridge actions are forwarded
        if !self.config.actions.contains(&action.name) {
            self.bridge_tx.try_send(action)?;
            return Ok(());
        }

        // Regular actions are executed natively
        match action.kind.as_ref() {
            "process" => {
                let command = action.name.clone();
                let payload = action.payload.clone();
                let id = action.action_id;

                self.process.execute(id.clone(), command.clone(), payload).await?;
            }
            v => return Err(Error::InvalidActionKind(v.to_owned())),
        }

        Ok(())
    }

    async fn forward_action_error(&mut self, id: &str, action: &str, error: Error) {
        error!("Failed to execute. Command = {:?}, Error = {:?}", action, error);
        let status = ActionResponse::failure(id, error.to_string());

        if let Err(e) = self.action_status.fill(status).await {
            error!("Failed to send status. Error = {:?}", e);
        }
    }
}
