use async_channel::{Receiver, Sender};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::HashMap;
use std::sync::Arc;

use super::{Config, Control, Package, Stream};

pub mod controller;
mod process;
mod response;
pub mod tunshell;

pub use controller::Controller;
pub use response::ActionResponse;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Process error {0}")]
    Process(#[from] process::Error),
    #[error("Controller error {0}")]
    Controller(#[from] controller::Error),
    #[error("Error sending keys to tunshell thread {0}")]
    TunshellSend(#[from] async_channel::SendError<String>),
    #[error("Invalid action")]
    InvalidActionKind(String),
}

/// On the Bytebeam platform, an Action is how beamd and through it,
/// the end-user, can communicate the tasks they want to perform on
/// said device, in this case, uplink.
#[derive(Debug, Serialize, Deserialize)]
pub struct Action {
    /// Action identifier
    pub id: String,
    /// Can be either control or process
    kind: String,
    /// Action name, can be used to identify action
    name: String,
    /// Action payload, usually json. Can be used as args, depending on the invoked command
    payload: String,
}

pub struct Actions {
    action_status: Stream<ActionResponse>,
    process: process::Process,
    controller: controller::Controller,
    actions_rx: Option<Receiver<Action>>,
    tunshell_tx: Sender<String>,
}

impl Actions {
    /// Create new Action processor/forwarder
    pub async fn new(
        _config: Arc<Config>,
        controllers: HashMap<String, Sender<Control>>,
        actions_rx: Receiver<Action>,
        tunshell_tx: Sender<String>,
        action_status: Stream<ActionResponse>,
    ) -> Actions {
        let controller = Controller::new(controllers, action_status.clone());
        let process = process::Process::new(action_status.clone());

        Actions { action_status, process, controller, actions_rx: Some(actions_rx), tunshell_tx }
    }

    /// Loop through, receiving
    pub async fn start(&mut self) {
        let action_stream = self.actions_rx.take().unwrap();

        // start receiving and processing actions
        loop {
            let action = match action_stream.recv().await {
                Ok(v) => v,
                Err(e) => {
                    error!("Action stream receiver error = {:?}", e);
                    break;
                }
            };

            debug!("Action = {:?}", action);

            let action_id = action.id.clone();
            let action_name = action.name.clone();
            let error = match self.handle(action).await {
                Ok(_) => continue,
                Err(e) => e,
            };

            self.forward_action_error(&action_id, &action_name, error).await;
        }
    }

    async fn handle(&mut self, action: Action) -> Result<(), Error> {
        match action.kind.as_ref() {
            "control" => {
                let command = action.name.clone();
                let id = action.id;
                self.controller.execute(&id, command).await?;
            }
            "process" => {
                let command = action.name.clone();
                let payload = action.payload.clone();
                let id = action.id;

                self.process.execute(id.clone(), command.clone(), payload).await?;
            }
            "tunshell" => {
                self.tunshell_tx.send(action.payload).await?;
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
