use super::{Control, Package};
use async_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod controller;
mod process;

use crate::base::{Buffer, Stream};
pub use controller::Controller;
use tokio::time::Duration;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Process error {0}")]
    Process(#[from] process::Error),
    #[error("Controller error {0}")]
    Controller(#[from] controller::Error),
    #[error("Invalid action")]
    InvalidActionKind(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Action {
    // action id
    pub id: String,
    // control or process
    kind: String,
    // action name
    name: String,
    // action payload. json. can be args/payload. depends on the invoked command
    payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    id: String,
    // sequence number
    sequence: u64,
    // timestamp
    timestamp: u128,
    // running, failed
    state: String,
    // progress percentage for processes
    progress: u8,
    // list of error
    errors: Vec<String>,
}

impl ActionResponse {
    pub fn new(id: &str) -> Self {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));

        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp: timestamp.as_millis(),
            state: "Running".to_owned(),
            progress: 0,
            errors: vec![],
        }
    }

    pub fn success(id: &str) -> ActionResponse {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp: timestamp.as_millis(),
            state: "Completed".to_owned(),
            progress: 100,
            errors: vec![],
        }
    }

    pub fn failure<E: Into<String>>(id: &str, error: E) -> ActionResponse {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp: timestamp.as_millis(),
            state: "Failed".to_owned(),
            progress: 100,
            errors: vec![error.into()],
        }
    }
}

pub struct Actions {
    status_bucket: Stream<ActionResponse>,
    process: process::Process,
    controller: controller::Controller,
    actions_rx: Option<Receiver<Action>>,
}

pub async fn new(
    collector_tx: Sender<Box<dyn Package>>,
    controllers: HashMap<String, Sender<Control>>,
    actions_rx: Receiver<Action>,
) -> Actions {
    let controller = Controller::new(controllers, collector_tx.clone());
    let process = process::Process::new(collector_tx.clone());
    let status_bucket = Stream::new("action_status", 1, collector_tx);
    Actions { status_bucket, process, controller, actions_rx: Some(actions_rx) }
}

impl Actions {
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
            v => return Err(Error::InvalidActionKind(v.to_owned())),
        }

        Ok(())
    }

    async fn forward_action_error(&mut self, id: &str, action: &str, error: Error) {
        error!("Failed to execute. Command = {:?}, Error = {:?}", action, error);
        let status = ActionResponse::failure(id, error.to_string());

        if let Err(e) = self.status_bucket.fill(status).await {
            error!("Failed to send status. Error = {:?}", e);
        }
    }
}

impl Package for Buffer<ActionResponse> {
    fn stream(&self) -> String {
        return "action_status".to_owned();
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }
}
