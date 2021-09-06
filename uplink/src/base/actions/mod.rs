use super::{Config, Control, Package};
use async_channel::{Receiver, Sender};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod controller;
mod process;
pub mod tunshell;

use crate::base::{Buffer, Point, Stream};
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
    #[error("Error sending keys to tunshell thread {0}")]
    TunshellSendError(#[from] async_channel::SendError<String>),
    #[error("Error sending Action through bridge {0}")]
    BridgeSendError(#[from] async_channel::TrySendError<Action>),
    #[error("Invalid action")]
    InvalidActionKind(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

impl Action {
    /// Download contents of the OTA update if action is named "update_firmware"
    pub async fn firmware_downloader(
        self,
        ota_path: String,
        bridge_tx: Sender<Action>,
    ) -> Result<(), Error> {
        println!("Dowloading firmware");
        if let Some(url) =
            serde_json::from_str::<HashMap<String, String>>(&self.payload)?.get("url")
        {
            println!("Dowloading {}", url);
            let url = url.clone();
            let action = self.clone();
            tokio::task::spawn(async move {
                match reqwest::get(url).await {
                    Ok(resp) => match resp.bytes().await {
                        Ok(content) => match File::create(ota_path).await {
                            Ok(mut file) => {
                                file.write_all(&content).await.unwrap_or_else(|e| {
                                    error!("Failed to download | Error: {}", e)
                                });

                                bridge_tx.try_send(action).unwrap_or_else(|e| {
                                    error!("Failed forwarding to bridge | Error: {}", e)
                                });
                            }
                            Err(e) => error!("Error: {}", e),
                        },
                        Err(e) => error!("Couldn't unpack downloaded OTA update: {}", e),
                    },
                    Err(e) => error!("Couldn't download OTA update: {}", e),
                }
            })
            .await;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    id: String,
    // sequence number
    sequence: u32,
    // timestamp
    timestamp: u64,
    // running, failed
    state: String,
    // progress percentage for processes
    progress: u8,
    // list of error
    errors: Vec<String>,
}

impl ActionResponse {
    pub fn new(id: &str) -> Self {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));

        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp: timestamp.as_millis() as u64,
            state: "Running".to_owned(),
            progress: 0,
            errors: vec![],
        }
    }

    pub fn success(id: &str) -> ActionResponse {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp: timestamp.as_millis() as u64,
            state: "Completed".to_owned(),
            progress: 100,
            errors: vec![],
        }
    }

    pub fn failure<E: Into<String>>(id: &str, error: E) -> ActionResponse {
        let timestamp =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp: timestamp.as_millis() as u64,
            state: "Failed".to_owned(),
            progress: 100,
            errors: vec![error.into()],
        }
    }
}

impl Point for ActionResponse {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

pub struct Actions {
    config: Arc<Config>,
    action_status: Stream<ActionResponse>,
    process: process::Process,
    controller: controller::Controller,
    actions_rx: Option<Receiver<Action>>,
    tunshell_tx: Sender<String>,
    bridge_tx: Sender<Action>,
}

impl Actions {
    pub async fn new(
        config: Arc<Config>,
        controllers: HashMap<String, Sender<Control>>,
        actions_rx: Receiver<Action>,
        tunshell_tx: Sender<String>,
        action_status: Stream<ActionResponse>,
        bridge_tx: Sender<Action>,
    ) -> Actions {
        let controller = Controller::new(controllers, action_status.clone());
        let process = process::Process::new(action_status.clone());
        Actions {
            config,
            action_status,
            process,
            controller,
            actions_rx: Some(actions_rx),
            tunshell_tx,
            bridge_tx,
        }
    }
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

    /// Handle received actions
    async fn handle(&mut self, action: Action) -> Result<(), Error> {
        match action.name.as_ref() {
            "tunshell" => {
                self.tunshell_tx.send(action.payload).await?;
                return Ok(());
            }
            "firmware_update" => {
                action
                    .firmware_downloader(self.config.ota_path.clone(), self.bridge_tx.clone())
                    .await?;
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

        if let Err(e) = self.action_status.fill(status).await {
            error!("Failed to send status. Error = {:?}", e);
        }
    }
}

impl Package for Buffer<ActionResponse> {
    fn topic(&self) -> Arc<String> {
        return self.topic.clone();
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.buffer).unwrap()
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}
