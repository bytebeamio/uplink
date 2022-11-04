use super::{Config, Package};
use flume::{Receiver, Sender, TrySendError};
use log::{debug, error};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Duration;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

mod logging;
pub mod ota;
mod process;
pub mod tunshell;

use crate::base::{Buffer, Point, Stream};
use crate::Payload;

#[cfg(target_os = "linux")]
use logging::new_journalctl;
#[cfg(target_os = "android")]
use logging::new_logcat;
use logging::{LoggerInstance, LoggingConfig};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Process error {0}")]
    Process(#[from] process::Error),
    #[error("Error sending keys to tunshell thread {0}")]
    TunshellSend(#[from] flume::SendError<Action>),
    #[error("Error forwarding Action {0}")]
    TrySend(#[from] flume::TrySendError<Action>),
    #[error("Invalid action")]
    InvalidActionKind(String),
    #[error("Another OTA downloading")]
    Downloading,
}

/// On the Bytebeam platform, an Action is how beamd and through it,
/// the end-user, can communicate the tasks they want to perform on
/// said device, in this case, uplink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    #[serde(skip)]
    pub device_id: String,
    // action id
    #[serde(alias = "id")]
    pub action_id: String,
    // determines if action is a process
    pub kind: String,
    // action name
    pub name: String,
    // action payload. json. can be args/payload. depends on the invoked command
    pub payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    pub id: String,
    // sequence number
    pub sequence: u32,
    // timestamp
    pub timestamp: u64,
    // running, failed
    pub state: String,
    // progress percentage for processes
    pub progress: u8,
    // list of error
    pub errors: Vec<String>,
}

impl ActionResponse {
    fn new(id: &str, state: &str, progress: u8, errors: Vec<String>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;

        ActionResponse {
            id: id.to_owned(),
            sequence: 0,
            timestamp,
            state: state.to_owned(),
            progress,
            errors,
        }
    }

    pub fn progress(id: &str, state: &str, progress: u8) -> Self {
        ActionResponse::new(id, state, progress, vec![])
    }

    pub fn success(id: &str) -> ActionResponse {
        ActionResponse::new(id, "Completed", 100, vec![])
    }

    pub fn add_error<E: Into<String>>(mut self, error: E) -> ActionResponse {
        self.errors.push(error.into());
        self
    }

    pub fn failure<E: Into<String>>(id: &str, error: E) -> ActionResponse {
        ActionResponse::new(id, "Failed", 100, vec![]).add_error(error)
    }

    pub fn set_sequence(mut self, seq: u32) -> ActionResponse {
        self.sequence = seq;
        self
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
    actions_rx: Receiver<Action>,
    tunshell_tx: Sender<Action>,
    ota_tx: Sender<Action>,
    bridge_tx: Sender<Action>,
    bridge_data_tx: Sender<Box<dyn Package>>,
    logger: Option<LoggerInstance>,
}

impl Actions {
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        tunshell_tx: Sender<Action>,
        ota_tx: Sender<Action>,
        action_status: Stream<ActionResponse>,
        bridge_tx: Sender<Action>,
        bridge_data_tx: Sender<Box<dyn Package>>,
    ) -> Actions {
        let process = process::Process::new(action_status.clone());
        Actions {
            config,
            action_status,
            process,
            actions_rx,
            tunshell_tx,
            ota_tx,
            bridge_tx,
            bridge_data_tx,
            logger: None,
        }
    }

    fn create_log_stream(&self) -> Stream<Payload> {
        Stream::dynamic_with_size(
            "logs",
            &self.config.project_id,
            &self.config.device_id,
            32,
            self.bridge_data_tx.clone(),
        )
    }

    /// Start receiving and processing [Action]s
    pub async fn start(mut self) {
        #[cfg(target_os = "linux")]
        if let Some(super::JournalctlConfig { priority, tags }) = &self.config.journalctl {
            debug!("starting journalctl");
            self.logger = Some(new_journalctl(
                self.create_log_stream(),
                &LoggingConfig { tags: tags.clone(), min_level: *priority },
            ));
        }

        #[cfg(target_os = "android")]
        if self.config.run_logcat {
            debug!("starting logcat");
            self.logger = Some(new_logcat(
                self.create_log_stream(),
                &LoggingConfig { tags: vec!["*".to_string()], min_level: 1 },
            ));
        }

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
                let mut config = serde_json::from_str::<LoggingConfig>(action.payload.as_str())?;
                config.tags = config.tags.into_iter().filter(|tag| !tag.is_empty()).collect();
                log::info!("restarting journalctl with following config: {:?}", config);
                self.logger = Some(new_journalctl(self.create_log_stream(), &config))
            }
            #[cfg(target_os = "android")]
            "configure_logcat" => {
                let mut config = serde_json::from_str::<LoggingConfig>(action.payload.as_str())?;
                config.tags = config.tags.into_iter().filter(|tag| !tag.is_empty()).collect();
                log::info!("restarting logcat with following config: {:?}", config);
                self.logger = Some(new_logcat(self.create_log_stream(), &config))
            }
            "update_firmware" if self.config.ota.enabled => {
                // if action can't be sent, Error out and notify cloud
                self.ota_tx.try_send(action).map_err(|e| match e {
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

impl Package for Buffer<ActionResponse> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.buffer)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}
