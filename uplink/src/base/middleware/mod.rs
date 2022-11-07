use std::collections::HashMap;
use std::sync::Arc;

use flume::{Receiver, Sender, TrySendError};
use log::{debug, error};

mod actions;
pub mod logcat;
pub mod ota;
mod process;
pub mod tunshell;

use crate::base::Stream;
use crate::{Config, Package, Payload};
pub use actions::{Action, ActionResponse};
use logcat::{LogLevel, LogcatConfig, LogcatInstance};
use process::Process;

#[derive(thiserror::Error, Debug)]
enum Error {
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
    #[error("Another Action is being processed by collector")]
    CollectorOccupied,
}

pub struct Middleware {
    config: Arc<Config>,
    action_status: Stream<ActionResponse>,
    process: Process,
    actions_rx: Receiver<Action>,
    action_fwd: HashMap<String, Sender<Action>>,
    bridge_data_tx: Sender<Box<dyn Package>>,
    logcat: Option<LogcatInstance>,
}

impl Middleware {
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        action_status: Stream<ActionResponse>,
        action_fwd: HashMap<String, Sender<Action>>,
        bridge_data_tx: Sender<Box<dyn Package>>,
    ) -> Self {
        let process = Process::new(action_status.clone());
        Self {
            config,
            action_status,
            process,
            actions_rx,
            action_fwd,
            bridge_data_tx,
            logcat: None,
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
        if self.config.run_logcat {
            debug!("starting logcat");
            self.logcat = Some(LogcatInstance::new(
                self.create_log_stream(),
                &LogcatConfig { tags: vec!["*".to_string()], min_level: LogLevel::Debug },
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
                self.action_fwd.get("launch_shell").unwrap().send_async(action).await?;
                return Ok(());
            }
            "configure_logcat" => {
                match serde_json::from_str::<LogcatConfig>(action.payload.as_str()) {
                    Ok(mut logcat_config) => {
                        logcat_config.tags =
                            logcat_config.tags.into_iter().filter(|tag| !tag.is_empty()).collect();
                        log::info!("restarting logcat with following config: {:?}", logcat_config);
                        self.logcat =
                            Some(LogcatInstance::new(self.create_log_stream(), &logcat_config))
                    }
                    Err(e) => {
                        error!("couldn't parse logcat config payload:\n{}\n{}", action.payload, e)
                    }
                }
            }
            "update_firmware" if self.config.ota.enabled => {
                // if action can't be sent, Error out and notify cloud
                self.action_fwd.get("update_firmware").unwrap().try_send(action).map_err(|e| {
                    match e {
                        TrySendError::Full(_) => Error::CollectorOccupied,
                        e => Error::TrySend(e),
                    }
                })?;
                return Ok(());
            }

            // Bridge actions are forwarded
            name => {
                if let Some(tx) = self.action_fwd.get(name) {
                    tx.try_send(action)?;
                    return Ok(());
                }
            }
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
