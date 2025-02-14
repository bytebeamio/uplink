use flume::{Receiver, RecvError, SendError};
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::select;

use super::downloader::DownloadFile;
use crate::base::actions::Cancellation;
use crate::base::bridge::BridgeTx;
use crate::{Action, ActionResponse};

use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use anyhow::Context;
use crate::base::bridge::stream::MessageBuffer;
use crate::base::clock;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error {0}")]
    Io(#[from] io::Error),
    #[error("Json error {0}")]
    Json(#[from] serde_json::Error),
    #[error("Recv error {0}")]
    Recv(#[from] RecvError),
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<MessageBuffer>>),
    #[error("Busy with previous action")]
    Busy,
    #[error("No stdout in spawned action")]
    NoStdout,
    #[error("Script has been cancelled: '{0}'")]
    Cancelled(String),
}

/// Script runner runs a script downloaded with FileDownloader and handles their output over the action_status stream.
/// Multiple scripts can't be run in parallel. It can also send progress, result and errors to the platform by using
/// the JSON formatted output over STDOUT.
pub struct ScriptRunner {
    // to receive actions
    actions_rx: Receiver<Action>,
    // to send responses back to bridge
    bridge_tx: BridgeTx,
    sequence: u32,
}

impl ScriptRunner {
    pub fn new(actions_rx: Receiver<Action>, bridge_tx: BridgeTx) -> Self {
        Self { actions_rx, bridge_tx, sequence: 0 }
    }

    /// Spawn a child process to run the script with sh
    pub async fn run(&mut self, command: PathBuf) -> Result<Child, Error> {
        let mut cmd = Command::new("sh");
        cmd.arg(command).kill_on_drop(true).stdout(Stdio::piped());

        let child = cmd.spawn()?;

        Ok(child)
    }

    /// Capture stdout of the running process in a spawned task and forward any action_status in JSON format
    pub async fn spawn_and_capture_stdout(
        &mut self,
        mut child: Child,
        id: &str,
    ) -> Result<(), Error> {
        let stdout = child.stdout.take().ok_or(Error::NoStdout)?;
        let mut stdout = BufReader::new(stdout).lines();
        let mut sequence = 0;
        loop {
            select! {
                Ok(Some(line)) = stdout.next_line() => {
                    self.forward_status(
                        ActionResponse {
                            action_id: id.to_owned(),
                            sequence,
                            timestamp: clock() as _,
                            state: line,
                            progress: 50,
                            errors: vec![],
                            done_response: None,
                        }
                    ).await;
                }
                // Send a success status at the end of execution
                status = child.wait() => {
                    info!("Script finished!! Status = {:?}", status);
                    match status {
                        Ok(status) => {
                            if status.success() {
                                self.forward_status(ActionResponse::success(id)).await;
                            } else {
                                self.forward_status(ActionResponse::failure(id, format!("Script failed with status: {status}"))).await;
                            }
                        }
                        Err(e) => {
                            self.forward_status(ActionResponse::failure(id, format!("Error: {e}"))).await;
                        }
                    }
                    break;
                },
                // Cancel script run on receiving cancel action, e.g. on action timeout
                Ok(action) = self.actions_rx.recv_async() => {
                    if action.action_id == id {
                        log::error!("Backend tried sending the same action again!");
                    } else if action.name != "cancel_action" {
                        self.bridge_tx.send_action_response(ActionResponse::failure(action.action_id.as_str(), "Script runner is already occupied")).await;
                    } else {
                        match serde_json::from_str::<Cancellation>(&action.payload)
                            .context("Invalid cancel action payload")
                            .and_then(|cancellation| {
                                if cancellation.action_id == id {
                                    Ok(())
                                } else {
                                    Err(anyhow::Error::msg(format!("Cancel action target ({}) doesn't match active script action id ({})", cancellation.action_id, id)))
                                }
                            }) {
                            Ok(_) => {
                                // TODO: send stop signal, wait for a few seconds, then send kill signal, updating cancel action status at each step
                                let _ = child.kill().await;
                                self.bridge_tx.send_action_response(ActionResponse::success(action.action_id.as_str())).await;
                                self.bridge_tx.send_action_response(ActionResponse::failure(id, "Script killed")).await;
                            },
                            Err(e) => {
                                self.bridge_tx.send_action_response(ActionResponse::failure(action.action_id.as_str(), format!("Could not stop script: {e:?}"))).await;
                            },
                        }
                    }
                },
            }
        }


        Ok(())
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        info!("Script runner is ready");

        loop {
            let action = self.actions_rx.recv_async().await?;
            let command = match serde_json::from_str::<DownloadFile>(&action.payload) {
                Ok(DownloadFile { download_path: Some(download_path), .. }) => download_path,
                Ok(_) => {
                    let err = format!(
                        "Action payload doesn't contain path for script execution; payload: {:?}",
                        action.payload
                    );
                    warn!("{err}");
                    self.forward_status(ActionResponse::failure(&action.action_id, err)).await;
                    continue;
                }
                Err(e) => {
                    let err = format!(
                        "Failed to deserialize action payload: {e}; payload: {:?}",
                        action.payload
                    );
                    error!("{err}");
                    self.forward_status(ActionResponse::failure(&action.action_id, err)).await;
                    continue;
                }
            };
            // Spawn the action and capture its stdout
            let child = self.run(command).await?;
            self.spawn_and_capture_stdout(child, &action.action_id).await?
        }
    }

    // Forward action status to bridge
    async fn forward_status(&mut self, status: ActionResponse) {
        self.sequence += 1;
        let status = status.set_sequence(self.sequence);
        self.bridge_tx.send_action_response(status).await;
    }
}
