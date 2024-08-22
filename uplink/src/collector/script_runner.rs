use flume::{Receiver, RecvError, SendError};
use log::{debug, error, info, trace, warn};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::select;

use super::downloader::DownloadFile;
use crate::base::actions::Cancellation;
use crate::base::bridge::BridgeTx;
use crate::{Action, ActionResponse, Package};

use std::io;
use std::path::PathBuf;
use std::process::Stdio;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error {0}")]
    Io(#[from] io::Error),
    #[error("Json error {0}")]
    Json(#[from] serde_json::Error),
    #[error("Recv error {0}")]
    Recv(#[from] RecvError),
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
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

        loop {
            select! {
                Ok(Some(line)) = stdout.next_line() => {
                    let mut status: ActionResponse = match serde_json::from_str(&line) {
                        Ok(status) => status,
                        Err(e) => {
                            error!("Failed to deserialize script output: {line:?}; Error: {e}");
                            continue;
                        },
                    };
                    id.clone_into(&mut status.action_id);

                    debug!("Action status: {:?}", status);
                    self.forward_status(status).await;
                }
                // Send a success status at the end of execution
                status = child.wait() => {
                    info!("Action done!! Status = {:?}", status);
                    self.forward_status(ActionResponse::success(id)).await;
                    break;
                },
                // Cancel script run on receiving cancel action, e.g. on action timeout
                Ok(action) = self.actions_rx.recv_async() => {
                    let cancellation: Cancellation = serde_json::from_str(&action.payload)?;

                    trace!("Cancelling script: '{}'", cancellation.action_id);
                    let status = ActionResponse::failure(id, Error::Cancelled(action.action_id).to_string());
                    self.bridge_tx.send_action_response(status).await;
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
