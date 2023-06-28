use flume::{RecvError, SendError};
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::{pin, select, time};

use super::downloader::DownloadFile;
use crate::base::{bridge::BridgeTx, ActionRoute};
use crate::{ActionResponse, Package};

use std::io;
use std::process::Stdio;
use std::time::Duration;

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
}

/// Script runner runs a script downloaded with FileDownloader and handles their output over the action_status stream.
/// Multiple scripts can't be run in parallel. It can also send progress, result and errors to the platform by using
/// the JSON formatted output over STDOUT.
pub struct ScriptRunner {
    // to receive actions and send responses back to bridge
    bridge_tx: BridgeTx,
}

impl ScriptRunner {
    pub fn new(bridge_tx: BridgeTx) -> Self {
        Self { bridge_tx }
    }

    /// Spawn a child process to run the script with sh
    pub async fn run(&mut self, command: String) -> Result<Child, Error> {
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

        let timeout = time::sleep(Duration::from_secs(10));
        pin!(timeout);

        loop {
            select! {
                Ok(Some(line)) = stdout.next_line() => {
                    let status: ActionResponse = match serde_json::from_str(&line) {
                        Ok(status) => status,
                        Err(e) => {
                            error!("Failed to deserialize script output: \"{line}\"; Error: {e}");
                            continue;
                        },
                    };

                    debug!("Action status: {:?}", status);
                    self.bridge_tx.send_action_response(status).await;
                }
                // Send a success status at the end of execution
                status = child.wait() => {
                    info!("Action done!! Status = {:?}", status);
                    self.bridge_tx.send_action_response(ActionResponse::success(id)).await;
                    break;
                },
                _ = &mut timeout => break
            }
        }

        Ok(())
    }

    pub async fn start(mut self, routes: Vec<ActionRoute>) -> Result<(), Error> {
        let action_rx = match self.bridge_tx.register_action_routes(routes).await {
            Some(r) => r,
            _ => return Ok(()),
        };

        info!("Script runner is ready");

        loop {
            let action = action_rx.recv_async().await?;
            let command = match serde_json::from_str::<DownloadFile>(&action.payload) {
                Ok(DownloadFile { download_path: Some(download_path), .. }) => download_path,
                Ok(_) => {
                    warn!("Action payload could not be used for script execution");
                    continue;
                }
                Err(e) => {
                    error!("Failed to deserialize action payload: {e}");
                    continue;
                }
            };

            // Spawn the action and capture its stdout
            let child = self.run(command).await?;
            self.spawn_and_capture_stdout(child, &action.action_id).await?;
        }
    }
}
