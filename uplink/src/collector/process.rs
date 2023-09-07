use flume::{Receiver, RecvError, SendError};
use log::{debug, error, info};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::{pin, select, time};

use crate::base::bridge::BridgeTx;
use crate::{Action, ActionResponse, Package};

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

/// Process abstracts functions to spawn process and handle their output
/// It makes sure that a new process isn't executed when the previous process
/// is in progress.
/// It sends result and errors to the broker over collector_tx
pub struct ProcessHandler {
    // to receive actions
    actions_rx: Receiver<Action>,
    // to send responses back to bridge
    bridge_tx: BridgeTx,
}

impl ProcessHandler {
    pub fn new(actions_rx: Receiver<Action>, bridge_tx: BridgeTx) -> Self {
        Self { actions_rx, bridge_tx }
    }

    /// Run a process of specified command
    pub async fn run(
        &mut self,
        id: String,
        command: String,
        payload: String,
    ) -> Result<Child, Error> {
        let mut cmd = Command::new(command);
        cmd.arg(id).arg(payload).kill_on_drop(true).stdout(Stdio::piped());

        let child = cmd.spawn()?;

        Ok(child)
    }

    /// Capture stdout of the running process in a spawned task
    pub async fn spawn_and_capture_stdout(&mut self, mut child: Child) -> Result<(), Error> {
        let stdout = child.stdout.take().ok_or(Error::NoStdout)?;
        let mut stdout = BufReader::new(stdout).lines();

        let timeout = time::sleep(Duration::from_secs(10));
        pin!(timeout);

        loop {
            select! {
                 Ok(Some(line)) = stdout.next_line() => {
                    let status: ActionResponse = match serde_json::from_str(&line) {
                        Ok(status) => status,
                        Err(e) => ActionResponse::failure("dummy", e.to_string()),
                    };

                    debug!("Action status: {:?}", status);
                    self.bridge_tx.send_action_response(status).await;
                 }
                 status = child.wait() => info!("Action done!! Status = {:?}", status),
                 _ = &mut timeout => break
            }
        }

        Ok(())
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        loop {
            let action = self.actions_rx.recv_async().await?;
            let command = String::from("tools/") + &action.name;

            // Spawn the action and capture its stdout
            let child = self.run(action.action_id, command, action.payload).await?;
            self.spawn_and_capture_stdout(child).await?;
        }
    }
}
