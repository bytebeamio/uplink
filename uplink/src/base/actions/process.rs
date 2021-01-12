use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::time;

use super::{ActionResponse, Package};

use crate::base::Bucket;
use std::io;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Process abstracts functions to spawn process and handle their output
/// It makes sure that a new process isn't executed when the previous process
/// is in progress.
/// It sends result and errors to the broker over collector_tx
pub struct Process {
    // buffer to send status messages to cloud
    status_bucket: Bucket<ActionResponse>,
    // we use this flag to ignore new process spawn while previous process is in progress
    last_process_done: Arc<Mutex<bool>>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error {0}")]
    Io(#[from] io::Error),
    #[error("Json error {0}")]
    Json(#[from] serde_json::Error),
    #[error("Send error {0}")]
    Send(#[from] SendError<Box<dyn Package>>),
    #[error("Busy error")]
    Busy,
}

impl Process {
    pub fn new(collector_tx: Sender<Box<dyn Package>>) -> Process {
        let status_bucket = Bucket::new(collector_tx, "action_status", 1);
        Process { status_bucket, last_process_done: Arc::new(Mutex::new(true)) }
    }

    pub async fn command(&mut self, id: String, command: String, payload: String) -> Result<Child, Error> {
        *self.last_process_done.lock().unwrap() = false;

        let mut cmd = Command::new(command);
        cmd.arg(id).arg(payload).kill_on_drop(true).stdout(Stdio::piped());

        match cmd.spawn() {
            Ok(child) => Ok(child),
            Err(e) => {
                *self.last_process_done.lock().unwrap() = true;
                return Err(e.into());
            }
        }
    }

    pub async fn capture_stdout(&mut self, stdout: ChildStdout) {
        let mut status_bucket = self.status_bucket.clone();
        let last_process_done = self.last_process_done.clone();
        let mut stdout = BufReader::new(stdout).lines();

        tokio::spawn(async move {
            // Capture and send progress of spawned process
            while let Some(line) = stdout.next_line().await.unwrap() {
                let status: ActionResponse = match serde_json::from_str(&line) {
                    Ok(status) => status,
                    Err(e) => {
                        let mut status = ActionResponse::new("dummy", "Failed");
                        status.add_error(e.to_string());
                        break;
                    }
                };

                debug!("Action status: {:?}", status);
                if let Err(e) = status_bucket.fill(status).await {
                    error!("Failed to send child process status. Error = {:?}", e);
                }
            }

            *last_process_done.lock().unwrap() = true;
        });
    }

    pub async fn execute<S: Into<String>>(&mut self, id: S, command: S, payload: S) -> Result<(), Error> {
        let command = String::from("tools/") + &command.into();

        // check if last process is in progress
        if *self.last_process_done.lock().unwrap() == false {
            return Err(Error::Busy);
        }

        let mut child = self.command(id.into(), command.into(), payload.into()).await?;
        let stdout = child.stdout.take().expect("child did not have a handle to stdout");
        self.capture_stdout(stdout).await;

        // Spawn a process and capture its stdout without blocking the `execute` method
        // We spawn instead of blocking to allow execute to return immediately and reply
        // next action with busy status to cloud instead of blocking
        tokio::spawn(async move {
            // Wait for spawned process result without blocking. This to reply cloud with progress
            // of spawned process
            tokio::spawn(async move {
                let status = time::timeout(Duration::from_secs(120), child.wait()).await;
                debug!("child status was: {:?}", status);
            });
        });

        Ok(())
    }
}
