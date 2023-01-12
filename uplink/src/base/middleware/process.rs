use flume::SendError;
use log::{debug, error, info};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::{pin, select, task, time};

use crate::{ActionResponse, Package, Stream};

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
    action_status: Stream<ActionResponse>,
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
    #[error("Busy with previous action")]
    Busy,
    #[error("No stdout in spawned action")]
    NoStdout,
}

impl Process {
    pub fn new(action_status: Stream<ActionResponse>) -> Process {
        Process { last_process_done: Arc::new(Mutex::new(true)), action_status }
    }

    /// Run a process of specified command
    #[tracing::instrument(name = "Process", skip_all)]
    pub async fn run(
        &mut self,
        id: String,
        command: String,
        payload: String,
    ) -> Result<Child, Error> {
        *self.last_process_done.lock().unwrap() = false;

        let mut cmd = Command::new(command);
        cmd.arg(id).arg(payload).kill_on_drop(true).stdout(Stdio::piped());

        match cmd.spawn() {
            Ok(child) => Ok(child),
            Err(e) => {
                *self.last_process_done.lock().unwrap() = true;
                Err(e.into())
            }
        }
    }

    /// Capture stdout of the running process in a spawned task
    pub async fn spawn_and_capture_stdout(&mut self, mut child: Child) -> Result<(), Error> {
        let stdout = child.stdout.take().ok_or(Error::NoStdout)?;
        let mut stdout = BufReader::new(stdout).lines();

        let mut status_bucket = self.action_status.clone();
        let last_process_done = self.last_process_done.clone();

        task::spawn(async move {
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
                        if let Err(e) = status_bucket.fill(status).await {
                            error!("Failed to send child process status. Error = {:?}", e);
                        }
                     }
                     status = child.wait() => info!("Action done!! Status = {:?}", status),
                     _ = &mut timeout => break
                }
            }

            *last_process_done.lock().unwrap() = true;
        });

        Ok(())
    }

    pub async fn execute<S: Into<String>>(
        &mut self,
        id: S,
        command: S,
        payload: S,
    ) -> Result<(), Error> {
        let command = String::from("tools/") + &command.into();

        // Check if last process is in progress
        if !(*self.last_process_done.lock().unwrap()) {
            return Err(Error::Busy);
        }

        // Spawn the action and capture its stdout
        let child = self.run(id.into(), command, payload.into()).await?;
        self.spawn_and_capture_stdout(child).await?;

        Ok(())
    }
}
