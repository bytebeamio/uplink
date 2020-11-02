use tokio::io::{ AsyncBufReadExt, BufReader };
use tokio::process::{ Command, ChildStdout};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::SendError;
use tokio::time;
use derive_more::From;

use super::{ActionResponse, Package};

use std::sync::{Arc, Mutex};
use std::io;
use std::process::Stdio;
use std::time::Duration;

/// Process abstracts functions to spawn process and handle their output
/// It makes sure that a new process isn't executed when the previous process
/// is in progress.
/// It sends result and errors to the broker over collector_tx
pub struct Process {
    // we use this flag to ignore new process spawn while previous process is in progress
    last_process_done: Arc<Mutex<bool>>,
    // used to send errors and process status to cloud
    collector_tx:      Sender<Box<dyn Package>>,
}

#[derive(Debug, From)]
pub enum Error {
    Io(io::Error),
    Json(serde_json::Error),
    Send(SendError<Box<dyn Package>>),
    Busy
}

impl Process {
    pub fn new(collector_tx: Sender<Box<dyn Package>>) -> Process {
        Process { last_process_done: Arc::new(Mutex::new(true)), collector_tx }
    }

    pub async fn execute<S: Into<String>>(&mut self, id: S, command: S, payload: S) -> Result<(), Error> {
        let command = String::from("tools/") + &command.into();

        // check if last process is in progress
        if *self.last_process_done.lock().unwrap() == false {
            return Err(Error::Busy);
        }
            
        *self.last_process_done.lock().unwrap() = false;
        let mut cmd = Command::new(command);
        cmd.arg(id.into()).arg(payload.into()).kill_on_drop(true).stdout(Stdio::piped());

        let collector_tx = self.collector_tx.clone();
        let last_process_done = self.last_process_done.clone();
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                *self.last_process_done.lock().unwrap() = true;
                return Err(e.into())
            }
        };

        // spawn a process and capture its stdout without blocking the `execute` method
        tokio::spawn(async move {
            let stdout = child.stdout.take().expect("child did not have a handle to stdout");

            // wait for spawned process result without blocking
            tokio::spawn(async {
                let status = time::timeout(Duration::from_secs(120), async {
                    let status = child.await?;
                    Ok::<_, io::Error>(status)
                }).await;

                debug!("child status was: {:?}", status);
            });

            if let Err(e) = capture_stdout(stdout, collector_tx).await {
                error!("Failed to capture stdout. Error = {:?}", e);
            }

            *last_process_done.lock().unwrap() = true;
        });

        Ok(())
    }

}

async fn capture_stdout(stdout: ChildStdout, mut collector_tx: Sender<Box<dyn Package>>) -> Result<(), Error>  {
    // stream the stdout of spawned process to capture its progress
    let mut stdout = BufReader::new(stdout).lines();
    while let Some(line) = stdout.next_line().await.unwrap() {
        let status: ActionResponse = serde_json::from_str(&line)?;
        warn!("Acion status: {:?}", status);
        collector_tx.send(Box::new(status)).await?;
    }

    Ok(())
}
