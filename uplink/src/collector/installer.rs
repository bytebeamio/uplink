use std::{fs::File, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use flate2::read::GzDecoder;
use log::{debug, error, info};
use tar::Archive;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::{pin, select, time};

use super::downloader::DownloadFile;
use crate::base::{bridge::BridgeTx, InstallerConfig};
use crate::{Action, ActionResponse, Config};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("File io Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Download path missing from received action")]
    MissingPath,
    #[error("No stdout in spawned action")]
    NoStdout,
}

pub struct OTAInstaller {
    config: InstallerConfig,
    bridge_tx: BridgeTx,
}

impl OTAInstaller {
    pub fn new(config: Arc<Config>, bridge_tx: BridgeTx) -> Self {
        let config = config.ota_installer.clone();
        Self { config, bridge_tx }
    }

    #[tokio::main]
    pub async fn start(&self) {
        let actions_rx = match self.bridge_tx.register_action_routes(&self.config.actions).await {
            Some(r) => r,
            _ => return,
        };

        while let Ok(action) = actions_rx.recv_async().await {
            if let Err(e) = self.extractor(&action) {
                error!("Error extracting zip: {e}");
                self.forward_action_error(action, e).await;
                continue;
            }

            if let Err(e) = self.installer(&action).await {
                error!("Error installing: {e}");
                self.forward_action_error(action, e).await;
            }
        }
    }

    async fn forward_action_error(&self, action: Action, error: Error) {
        let status = ActionResponse::failure(&action.action_id, error.to_string());
        self.bridge_tx.send_action_response(status).await
    }

    fn extractor(&self, action: &Action) -> Result<(), Error> {
        let info: DownloadFile = serde_json::from_str(&action.payload)?;
        let path = info.download_path.ok_or(Error::MissingPath)?;
        let tar_gz = File::open(path)?;
        let tar = GzDecoder::new(tar_gz);
        let mut archive = Archive::new(tar);
        archive.unpack(&self.config.path)?;

        Ok(())
    }

    // Run `update.sh` from extracted tarball
    async fn installer(&self, action: &Action) -> Result<(), Error> {
        let script_path = PathBuf::from(self.config.path.clone()).join("update.sh");
        let mut cmd = Command::new(script_path);
        cmd.arg(&action.action_id).kill_on_drop(true).stdout(Stdio::piped());
        let child = cmd.spawn()?;

        self.spawn_and_capture_stdout(child, action).await?;

        Ok(())
    }

    /// Capture stdout from running update script and push to cloud
    pub async fn spawn_and_capture_stdout(
        &self,
        mut child: Child,
        action: &Action,
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
                        Err(e) => ActionResponse::failure(&action.action_id, e.to_string()),
                    };

                    debug!("Action status: {:?}", status);
                    self.bridge_tx.send_action_response(status).await;
                 }
                 status = child.wait() => info!("Action done!! Status = {:?}", status),
                 _ = &mut timeout => return Ok(())
            }
        }
    }
}
