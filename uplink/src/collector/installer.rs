use std::{fs::File, path::PathBuf};

use log::{debug, error, warn};
use tar::Archive;
use tokio::process::Command;

use super::downloader::DownloadFile;
use crate::base::{bridge::BridgeTx, InstallerConfig};
use crate::{Action, ActionResponse};

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
    pub fn new(config: InstallerConfig, bridge_tx: BridgeTx) -> Self {
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
                error!("Error extracting tarball: {e}");
                self.forward_action_error(action, e).await;
                continue;
            }

            if let Err(e) = self.installer(&action).await {
                error!("Error installing ota update: {e}");
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

        debug!("Extracting tar from:{path}; to: {}", self.config.path);
        let dst = PathBuf::from(&self.config.path);
        if dst.exists() {
            warn!("Cleaning up {}", &self.config.path);
            std::fs::remove_dir_all(&dst)?;
        }
        let tar_gz = File::open(path)?;
        let mut archive = Archive::new(tar_gz);
        archive.unpack(dst)?;

        Ok(())
    }

    // Run `updater` from extracted tarball
    async fn installer(&self, action: &Action) -> Result<(), Error> {
        let updater_path = PathBuf::from(self.config.path.clone()).join("updater");
        debug!("Running updater: {}/updater", self.config.path);

        // Ensure updater has execution rights
        #[cfg(unix)]
        {
            let file = File::open(&updater_path)?;
            file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o777))?;
        }

        let mut cmd = Command::new(updater_path.as_path());
        cmd.arg(&action.action_id).arg(self.config.uplink_port.to_string());
        cmd.spawn()?;

        Ok(())
    }
}
