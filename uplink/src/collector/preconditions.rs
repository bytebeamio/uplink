use std::{fs::metadata, os::unix::fs::MetadataExt, sync::Arc};

use flume::Receiver;
use human_bytes::human_bytes;
use log::debug;
use serde::Deserialize;

use crate::{base::bridge::BridgeTx, Action, ActionResponse, Config};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File io Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Disk space is insufficient: {0}")]
    InsufficientDisk(String),
}

#[derive(Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct Preconditions {
    #[serde(alias = "content-length")]
    content_length: usize,
    #[serde(alias = "uncompressed-size")]
    uncompressed_length: Option<usize>,
}

pub struct PreconditionChecker {
    config: Arc<Config>,
    actions_rx: Receiver<Action>,
    bridge_tx: BridgeTx,
}

impl PreconditionChecker {
    pub fn new(config: Arc<Config>, actions_rx: Receiver<Action>, bridge_tx: BridgeTx) -> Self {
        Self { config, actions_rx, bridge_tx }
    }

    #[tokio::main]
    pub async fn start(self) {
        while let Ok(action) = self.actions_rx.recv() {
            let preconditions: Preconditions = match serde_json::from_str(&action.payload) {
                Ok(c) => c,
                Err(e) => {
                    let response = ActionResponse::failure(&action.action_id, e.to_string());
                    self.bridge_tx.send_action_response(response).await;
                    continue;
                }
            };

            if let Err(e) = self.check_disk_size(preconditions) {
                let response = ActionResponse::failure(&action.action_id, e.to_string());
                self.bridge_tx.send_action_response(response).await;
                continue;
            }

            let response = ActionResponse::done(&action.action_id, "Checked OK", Some(action.clone()));
            self.bridge_tx.send_action_response(response).await;
        }
    }

    // Fails if there isn't enough space to download and/or install update
    // NOTE: both download and installation could happen in the same partition
    // TODO: this should be significantly simplified once we move to using `expected-free-space`
    // comparison instead of making assumptions about what the user might want.
    fn check_disk_size(&self, preconditions: Preconditions) -> Result<(), Error> {
        let Some(mut required_free_space) = preconditions.uncompressed_length else {
            return Ok(());
        };
        let disk_free_space =
            fs2::free_space(&self.config.precondition_checks.as_ref().unwrap().path)? as usize;

        // Check if download and installation paths are on the same partition, if yes add download file size to required
        if metadata(&self.config.downloader.path)?.dev()
            == metadata(&self.config.precondition_checks.as_ref().unwrap().path)?.dev()
        {
            required_free_space += preconditions.content_length;
        }

        let req_size = human_bytes(required_free_space as f64);
        let free_size = human_bytes(disk_free_space as f64);
        debug!("The installation requires {req_size}; Disk free space is {free_size}");

        if required_free_space > disk_free_space {
            return Err(Error::InsufficientDisk(free_size));
        }

        Ok(())
    }
}
