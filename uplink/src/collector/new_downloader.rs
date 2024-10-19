//! Contains the handler and definitions necessary to download files such as OTA updates, as notified by a download file [`Action`]
//!
//! The thread running [`Bridge`] forwards the appropriate `Action`s to the [`FileDownloader`].
//!
//! Download file `Action`s contain JSON formatted [`payload`] which can be deserialized into an object of type [`DownloadFile`].
//! This object contains information such as the `url` where the download file is accessible from, the `file_name` or `version` number
//! associated with the downloaded file and a field which must be updated with the location in file-system where the file is stored into.
//!
//! The `FileDownloader` also updates the state of a downloading `Action` using periodic [`ActionResponse`]s containing
//! progress information. On completion of a download, the received `Action`'s `payload` is updated to contain information
//! about where the file was downloaded into, within the file-system. This action is then sent back to bridge as part of
//! the final "Completed" response through use of the [`done_response`].
//!
//! As illustrated in the following diagram, the [`Bridge`] forwards download actions to the [`FileDownloader`] where it is downloaded and
//! intermediate [`ActionResponse`]s are sent back to bridge as progress notifications. On completion of a download, the action response
//! also includes a modified action with the [`done_response`], where the action received by the downloader is suitably modified to include
//! information such as the path into which the file was downloaded. As seen in the diagram, two actions with [`action_id`] `"1"` and `"3"` are
//! forwarded from bridge. In the case of `action_id = 1` we can see that 3 action responses containing progress information are sent back
//! to bridge and on completion of download, action response containing the done_response is also sent to the bridge from where it might be
//! forwarded with help of [`action_redirections`]. The same is repeated in the case of `action_id = 3`.
//!
//! ```text
//!                                 ┌──────────────┐
//!                                 │FileDownloader│
//!                                 └──────┬───────┘
//!                                        │
//!                          .recv_async() │
//!     ┌──────┐    ┌────────────────┐  1  │
//!     │Bridge├────►Receiver<Action>├────►├───────┐
//!     └───▲──┘    └───────┬────────┘     │       │
//!         │               │              ├───────┤
//!         │               │              │       | progress = x%
//!         │               │              ├───────┤
//!         │               │              ├-------┼-----┐
//!         │               │              │   1   │     '
//!         │               │     3        │       │     '
//!         │               └─────────────►├───────┤     ' done_response = Some(action)
//!         │                              │       │     '
//!         │                              ├───────┘     '
//!         └───────ActionResponse─────────┴-------------┘
//!                                                3
//! ```
//!
//! [`Bridge`]: crate::Bridge
//! [`action_id`]: Action#structfield.action_id
//! [`payload`]: Action#structfield.payload
//! [`done_response`]: ActionResponse#structfield.done_response
//! [`action_redirections`]: Config#structfield.action_redirections

use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use human_bytes::human_bytes;
use log::{debug, error, info, trace, warn};
use reqwest::{Certificate, Client, ClientBuilder, Error as ReqwestError, Identity};
use rsa::sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::time::{sleep};

use std::fs::{metadata, read, remove_dir_all, remove_file, write, File};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(unix)]
use std::{
    fs::{create_dir, set_permissions, Permissions},
    path::Path,
};
use std::{io::Write, path::PathBuf};
use anyhow::Context;
use crate::base::actions::Cancellation;
use crate::config::{Authentication, Config, DownloaderConfig};
use crate::{base::bridge::BridgeTx, Action, ActionResponse};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error from reqwest: {0}")]
    Reqwest(#[from] ReqwestError),
    #[error("File io Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Empty file name")]
    EmptyFileName,
    #[error("Missing file path")]
    FilePathMissing,
    #[error("Download failed, content length zero")]
    EmptyFile,
    #[error("Downloaded file has unexpected checksum")]
    BadChecksum,
    #[error("Disk space is insufficient: {0}")]
    InsufficientDisk(String),
    #[error("Save file is corrupted")]
    BadSave,
    #[error("Save file doesn't exist")]
    NoSave,
    #[error("Download has been cancelled by '{0}'")]
    Cancelled(String),
}

/// This struct contains the necessary components to download and store file as notified by a download file
/// [`Action`], while also sending periodic [`ActionResponse`]s to update progress and finally forwarding
/// the download [`Action`], updated with information regarding where the file is stored in the file-system
/// to the connected bridge application.
pub struct FileDownloader {
    config: DownloaderConfig,
    actions_rx: Receiver<Action>,
    bridge_tx: BridgeTx,
    client: Client,
    shutdown_rx: Receiver<DownloaderShutdown>,
    disabled: Arc<Mutex<bool>>,
    downloader_state: DownloaderState,
    state_path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
enum DownloaderState {
    Idle,
    Downloading(ActiveDownloadState),
}

#[derive(Debug, Deserialize, Serialize)]
struct ActiveDownloadState {
    action_id: String,
    url: String,
    download_path: String,
    content_length: usize,
    bytes_written: usize,
    checksum: Option<String>,
    #[serde(skip)]
    file: Option<File>,
}

impl DownloaderState {
    fn save_to_file(&self, path: &Path) {
        if let Err(e) = std::fs::write(path, serde_json::to_string_pretty(&self).unwrap()) {
            log::error!("Encountered error when saving downloader state: {e}");
        }
    }
}

impl FileDownloader {
    /// Creates a handler for download actions within uplink and uses HTTP to download files.
    pub fn new(
        config: Arc<Config>,
        authentication: &Option<Authentication>,
        actions_rx: Receiver<Action>,
        bridge_tx: BridgeTx,
        shutdown_rx: Receiver<DownloaderShutdown>,
        disabled: Arc<Mutex<bool>>,
    ) -> Result<Self, Error> {
        // Authenticate with TLS certs from config
        let client_builder = ClientBuilder::new();
        let client = match authentication {
            Some(certs) => {
                let ca = Certificate::from_pem(certs.ca_certificate.as_bytes())?;
                let mut buf = BytesMut::from(certs.device_private_key.as_bytes());
                buf.extend_from_slice(certs.device_certificate.as_bytes());
                // buf contains the private key and certificate of device
                let device = Identity::from_pem(&buf)?;
                client_builder.add_root_certificate(ca).identity(device)
            }
            None => client_builder,
        }
        .build()?;

        let mut state_path = config.downloader.path.clone();
        state_path.push(".downloader_state.json");
        let downloader_state = match std::fs::read_to_string(state_path.as_path()).context("couldn't read state file")
            .and_then(|s| serde_json::from_str::<DownloaderState>(s.as_str()).context("download state file has invalid data")) {
            Ok(ds) => {
                ds
            }
            Err(_) => {
                if let Err(_) = std::fs::remove_file(state_path.as_path()) {
                    let _ = std::fs::remove_dir_all(state_path.as_path());
                }
                let result = DownloaderState::Idle;
                result.save_to_file(state_path.as_path());
                if let Err(e) = std::fs::write(state_path.as_path(), serde_json::to_string_pretty(&result).unwrap()) {
                    log::error!("Encountered error when saving downloader state: {e}");
                }
                result
            }
        };

        let result = Self {
            config: config.downloader.clone(),
            actions_rx,
            client,
            bridge_tx,
            shutdown_rx,
            disabled,
            state_path,
            downloader_state,
        };

        Ok(result)
    }

    fn download_is_active(&self) -> bool {
        match &self.downloader_state {
            DownloaderState::Idle => false,
            DownloaderState::Downloading(_) => true,
        }
    }

    fn curr_action_id(&self) -> Option<String> {
        match &self.downloader_state {
            DownloaderState::Idle => None,
            DownloaderState::Downloading(s) => Some(s.action_id.clone()),
        }
    }

    async fn process_action(&mut self, action: Action) {
        if let DownloaderState::Downloading(curr_state) = &self.downloader_state {
            let action_id = curr_state.action_id.as_str();
            if action.name == "cancel_action" {
                match serde_json::from_str::<Cancellation>(&action.payload)
                    .context("Invalid cancel action payload")
                    .and_then(|cancellation| {
                        if cancellation.action_id == action_id {
                            Ok(())
                        } else {
                            Err(anyhow::Error::msg(format!("Cancel action target ({}) doesn't match active download action id ({})", cancellation.action_id, action_id)))
                        }
                    }) {
                    Ok(_) => {
                        self.bridge_tx.send_action_response(ActionResponse::success(action.action_id.as_str())).await;
                        self.bridge_tx.send_action_response(ActionResponse::failure(action_id, "Download cancelled!")).await;

                        // whenever we set the state to downloading, we open the corresponding file as well
                        if let Err(e) = std::fs::remove_file(curr_state.download_path.as_str()) {
                            log::error!("could not delete file: {e}");
                        }
                        // drop reference so we can store new value
                        self.downloader_state = DownloaderState::Idle;
                        self.downloader_state.save_to_file(self.state_path.as_path());
                    },
                    Err(e) => {
                        self.bridge_tx.send_action_response(ActionResponse::failure(action.action_id.as_str(), format!("{e}"))).await;
                    },
                }
            } else {
                self.bridge_tx.send_action_response(
                    ActionResponse::failure(&action.action_id, "Downloader is occupied!")
                ).await;
            }
        }
    }

    async fn increment_download(&mut self) {
    }

    /// Spawn a thread to handle downloading files as notified by download actions and for forwarding the updated actions
    /// back to bridge for further processing, e.g. OTA update installation.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        info!("Downloader thread is ready to receive download actions");

        let mut timeout = std::time::Instant::now();
        loop {
            if ! self.download_is_active() {
                if let Ok(action) = self.actions_rx.recv_async().await {
                    self.process_action(action).await;
                }
            } else {
                self.increment_download().await;
                if timeout.elapsed() > Duration::from_millis(100) {
                    timeout = Instant::now();
                    if let Ok(action) = self.actions_rx.try_recv() {
                        self.process_action(action).await;
                    }
                }
            }
        }

        error!("Downloader thread stopped");
    }

    async fn initialize_download(&mut self, action: Action) -> Result<(), String> {
        let payload = match serde_json::from_str::<DownloadFile>(&action.payload) {
            Err(e) => {
                return Err(format!("Invalid payload: {e}"));
            }
            Ok(DownloadFile { file_name, .. }) if file_name.is_empty() => {
                return Err("Invalid file name".to_string());
            }
            Ok(DownloadFile { content_length: 0, .. }) => {
                return Err("Download file is zero sized!".to_string());
            },
            Ok(result) => result
        };
        // this is safe because we use only utf8 strings when creating this path
        let download_path = self.config.path.join(action.name.as_str()).join(payload.file_name.as_str()).to_str()
            .unwrap().to_string();
        if let Err(_) = std::fs::remove_file(&download_path) {
            let _ = std::fs::remove_dir_all(&download_path);
        }
        let file = File::create(&download_path)
            .map_err(|e| format!("couldn't create file: {e}"))?;
        #[cfg(unix)]
        file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o666))
            .map_err(|e| "couldn't change file permissions".to_string())?;
        self.downloader_state = DownloaderState::Downloading(ActiveDownloadState {
            action_id: action.action_id,
            url: payload.url,
            content_length: payload.content_length,
            bytes_written: 0,
            download_path,
            checksum: payload.checksum,
            file: Some(file),
        });
        Ok(())
    }
}

#[cfg(unix)]
/// Custom create_dir_all which sets permissions on each created directory, only works on unix
fn create_dirs_with_perms(path: &Path, perms: Permissions) -> std::io::Result<()> {
    let mut current_path = PathBuf::new();

    for component in path.components() {
        current_path.push(component);

        if !current_path.exists() {
            create_dir(&current_path)?;
            set_permissions(&current_path, perms.clone())?;
        }
    }

    Ok(())
}

fn check_disk_size(config: &DownloaderConfig, download: &DownloadFile) -> Result<(), Error> {
    let disk_free_space = fs2::free_space(&config.path)? as usize;

    let req_size = human_bytes(download.content_length as f64);
    let free_size = human_bytes(disk_free_space as f64);
    debug!("Download requires {req_size}; Disk free space is {free_size}");

    if download.content_length > disk_free_space {
        return Err(Error::InsufficientDisk(free_size));
    }

    Ok(())
}

/// Expected JSON format of data contained in the [`payload`] of a download file [`Action`]
///
/// [`payload`]: Action#structfield.payload
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DownloadFile {
    pub url: String,
    #[serde(alias = "content-length")]
    pub content_length: usize,
    #[serde(alias = "version")]
    pub file_name: String,
    /// Path to location in fs where file will be stored
    pub download_path: Option<PathBuf>,
    /// Checksum that can be used to verify download was successful
    pub checksum: Option<String>,
}

impl DownloadFile {
    fn verify_checksum(&self) -> Result<(), Error> {
        let Some(checksum) = &self.checksum else { return Ok(()) };
        let path = self.download_path.as_ref().expect("Downloader didn't set \"download_path\"");
        let mut file = File::open(path)?;
        let mut hasher = Sha256::new();
        io::copy(&mut file, &mut hasher)?;
        let hash = hasher.finalize();

        if checksum != &hex::encode(hash) {
            return Err(Error::BadChecksum);
        }

        Ok(())
    }
}


/// Command to remotely trigger `Downloader` shutdown
pub struct DownloaderShutdown;

/// Handle to send control messages to `Downloader`
#[derive(Debug, Clone)]
pub struct CtrlTx {
    pub(crate) inner: Sender<DownloaderShutdown>,
}

impl CtrlTx {
    /// Triggers shutdown of `Downloader`
    pub async fn trigger_shutdown(&self) {
        _ = self.inner.send_async(DownloaderShutdown).await;
    }
}

