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
use flume::Receiver;
use futures_util::StreamExt;
use human_bytes::human_bytes;
use log::{debug, error, info, trace, warn};
use reqwest::{Certificate, Client, ClientBuilder, Identity, Response};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use std::collections::HashMap;
use std::fs::{metadata, remove_dir_all, File};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(unix)]
use std::{
    fs::{create_dir, set_permissions, Permissions},
    path::Path,
};
use std::{io::Write, path::PathBuf};

use crate::base::bridge::BridgeTx;
use crate::base::DownloaderConfig;
use crate::{Action, ActionResponse, Config};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error from reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("File io Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Empty file name")]
    EmptyFileName,
    #[error("Missing file path")]
    FilePathMissing,
    #[error("Download failed, content length zero")]
    EmptyFile,
    #[error("Disk space is insufficient: {0}")]
    InsufficientDisk(String),
}

/// This struct contains the necessary components to download and store file as notified by a download file
/// [`Action`], while also sending periodic [`ActionResponse`]s to update progress and finally forwarding
/// the download [`Action`], updated with information regarding where the file is stored in the file-system
/// to the connected bridge application.
pub struct FileDownloader {
    config: DownloaderConfig,
    actions_rx: Receiver<Action>,
    action_id: String,
    bridge_tx: BridgeTx,
    client: Client,
    sequence: u32,
    timeouts: HashMap<String, Duration>,
}

impl FileDownloader {
    /// Creates a handler for download actions within uplink and uses HTTP to download files.
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        bridge_tx: BridgeTx,
    ) -> Result<Self, Error> {
        // Authenticate with TLS certs from config
        let client_builder = ClientBuilder::new();
        let client = match &config.authentication {
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

        let timeouts = config
            .downloader
            .actions
            .iter()
            .map(|s| (s.name.to_owned(), Duration::from_secs(s.timeout)))
            .collect();

        Ok(Self {
            config: config.downloader.clone(),
            actions_rx,
            timeouts,
            client,
            bridge_tx,
            sequence: 0,
            action_id: String::default(),
        })
    }

    /// Spawn a thread to handle downloading files as notified by download actions and for forwarding the updated actions
    /// back to bridge for further processing, e.g. OTA update installation.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        info!("Downloader thread is ready to receive download actions");
        loop {
            self.sequence = 0;
            let action = match self.actions_rx.recv_async().await {
                Ok(a) => a,
                Err(e) => {
                    error!("Downloader thread had to stop: {e}");
                    break;
                }
            };
            self.action_id = action.action_id.clone();

            let duration = match self.timeouts.get(&action.name) {
                Some(t) => *t,
                _ => {
                    error!("Action: {} unconfigured", action.name);
                    continue;
                }
            };

            // NOTE: if download has timedout don't do anything, else ensure errors are forwarded after three retries
            match timeout(duration, self.retry_thrice(action)).await {
                Ok(Err(e)) => self.forward_error(e).await,
                Err(_) => error!("Last download has timedout"),
                _ => {}
            }
        }
    }

    // Forward errors as action response to bridge
    async fn forward_error(&mut self, err: Error) {
        let status =
            ActionResponse::failure(&self.action_id, err.to_string()).set_sequence(self.sequence());
        self.bridge_tx.send_action_response(status).await;
    }

    // Retry mechanism tries atleast 3 times before returning an error
    async fn retry_thrice(&mut self, action: Action) -> Result<(), Error> {
        for _ in 0..3 {
            match self.run(action.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    if let Error::Reqwest(e) = e {
                        error!("Download failed: {e}");
                    } else {
                        return Err(e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
            warn!("Retrying download");
        }

        Ok(())
    }

    // Accepts a download `Action` and performs necessary data extraction to actually download the file
    async fn run(&mut self, mut action: Action) -> Result<(), Error> {
        // Update action status for process initiated
        let status = ActionResponse::progress(&self.action_id, "Downloading", 0);
        let status = status.set_sequence(self.sequence());
        self.bridge_tx.send_action_response(status).await;

        // Ensure that directory for downloading file into, exists
        let mut download_path = self.config.path.clone();
        download_path.push(&action.name);

        #[cfg(unix)]
        self.create_dirs_with_perms(
            download_path.as_path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o777),
        )?;

        #[cfg(not(unix))]
        std::fs::create_dir_all(&download_path)?;

        // Extract url information from action payload
        let mut update = match serde_json::from_str::<DownloadFile>(&action.payload)? {
            DownloadFile { file_name, .. } if file_name.is_empty() => {
                return Err(Error::EmptyFileName)
            }
            DownloadFile { content_length: 0, .. } => return Err(Error::EmptyFile),
            u => u,
        };

        self.check_disk_size(&update)?;

        let url = update.url.clone();

        // Create file to actually download into
        let (file, file_path) = self.create_file(&download_path, &update.file_name)?;

        // Create handler to perform download from URL
        // TODO: Error out for 1XX/3XX responses
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        info!("Downloading from {} into {}", url, file_path.display());
        self.download(resp, file, update.content_length).await?;

        // Update Action payload with `download_path`, i.e. downloaded file's location in fs
        update.download_path = Some(file_path.clone());
        action.payload = serde_json::to_string(&update)?;

        let status = ActionResponse::done(&self.action_id, "Downloaded", Some(action));
        let status = status.set_sequence(self.sequence());
        self.bridge_tx.send_action_response(status).await;

        Ok(())
    }

    fn check_disk_size(&mut self, download: &DownloadFile) -> Result<(), Error> {
        let disk_free_space = fs2::free_space(&self.config.path)? as usize;

        let req_size = human_bytes(download.content_length as f64);
        let free_size = human_bytes(disk_free_space as f64);
        debug!("Download requires {req_size}; Disk free space is {free_size}");

        if download.content_length > disk_free_space {
            return Err(Error::InsufficientDisk(free_size));
        }

        Ok(())
    }

    #[cfg(unix)]
    /// Custom create_dir_all which sets permissions on each created directory, only works on unix
    fn create_dirs_with_perms(&self, path: &Path, perms: Permissions) -> std::io::Result<()> {
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

    /// Creates file to download into
    fn create_file(
        &self,
        download_path: &PathBuf,
        file_name: &str,
    ) -> Result<(File, PathBuf), Error> {
        let mut file_path = download_path.to_owned();
        file_path.push(file_name);
        // NOTE: if file_path is occupied by a directory due to previous working of uplink, remove it
        if let Ok(f) = metadata(&file_path) {
            if f.is_dir() {
                remove_dir_all(&file_path)?;
            }
        }
        let file = File::create(&file_path)?;
        #[cfg(unix)]
        file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o666))?;

        Ok((file, file_path))
    }

    /// Downloads from server and stores into file
    async fn download(
        &mut self,
        resp: Response,
        mut file: File,
        content_length: usize,
    ) -> Result<(), Error> {
        let mut downloaded = 0;
        let mut next = 1;
        let mut stream = resp.bytes_stream();
        let start = Instant::now();
        let size = human_bytes(content_length as f64);

        debug!("Download started: size = {size}");

        // Download and store to disk by streaming as chunks
        while let Some(item) = stream.next().await {
            let chunk = item?;
            downloaded += chunk.len();
            file.write_all(&chunk)?;

            // Calculate percentage on the basis of content_length
            let percentage = 99 * downloaded / content_length;

            trace!(
                "Downloading: size = {size}, percentage = {percentage}, elapsed = {}s",
                start.elapsed().as_secs()
            );
            // NOTE: ensure lesser frequency of action responses, once every percentage points
            if percentage >= next {
                next += 1;

                debug!(
                    "Downloading: size = {size}, percentage = {percentage}, elapsed = {}s",
                    start.elapsed().as_secs()
                );
                //TODO: Simplify progress by reusing action_id and state
                //TODO: let response = self.response.progress(percentage);??
                let status =
                    ActionResponse::progress(&self.action_id, "Downloading", percentage as u8);
                let status = status.set_sequence(self.sequence());
                self.bridge_tx.send_action_response(status).await;
            }
        }

        info!("Firmware downloaded successfully");

        Ok(())
    }

    fn sequence(&mut self) -> u32 {
        self.sequence += 1;
        self.sequence
    }
}

/// Expected JSON format of data contained in the [`payload`] of a download file [`Action`]
///
/// [`payload`]: Action#structfield.payload
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct DownloadFile {
    url: String,
    #[serde(alias = "content-length")]
    content_length: usize,
    #[serde(alias = "version")]
    file_name: String,
    /// Path to location in fs where file will be stored
    pub download_path: Option<PathBuf>,
}

#[cfg(test)]
mod test {
    use flume::{bounded, TrySendError};
    use serde_json::json;

    use std::{collections::HashMap, time::Duration};

    use super::*;
    use crate::base::{bridge::Event, ActionRoute, DownloaderConfig, MqttConfig};

    const DOWNLOAD_DIR: &str = "/tmp/uplink_test";

    fn config(downloader: DownloaderConfig) -> Config {
        Config {
            broker: "localhost".to_owned(),
            port: 1883,
            device_id: "123".to_owned(),
            streams: HashMap::new(),
            mqtt: MqttConfig { max_packet_size: 1024 * 1024, ..Default::default() },
            downloader,
            ..Default::default()
        }
    }

    #[test]
    // Test file downloading capabilities of FileDownloader by downloading the uplink logo from GitHub
    fn download_file() {
        // Ensure path exists
        std::fs::create_dir_all(DOWNLOAD_DIR).unwrap();
        // Prepare config
        let mut path = PathBuf::from(DOWNLOAD_DIR);
        path.push("uplink-test");
        let downloader_cfg = DownloaderConfig {
            actions: vec![ActionRoute { name: "firmware_update".to_owned(), timeout: 10 }],
            path,
        };
        let config = config(downloader_cfg.clone());
        let (events_tx, events_rx) = flume::bounded(2);
        let (shutdown_handle, _) = bounded(1);
        let bridge_tx = BridgeTx { events_tx, shutdown_handle };

        // Create channels to forward and push actions on
        let (download_tx, download_rx) = bounded(1);
        let downloader = FileDownloader::new(Arc::new(config), download_rx, bridge_tx).unwrap();

        // Start FileDownloader in separate thread
        std::thread::spawn(|| downloader.start());

        // Create a firmware update action
        let download_update = DownloadFile {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            content_length: 296658,
            file_name: "test.txt".to_string(),
            download_path: None,
        };
        let mut expected_forward = download_update.clone();
        let mut path = downloader_cfg.path;
        path.push("firmware_update");
        path.push("test.txt");
        expected_forward.download_path = Some(path);
        let download_action = Action {
            action_id: "1".to_string(),
            kind: "firmware_update".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(download_update).to_string(),
        };

        std::thread::sleep(Duration::from_millis(10));

        // Send action to FileDownloader with Sender<Action>
        download_tx.try_send(download_action).unwrap();

        // Collect action_status and ensure it is as expected
        let status = match events_rx.recv().unwrap() {
            Event::ActionResponse(status) => status,
            e => unreachable!("Unexpected event: {e:#?}"),
        };
        assert_eq!(status.state, "Downloading");
        let mut progress = 0;

        // Collect and ensure forwarded action contains expected info
        loop {
            let status = match events_rx.recv().unwrap() {
                Event::ActionResponse(status) => status,
                e => unreachable!("Unexpected event: {e:#?}"),
            };

            assert!(progress <= status.progress);
            progress = status.progress;

            if status.is_done() {
                let fwd_action = status.done_response.unwrap();
                let fwd = serde_json::from_str(&fwd_action.payload).unwrap();
                assert_eq!(expected_forward, fwd);
                break;
            } else if status.is_failed() {
                break;
            }
        }
    }

    #[test]
    fn multiple_actions_at_once() {
        // Ensure path exists
        std::fs::create_dir_all(DOWNLOAD_DIR).unwrap();
        // Prepare config
        let mut path = PathBuf::from(DOWNLOAD_DIR);
        path.push("download");
        let downloader_cfg = DownloaderConfig {
            actions: vec![ActionRoute { name: "firmware_update".to_owned(), timeout: 10 }],
            path,
        };
        let config = config(downloader_cfg.clone());
        let (events_tx, _) = flume::bounded(3);
        let (shutdown_handle, _) = bounded(1);
        let bridge_tx = BridgeTx { events_tx, shutdown_handle };

        // Create channels to forward and push actions on
        let (download_tx, download_rx) = bounded(1);
        let downloader = FileDownloader::new(Arc::new(config), download_rx, bridge_tx).unwrap();

        // Start FileDownloader in separate thread
        std::thread::spawn(|| downloader.start());

        // Create a firmware update action
        let download_update = DownloadFile {
            content_length: 0,
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            file_name: "1.0".to_string(),
            download_path: None,
        };
        let mut expected_forward = download_update.clone();
        let mut path = downloader_cfg.path;
        path.push("firmware_update");
        path.push("test.txt");
        expected_forward.download_path = Some(path);
        let download_action = Action {
            action_id: "1".to_string(),
            kind: "firmware_update".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(download_update).to_string(),
        };

        std::thread::sleep(Duration::from_millis(10));

        // Send action to FileDownloader with Sender<Action>
        download_tx.try_send(download_action.clone()).unwrap();

        // Send action to FileDownloader immediately after, this must fail
        match download_tx.try_send(download_action).unwrap_err() {
            TrySendError::Full(_) => {}
            TrySendError::Disconnected(_) => panic!("Unexpected disconnect"),
        }
    }
}
