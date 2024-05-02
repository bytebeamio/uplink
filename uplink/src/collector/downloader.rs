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
use tokio::time::{sleep, timeout_at, Instant};

use std::fs::{metadata, read, remove_dir_all, remove_file, write, File};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
#[cfg(unix)]
use std::{
    fs::{create_dir, set_permissions, Permissions},
    path::Path,
};
use std::{io::Write, path::PathBuf};

use crate::{base::bridge::BridgeTx, config::DownloaderConfig, Action, ActionResponse, Config};

// Stops from downloading if true
pub static DOWNLOADER_DISABLED: AtomicBool = AtomicBool::new(false);

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
    #[error("Download timedout")]
    Timeout,
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
    shutdown_rx: Receiver<DownloaderShutdown>,
}

impl FileDownloader {
    /// Creates a handler for download actions within uplink and uses HTTP to download files.
    pub fn new(
        config: Arc<Config>,
        actions_rx: Receiver<Action>,
        bridge_tx: BridgeTx,
        shutdown_rx: Receiver<DownloaderShutdown>,
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

        Ok(Self {
            config: config.downloader.clone(),
            actions_rx,
            client,
            bridge_tx,
            action_id: String::default(),
            shutdown_rx,
        })
    }

    /// Spawn a thread to handle downloading files as notified by download actions and for forwarding the updated actions
    /// back to bridge for further processing, e.g. OTA update installation.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) {
        self.reload().await;

        info!("Downloader thread is ready to receive download actions");
        while let Ok(action) = self.actions_rx.recv_async().await {
            action.action_id.clone_into(&mut self.action_id);
            let mut state = match DownloadState::new(action, &self.config) {
                Ok(s) => s,
                Err(e) => {
                    self.forward_error(e).await;
                    continue;
                }
            };

            // Update action status for process initiated
            let status = ActionResponse::progress(&self.action_id, "Downloading", 0);
            self.bridge_tx.send_action_response(status).await;

            if let Err(e) = self.download(&mut state).await {
                self.forward_error(e).await;
                continue;
            }

            // Forward updated action as part of response
            let DownloadState { current: CurrentDownload { action, .. }, .. } = state;
            let status = ActionResponse::done(&self.action_id, "Downloaded", Some(action));
            self.bridge_tx.send_action_response(status).await;
        }

        error!("Downloader thread stopped");
    }

    // Loads a download left uncompleted during the previous run of uplink and continues it
    async fn reload(&mut self) {
        let mut state = match DownloadState::load(&self.config) {
            Ok(s) => s,
            Err(Error::NoSave) => return,
            Err(e) => {
                warn!("Couldn't reload current_download: {e}");
                return;
            }
        };
        state.current.action.action_id.clone_into(&mut self.action_id);

        if let Err(e) = self.download(&mut state).await {
            self.forward_error(e).await;
            return;
        }

        // Forward updated action as part of response
        let DownloadState { current: CurrentDownload { action, .. }, .. } = state;
        let status = ActionResponse::done(&self.action_id, "Downloaded", Some(action));
        self.bridge_tx.send_action_response(status).await;
    }

    // Accepts `DownloadState`, sets a timeout for the action
    async fn download(&mut self, state: &mut DownloadState) -> Result<(), Error> {
        let shutdown_rx = self.shutdown_rx.clone();
        let deadline = match &state.current.action.deadline {
            Some(d) => *d,
            _ => {
                error!("Unconfigured deadline: {}", state.current.action.name);
                return Ok(());
            }
        };
        select! {
            Ok(_) = shutdown_rx.recv_async(), if !shutdown_rx.is_disconnected() => {
                if let Err(e) = state.save(&self.config) {
                    error!("Error saving current_download: {e}");
                }

                return Ok(());
            },

            // NOTE: if download has timedout don't do anything, else ensure errors are forwarded after three retries
            o = timeout_at(deadline, self.continuous_retry(state)) => match o {
                Ok(r) => r?,
                Err(_) => {
                    // unwrap is safe because download_path is expected to be Some
                    _ = remove_file(state.current.meta.download_path.as_ref().unwrap());
                    error!("Last download has timedout; file deleted");

                    return Err(Error::Timeout);
                },
            }
        }

        state.current.meta.verify_checksum()?;
        // Update Action payload with `download_path`, i.e. downloaded file's location in fs
        state.current.action.payload = serde_json::to_string(&state.current.meta)?;

        Ok(())
    }

    // A download must be retried with Range header when HTTP/reqwest errors are faced
    async fn continuous_retry(&mut self, state: &mut DownloadState) -> Result<(), Error> {
        'outer: loop {
            let mut req = self.client.get(&state.current.meta.url);
            if let Some(range) = state.retry_range() {
                warn!("Retrying download; Continuing to download file from: {range}");
                req = req.header("Range", range);
            }
            let mut stream = req.send().await?.error_for_status()?.bytes_stream();

            // Download and store to disk by streaming as chunks
            loop {
                // Checks if downloader is disabled by user or not
                if DOWNLOADER_DISABLED.load(Ordering::Acquire) {
                    // async to ensure download can be cancelled during sleep
                    sleep(Duration::from_secs(1)).await;
                }
                let Some(item) = stream.next().await else { break };
                let chunk = match item {
                    Ok(c) => c,
                    // Retry non-status errors
                    Err(e) if !e.is_status() => {
                        let status =
                            ActionResponse::progress(&self.action_id, "Download Failed", 0)
                                .add_error(e.to_string());
                        self.bridge_tx.send_action_response(status).await;
                        error!("Download failed: {e}");
                        // Retry after wait
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue 'outer;
                    }
                    Err(e) => return Err(e.into()),
                };
                if let Some(percentage) = state.write_bytes(&chunk)? {
                    let status =
                        ActionResponse::progress(&self.action_id, "Downloading", percentage);
                    self.bridge_tx.send_action_response(status).await;
                }
            }

            info!("Firmware downloaded successfully");
            break;
        }

        Ok(())
    }

    // Forward errors as action response to bridge
    async fn forward_error(&mut self, err: Error) {
        let status = ActionResponse::failure(&self.action_id, err.to_string());
        self.bridge_tx.send_action_response(status).await;
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

/// Creates file to download into
fn create_file(download_path: &PathBuf, file_name: &str) -> Result<(File, PathBuf), Error> {
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
    url: String,
    #[serde(alias = "content-length")]
    content_length: usize,
    #[serde(alias = "version")]
    file_name: String,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CurrentDownload {
    action: Action,
    meta: DownloadFile,
    time_left: Option<Duration>,
}

// A temporary structure to help us retry downloads
// that failed after partial completion.
#[derive(Debug)]
struct DownloadState {
    current: CurrentDownload,
    file: File,
    bytes_written: usize,
    percentage_downloaded: u8,
    start: Instant,
}

impl DownloadState {
    fn new(action: Action, config: &DownloaderConfig) -> Result<Self, Error> {
        // Ensure that directory for downloading file into, exists
        let mut path = config.path.clone();
        path.push(&action.name);

        #[cfg(unix)]
        create_dirs_with_perms(
            path.as_path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o777),
        )?;

        #[cfg(not(unix))]
        std::fs::create_dir_all(&path)?;

        // Extract url information from action payload
        let mut meta = match serde_json::from_str::<DownloadFile>(&action.payload)? {
            DownloadFile { file_name, .. } if file_name.is_empty() => {
                return Err(Error::EmptyFileName)
            }
            DownloadFile { content_length: 0, .. } => return Err(Error::EmptyFile),
            u => u,
        };

        check_disk_size(config, &meta)?;

        let url = meta.url.clone();

        // Create file to actually download into
        let (file, file_path) = create_file(&path, &meta.file_name)?;
        // Retry downloading upto 3 times in case of connectivity issues
        // TODO: Error out for 1XX/3XX responses
        info!(
            "Downloading from {} into {}; size = {}",
            url,
            file_path.display(),
            human_bytes(meta.content_length as f64)
        );
        meta.download_path = Some(file_path);
        let current = CurrentDownload { action, meta, time_left: None };

        Ok(Self {
            current,
            file,
            bytes_written: 0,
            percentage_downloaded: 0,
            start: Instant::now(),
        })
    }

    fn load(config: &DownloaderConfig) -> Result<Self, Error> {
        let mut path = config.path.clone();
        path.push("current_download");

        if !path.exists() {
            return Err(Error::NoSave);
        }

        let read = read(&path)?;
        let mut current: CurrentDownload = serde_json::from_slice(&read)?;
        // Calculate deadline based on written time left
        current.action.deadline = current.time_left.map(|t| Instant::now() + t);

        let file = File::open(current.meta.download_path.as_ref().unwrap())?;
        let bytes_written = file.metadata()?.len() as usize;

        remove_file(path)?;

        Ok(DownloadState {
            current,
            file,
            bytes_written,
            percentage_downloaded: 0,
            start: Instant::now(),
        })
    }

    fn save(&self, config: &DownloaderConfig) -> Result<(), Error> {
        if self.bytes_written == self.current.meta.content_length {
            return Ok(());
        }

        let mut current = self.current.clone();
        // Calculate time left based on deadline
        current.time_left = current.action.deadline.map(|t| t.duration_since(Instant::now()));
        let json = serde_json::to_vec(&current)?;

        let mut path = config.path.clone();
        path.push("current_download");
        write(path, json)?;

        Ok(())
    }

    fn retry_range(&self) -> Option<String> {
        if self.bytes_written == 0 {
            return None;
        }

        Some(format!("bytes={}-{}", self.bytes_written, self.current.meta.content_length))
    }

    fn write_bytes(&mut self, buf: &[u8]) -> Result<Option<u8>, Error> {
        let bytes_downloaded = buf.len();
        self.file.write_all(buf)?;
        self.bytes_written += bytes_downloaded;
        let size = human_bytes(self.current.meta.content_length as f64);

        // Calculate percentage on the basis of content_length
        let factor = self.bytes_written as f32 / self.current.meta.content_length as f32;
        let percentage = (99.99 * factor) as u8;

        // NOTE: ensure lesser frequency of action responses, once every percentage points
        if percentage > self.percentage_downloaded {
            self.percentage_downloaded = percentage;
            debug!(
                "Downloading: size = {size}, percentage = {percentage}, elapsed = {}s",
                self.start.elapsed().as_secs()
            );

            Ok(Some(percentage))
        } else {
            trace!(
                "Downloading: size = {size}, percentage = {}, elapsed = {}s",
                self.percentage_downloaded,
                self.start.elapsed().as_secs()
            );

            Ok(None)
        }
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
        self.inner.send_async(DownloaderShutdown).await.unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use flume::bounded;
    use serde_json::json;
    use tempdir::TempDir;

    use crate::{
        base::bridge::{DataTx, StatusTx},
        config::{ActionRoute, MqttConfig},
    };

    use super::*;

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

    fn create_bridge() -> (BridgeTx, Receiver<ActionResponse>) {
        let (inner, _) = bounded(2);
        let data_tx = DataTx { inner };
        let (inner, status_rx) = bounded(2);
        let status_tx = StatusTx { inner };

        (BridgeTx { data_tx, status_tx }, status_rx)
    }

    // Prepare config
    fn test_config(temp_dir: &Path, test_name: &str) -> Config {
        let mut path = PathBuf::from(temp_dir);
        path.push(test_name);
        let downloader_cfg = DownloaderConfig {
            actions: vec![ActionRoute {
                name: "firmware_update".to_owned(),
                timeout: Duration::from_secs(10),
            }],
            path,
        };
        config(downloader_cfg.clone())
    }

    #[test]
    // Test file downloading capabilities of FileDownloader by downloading the uplink logo from GitHub
    fn download_file() {
        let temp_dir = TempDir::new("download_file").unwrap();
        let config = test_config(temp_dir.path(), "download_file");
        let mut downloader_path = config.downloader.path.clone();
        let (bridge_tx, status_rx) = create_bridge();

        // Create channels to forward and push actions on
        let (download_tx, download_rx) = bounded(1);
        let (_, ctrl_rx) = bounded(1);
        let downloader =
            FileDownloader::new(Arc::new(config), download_rx, bridge_tx, ctrl_rx).unwrap();

        // Start FileDownloader in separate thread
        std::thread::spawn(|| downloader.start());

        // Create a firmware update action
        let download_update = DownloadFile {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            content_length: 296658,
            file_name: "test.txt".to_string(),
            download_path: None,
            checksum: None,
        };
        let mut expected_forward = download_update.clone();
        downloader_path.push("firmware_update");
        downloader_path.push("test.txt");
        expected_forward.download_path = Some(downloader_path);
        let download_action = Action {
            action_id: "1".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(download_update).to_string(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
        };

        std::thread::sleep(Duration::from_millis(10));

        // Send action to FileDownloader with Sender<Action>
        download_tx.try_send(download_action).unwrap();

        // Collect action_status and ensure it is as expected
        let status = status_rx.recv().unwrap();
        assert_eq!(status.state, "Downloading");
        let mut progress = 0;

        // Collect and ensure forwarded action contains expected info
        loop {
            let status = status_rx.recv().unwrap();

            assert!(progress <= status.progress);
            progress = status.progress;

            if status.is_done() {
                let fwd_action = status.done_response.unwrap();
                let fwd = serde_json::from_str(&fwd_action.payload).unwrap();
                assert_eq!(expected_forward, fwd);
                break;
            }
        }
    }

    #[test]
    // Once a file is downloaded FileDownloader must check it's checksum value against what is provided
    fn checksum_of_file() {
        let temp_dir = TempDir::new("file_checksum").unwrap();
        let config = test_config(temp_dir.path(), "file_checksum");
        let (bridge_tx, status_rx) = create_bridge();

        // Create channels to forward and push action_status on
        let (download_tx, download_rx) = bounded(1);
        let (_, ctrl_rx) = bounded(1);
        let downloader =
            FileDownloader::new(Arc::new(config), download_rx, bridge_tx, ctrl_rx).unwrap();

        // Start FileDownloader in separate thread
        std::thread::spawn(|| downloader.start());

        std::thread::sleep(Duration::from_millis(10));

        // Correct firmware update action
        let correct_update = DownloadFile {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            content_length: 296658,
            file_name: "logo.png".to_string(),
            download_path: None,
            checksum: Some(
                "e22d4a7cf60ad13bf885c6d84af2f884f0c044faf0ee40b2e3c81896b226b2fc".to_string(),
            ),
        };
        let correct_action = Action {
            action_id: "1".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(correct_update).to_string(),
            deadline: Some(Instant::now() + Duration::from_secs(100)),
        };

        // Send the correct action to FileDownloader
        download_tx.try_send(correct_action).unwrap();

        // Collect action_status and ensure it is as expected
        let status = status_rx.recv().unwrap();
        assert_eq!(status.state, "Downloading");
        let mut progress = 0;

        // Collect and ensure forwarded action contains expected info
        loop {
            let status = status_rx.recv().unwrap();

            assert!(progress <= status.progress);
            progress = status.progress;

            if status.is_done() {
                if status.state != "Downloaded" {
                    panic!("unexpected status={status:?}")
                }
                break;
            }
        }

        // Wrong firmware update action
        let wrong_update = DownloadFile {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            content_length: 296658,
            file_name: "logo.png".to_string(),
            download_path: None,
            checksum: Some("abcd1234efgh5678".to_string()),
        };
        let wrong_action = Action {
            action_id: "1".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(wrong_update).to_string(),
            deadline: Some(Instant::now() + Duration::from_secs(100)),
        };

        // Send the wrong action to FileDownloader
        download_tx.try_send(wrong_action).unwrap();

        // Collect action_status and ensure it is as expected
        let status = status_rx.recv().unwrap();
        assert_eq!(status.state, "Downloading");
        let mut progress = 0;

        // Collect and ensure forwarded action contains expected info
        loop {
            let status = status_rx.recv().unwrap();

            assert!(progress <= status.progress);
            progress = status.progress;

            if status.is_done() {
                assert!(status.is_failed());
                assert_eq!(status.errors, vec!["Downloaded file has unexpected checksum"]);
                break;
            }
        }
    }
}
