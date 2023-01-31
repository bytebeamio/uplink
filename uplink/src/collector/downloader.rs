//! Contains definitions necessary to download files such as OTA updates, as notified by a download file [`Action`]
//!
//! The thread running [`Actions`] forwards the appropriate `Action`s to the [`FileDownloader`] with a _rendezvous channel_(A channel bounded to 0).
//! The `Action` is sent only when a receiver is awaiting on the otherside and fails otherwise. This allows us to instantly recognize that another
//! download is currently being performed.
//!
//! Download file `Action`s contain JSON formatted [`payload`] which can be deserialized into an object of type [`DownloadFile`].
//! This object contains information such as the `url` where the download file is accessible from, the `version` number associated
//! with the downloaded file and a field which must be updated with the location in file-system where the file is stored into.
//!
//! The `FileDownloader` also updates the state of a downloading `Action` using periodic [`ActionResponse`]s containing
//! progress information. On completion of a download, the received `Action`'s `payload` is updated to contain information
//! about where the file was downloaded into, within the file-system.
//!
//! ```text
//! ┌───────┐                                              ┌──────────────┐
//! │Actions│                                              │FileDownloader│
//! └───┬───┘                                              └──────┬───────┘
//!     │                                                         │
//!     │ .try_send()                                     .recv() │
//!     │  1  ┌──────────────┐             ┌────────────────┐  1  │
//!     ├─────►Sender<Action>├──────x──────►Receiver<Action>├────►│───────┐
//!     │     └───▲─────▲────┘             └───────┬────────┘     │       │          ┌─────────────┐
//!     │  2      │     │                          │              ├──ActionResponse──►action_status│
//!     ├─Action──┘     │                          │              │       │          └─────────────┘
//!     │               │                          │              ├───────┤
//!     │  3            │                          │              │-------│-┐
//!     ├───────────────┘                          │              │   1   │ '
//!     │                                          │     3        │       │ '          ┌─────────┐
//!     │                                          └─────────────►│───────┤ ├--Action--►bridge_tx│
//!     │                                                         │       │ '          └─────────┘
//!     │                                                         ├───────┘ '
//!     │                                                         │---------┘
//!                                                                   3
//! ```
//!
//! **NOTE:** The [`Actions`] thread will block between sending a `Some(..)` and a `None` onto the download channel.
//!
//! [`Actions`]: crate::Actions
//! [`payload`]: Action#structfield.payload
//! [`send()`]: Sender::send()
//! [`try_send()`]: Sender::try_send()
//! [`Error::Downloading`]: crate::base::actions::Error::Downloading

use bytes::BytesMut;
use flume::RecvError;
use futures_util::StreamExt;
use log::{error, info};
use reqwest::{Certificate, Client, ClientBuilder, Identity, Response};
use serde::{Deserialize, Serialize};

use std::fs::{create_dir_all, File};
use std::sync::Arc;
use std::time::Duration;
use std::{io::Write, path::PathBuf};

use crate::base::bridge::BridgeTx;
use crate::{Action, ActionResponse, Config};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error from reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("File io Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error forwarding download Action to bridge: {0}")]
    TrySend(Box<flume::TrySendError<Action>>),
    #[error("Error receiving action: {0}")]
    Recv(#[from] RecvError),
    #[error("Missing file name: {0}")]
    FileNameMissing(String),
    #[error("Missing file path")]
    FilePathMissing,
    #[error("Download failed, content length zero")]
    EmptyFile,
    #[error("Couldn't install apk")]
    InstallationError(String),
}

impl From<flume::TrySendError<Action>> for Error {
    fn from(e: flume::TrySendError<Action>) -> Self {
        Self::TrySend(Box::new(e))
    }
}

/// This struct contains the necessary components to download and store file as notified by a download file
/// [`Action`], while also sending periodic [`ActionResponse`]s to update progress and finally forwarding
/// the download [`Action`], updated with information regarding where the file is stored in the file-system
/// to the connected bridge application.
pub struct FileDownloader {
    config: Arc<Config>,
    action_id: String,
    bridge_tx: BridgeTx,
    client: Client,
    sequence: u32,
}

impl FileDownloader {
    /// Create a struct to manage downloads, runs on a separate thread. Also returns a [`Sender`]
    /// end of a "One" channel to send associated actions onto.
    pub fn new(config: Arc<Config>, bridge_tx: BridgeTx) -> Result<Self, Error> {
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

        Ok(Self { config, client, bridge_tx, sequence: 0, action_id: String::default() })
    }

    /// Spawn a thread to handle downloading files as notified by download actions and for forwarding the updated actions
    /// to bridge for further processing, e.g. OTA update installation.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        let routes = &self.config.actions;
        let download_rx = self.bridge_tx.register_action_routes(routes).await;
        loop {
            self.sequence = 0;
            // The 0 sized channel only allows one action to be in execution at a time. Only one action is accepted below,
            // all download actions before and after, till the next recv() won't get executed and will be reported to cloud.
            let action = download_rx.recv_async().await?;
            self.action_id = action.action_id.clone();

            let mut error = None;
            for _ in 0..3 {
                match self.run(action.clone()).await {
                    Ok(_) => {
                        error = None;
                        break;
                    }
                    Err(e) => {
                        error!("Download failed: {e}\nretrying");
                        error = Some(e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            if let Some(e) = error {
                let status = ActionResponse::failure(&self.action_id, e.to_string());
                let status = status.set_sequence(self.sequence());
                self.bridge_tx.send_action_response(status).await;
            }
        }
    }

    // Accepts a download `Action` and performs necessary data extraction to actually download the file
    async fn run(&mut self, mut action: Action) -> Result<(), Error> {
        // Update action status for process initiated
        let status = ActionResponse::progress(&self.action_id, "Downloading", 0);
        let status = status.set_sequence(self.sequence());
        self.bridge_tx.send_action_response(status).await;

        // Extract url information from action payload
        let mut update = serde_json::from_str::<DownloadFile>(&action.payload)?;
        let url = update.url.clone();

        // Create file to actually download into
        let (file, file_path) = self.create_file(&action.name, &url, &update.version)?;

        // Create handler to perform download from URL
        // TODO: Error out for 1XX/3XX responses
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        info!("Downloading from {} into {}", url, file_path);
        self.download(resp, file).await?;

        // Update Action payload with `download_path`, i.e. downloaded file's location in fs
        update.download_path = Some(file_path.clone());
        action.payload = serde_json::to_string(&update)?;

        let status = ActionResponse::progress(&self.action_id, "Downloaded", 50);
        let status = status.set_sequence(self.sequence());
        self.bridge_tx.send_action_response(status).await;

        Ok(())
    }

    /// Creates file to download into
    fn create_file(&self, name: &str, url: &str, version: &str) -> Result<(File, String), Error> {
        // Ensure that directory for downloading file into, of the format `path/to/{version}/`, exists
        let mut download_path = PathBuf::from(self.config.downloader.path.clone());
        download_path.push(name);
        download_path.push(version);
        create_dir_all(&download_path)?;

        let mut file_path = download_path.to_owned();
        let file_name =
            url.split('/').last().ok_or_else(|| Error::FileNameMissing(url.to_owned()))?;
        file_path.push(file_name);
        let file_path = file_path.as_path();
        let file = File::create(file_path)?;
        let file_path = file_path.to_str().ok_or(Error::FilePathMissing)?.to_owned();

        Ok((file, file_path))
    }

    /// Downloads from server and stores into file
    async fn download(&mut self, resp: Response, mut file: File) -> Result<(), Error> {
        // Error out in case of 0 sized files, but handle situation where file size is not
        // reported by the webserver in response by incrementing count 0..100 over and over.
        let content_length = match resp.content_length() {
            None => None,
            Some(0) => return Err(Error::EmptyFile),
            Some(l) => Some(l as usize),
        };
        let mut downloaded = 0;
        let mut next = 1;
        let mut stream = resp.bytes_stream();

        // Download and store to disk by streaming as chunks
        while let Some(item) = stream.next().await {
            let chunk = item?;
            downloaded += chunk.len();
            file.write_all(&chunk)?;

            // NOTE: ensure lesser frequency of action responses, once every 100KB
            if downloaded / 102400 > next {
                next += 1;
                // Calculate percentage on the basis of content_length if available,
                // else increment 0..100 till task is completed.
                let percentage = match content_length {
                    Some(content_length) => 100 * downloaded / content_length,
                    None => {
                        downloaded = (downloaded + 1) % 101;
                        downloaded
                    }
                };
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
    #[serde(alias = "file_name")]
    version: String,
    /// Path to location in fs where file will be stored
    download_path: Option<String>,
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use crate::base::{bridge::Event, Downloader};

    use super::*;
    use flume::TrySendError;
    use serde_json::json;

    const DOWNLOAD_DIR: &str = "/tmp/uplink_test";

    fn config(downloader: Downloader) -> Config {
        Config {
            broker: "localhost".to_owned(),
            port: 1883,
            device_id: "123".to_owned(),
            streams: HashMap::new(),
            max_packet_size: 1024 * 1024,
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
        let downloader_cfg = Downloader {
            actions: vec!["firmware_update".to_owned()],
            path: format!("{DOWNLOAD_DIR}/uplink-test"),
        };
        let config = config(downloader_cfg.clone());
        let (events_tx, events_rx) = flume::bounded(1);
        let bridge_tx = BridgeTx { events_tx };

        // Create channels to forward and push action_status on
        let downloader = FileDownloader::new(Arc::new(config), bridge_tx).unwrap();

        // Start FileDownloader in separate thread
        std::thread::spawn(|| downloader.start().unwrap());

        // Create a firmware update action
        let download_update = DownloadFile {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            version: "1.0".to_string(),
            download_path: None,
        };
        let mut expected_forward = download_update.clone();
        expected_forward.download_path =
            Some(downloader_cfg.path + "/firmware_update/1.0/logo.png");
        let download_action = Action {
            device_id: None,
            action_id: "1".to_string(),
            kind: "firmware_update".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(download_update).to_string(),
        };

        let download_tx = match events_rx.recv().unwrap() {
            Event::RegisterActionRoute(_, download_tx) => download_tx,
            e => unreachable!("Unexpected event: {e:#?}"),
        };

        match events_rx.recv().unwrap() {
            Event::RegisterActionRoute(_, _) => {}
            e => unreachable!("Unexpected event: {e:#?}"),
        }

        std::thread::sleep(Duration::from_millis(10));

        // Send action to FileDownloader with Sender<Action>
        download_tx.try_send(download_action).unwrap();

        // Collect action_status and ensure it is as expected
        let status = match events_rx.recv().unwrap() {
            Event::ActionResponse(status) => status,
            e => unreachable!("Unexpected event: {e:#?}"),
        };
        assert_eq!(status.state, "Downloading");

        // Collect and ensure forwarded action contains expected info
        // let Event::RegisterActionRoute(_, download_tx) = events_rx.recv().unwrap();
        // let forward: DownloadFile = serde_json::from_str(&forwardpayload).unwrap();
        // assert_eq!(forward, expected_forward);
    }

    #[test]
    fn multiple_actions_at_once() {
        // Ensure path exists
        std::fs::create_dir_all(DOWNLOAD_DIR).unwrap();
        // Prepare config
        let downloader_cfg = Downloader {
            actions: vec!["firmware_update".to_string()],
            path: format!("{}/download", DOWNLOAD_DIR),
        };
        let config = config(downloader_cfg.clone());
        let (events_tx, events_rx) = flume::bounded(1);
        let bridge_tx = BridgeTx { events_tx };

        // Create channels to forward and push action_status on
        let downloader = FileDownloader::new(Arc::new(config), bridge_tx).unwrap();

        // Start FileDownloader in separate thread
        std::thread::spawn(|| downloader.start().unwrap());

        // Create a firmware update action
        let download_update = DownloadFile {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            version: "1.0".to_string(),
            download_path: None,
        };
        let mut expected_forward = download_update.clone();
        expected_forward.download_path =
            Some(downloader_cfg.path + "/firmware_update/1.0/logo.png");
        let download_action = Action {
            device_id: None,
            action_id: "1".to_string(),
            kind: "firmware_update".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(download_update).to_string(),
        };

        let download_tx = match events_rx.recv().unwrap() {
            Event::RegisterActionRoute(_, download_tx) => download_tx,
            e => unreachable!("Unexpected event: {e:#?}"),
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
