//! Contains definitions necessary to Download OTA updates as notified by an [`Action`] with `name: "update_firmware"`
//!
//! The thread running [`Actions`] forwards an OTA `Action` to the [`OtaDownloader`] with an [`OneTx<Action>`], with a
//! [`try_send()`] of a value with type `Option<Action>`. If the channel is filled, another OTA is deemed to be downloading
//! and thus the `Action` can't be forwarded through the channel to [`OneRx<Action>`] and an [`Error::Downloading`] message
//! is returned.
//!
//! If the channel is not filled, the `Action` is sent onto the channel with the value being `Some(..)` and a blocking
//! [`send()`] with the value being `None` is sent to fill the channel and signal a running OTA download. This ensures that
//! multiple OTAs aren't waiting to be downloaded and is illustrated in the following diagram by how `Action` 2 doesn't reach
//! the `OtaDownloader` and is hence rejected.
//!
//! OTA `Action`s contain JSON formatted [`payload`] which can be deserialized into an object of type [`FirmwareUpdate`].
//! This object contains information such as the `url` where the OTA file is accessible from, the `version` number associated
//! with the OTA file and a field which must be updated with the location in file-system where the OTA file is stored into.
//!
//! The `OtaDownloader` also updates the state of a downloading `Action` using periodic [`ActionResponse`]s containing
//! progress information. On completion of a download, the received `Action`'s `payload` is updated to contain information
//! about where the OTA was downloaded into, within the file-system.
//!
//! ```text
//! ┌───────┐                                              ┌─────────────┐
//! │Actions│                                              │OtaDownloader│
//! └───┬───┘                                              └──────┬──────┘
//!     │          .try_send(Some(Action))   .recv()              │
//!     │.send() ┌────────────────────────────────────┐    .recv()│
//!     │  1  ┌──┴──┐                              ┌──▼──┐  1     │
//!     ├─────►OneTx├───────────────x──────────────┤OneRx├───────►│───────┐
//!     │     └▲───▲┘ .send(None)           .recv()└──┬──┘        │       │          ┌─────────────┐
//!     │  2   │   │                                  │           ├──ActionResponse──►action_status│
//!     ├Action┘   │                                  │           │       │          └─────────────┘
//!     │          │                                  │           ├───────┤
//!     │  3       │                                  │           │-------│-┐
//!     ├──────────┘                                  │           │   1   │ '
//!     │                                             │     3     │       │ '          ┌─────────┐
//!     │                                             └──────────►│───────┤ ├--Action--►bridge_tx│
//!     │                                                         │       │ '          └─────────┘
//!     │                                                         ├───────┘ '
//!     │                                                         │---------┘
//!                                                                   3
//! ```
//!
//! **NOTE:** The [`Actions`] thread will block between sending a `Some(..)` and a `None` onto the OTA channel.
//!
//! [`Actions`]: crate::Actions
//! [`payload`]: Action#structfield.payload
//! [`send()`]: Sender::send()
//! [`try_send()`]: Sender::try_send()
//! [`Error::Downloading`]: crate::base::actions::Error::Downloading

use bytes::BytesMut;
use flume::Sender;
use futures_util::StreamExt;
use log::{debug, error, info};
use reqwest::{Certificate, Client, ClientBuilder, Identity, Response};
use serde::{Deserialize, Serialize};

use std::fs::{create_dir_all, File};
use std::{io::Write, path::PathBuf, sync::Arc};

use super::{Action, ActionResponse};
use crate::base::{Config, OneChannel, OneError, OneRx, OneTx, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error from reqwest: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("File io Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Error forwarding OTA Action to bridge: {0}")]
    TrySendError(#[from] flume::TrySendError<Action>),
    #[error("Error receiving action: {0}")]
    Recv(#[from] OneError<Action>),
    #[error("Missing file name: {0}")]
    FileNameMissing(String),
    #[error("Missing file path")]
    FilePathMissing,
    #[error("Download failed, content length zero")]
    EmptyFile,
}

/// This struct contains the necessary components to download and store an OTA update as notified
/// by an [`Action`] with `name: "update_firmware"` while also sending periodic [`ActionResponse`]s
/// to update progress and finally forwarding the OTA [`Action`], updated with information regarding
/// where the OTA file is stored in the file-system.
pub struct OtaDownloader {
    config: Arc<Config>,
    action_id: String,
    status_bucket: Stream<ActionResponse>,
    ota_rx: OneRx<Action>,
    bridge_tx: Sender<Action>,
    client: Client,
    sequence: u32,
}

impl OtaDownloader {
    /// Create a struct to manage OTA downloads, runs on a separate thread. Also returns a [`Sender`]
    /// end of a "One" channel to send OTA actions onto.
    pub fn new(
        config: Arc<Config>,
        status_bucket: Stream<ActionResponse>,
        bridge_tx: Sender<Action>,
    ) -> Result<(OneTx<Action>, Self), Error> {
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

        let (ota_tx, ota_rx) = OneChannel::new();

        Ok((
            ota_tx,
            Self {
                config,
                client,
                ota_rx,
                status_bucket,
                bridge_tx,
                sequence: 0,
                action_id: String::default(),
            },
        ))
    }

    /// Spawn a thread to handle downloading OTA updates as per "update_firmware" actions and for
    /// forwarding updated actions to bridge for further processing, i.e. update installation.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        loop {
            self.sequence = 0;
            // `_lock` is held until current action completes, hence failing all sends till it's dropped
            let action = self.ota_rx.recv()?;
            self.action_id = action.action_id.clone();

            match self.run(action).await {
                Ok(_) => {
                    let status = ActionResponse::progress(&self.action_id, "Downloaded", 100)
                        .set_sequence(self.sequence());
                    self.send_status(status).await;
                }
                Err(e) => {
                    let status = ActionResponse::failure(&self.action_id, e.to_string())
                        .set_sequence(self.sequence());
                    self.send_status(status).await;
                }
            }
            // _lock is dropped here so that next OTA Action can be received
        }
    }

    // Accepts an OTA `Action` and performs necessary data extraction to actually download the OTA update
    async fn run(&mut self, action: Action) -> Result<(), Error> {
        // Update action status for process initiated
        let status = ActionResponse::progress(&self.action_id, "Downloading", 0)
            .set_sequence(self.sequence());
        self.send_status(status).await;

        // Extract url and add ota_path in payload before recreating action to be sent to bridge
        let Action { action_id, kind, name, payload } = action;
        let mut update = serde_json::from_str::<FirmwareUpdate>(&payload)?;
        let url = update.url.clone();

        // Ensure that directory for downloading file into, of the format `path/to/{version}/`, exists
        let mut ota_path = PathBuf::from(self.config.ota.path.clone());
        ota_path.push(&update.version);
        create_dir_all(&ota_path)?;

        // Create file to actually download into
        let mut file_path = ota_path.to_owned();
        let file_name = url.split("/").last().ok_or(Error::FileNameMissing(url.to_owned()))?;
        file_path.push(file_name);
        let file_path = file_path.as_path();
        let file = File::create(file_path)?;
        let ota_path = file_path.to_str().ok_or(Error::FilePathMissing)?.to_owned();

        // Create handler to perform download from URL
        let resp = self.client.get(&url).send().await?;
        info!("Dowloading from {} into {}", url, ota_path);
        self.download(resp, file).await?;

        // Update Action payload with `ota_path`, i.e. downloaded file's location in fs
        update.ota_path = Some(ota_path);
        let payload = serde_json::to_string(&update)?;
        let action = Action { action_id, kind, name, payload };

        // Forward Action packet through bridge
        self.bridge_tx.try_send(action)?;

        Ok(())
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
                    Some(content_length) => (100 * downloaded / content_length) % 101,
                    None => {
                        downloaded = (downloaded + 1) % 100;
                        downloaded
                    }
                };
                let status =
                    ActionResponse::progress(&self.action_id, "Downloading", percentage as u8)
                        .set_sequence(self.sequence());
                self.send_status(status).await;
            }
        }

        info!("Firmware dowloaded sucessfully");

        Ok(())
    }

    async fn send_status(&mut self, status: ActionResponse) {
        debug!("Action status: {:?}", status);
        if let Err(e) = self.status_bucket.fill(status).await {
            error!("Failed to send downloader status. Error = {:?}", e);
        }
    }

    fn sequence(&mut self) -> u32 {
        self.sequence += 1;
        self.sequence
    }
}

/// Expected JSON format of data contained in the [`payload`] of an OTA [`Action`]
///
/// [`payload`]: Action#structfield.payload
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct FirmwareUpdate {
    url: String,
    version: String,
    /// Path to location in fs where download will be stored
    ota_path: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::Ota;
    use serde_json::json;

    const OTA_DIR: &str = "/tmp/uplink_test";

    #[test]
    // Test file downloading capabilities of OtaDownloader by downloading the uplink logo from GitHub
    fn download_file() {
        // Ensure path exists
        std::fs::create_dir_all(OTA_DIR).unwrap();
        // Prepare config
        let ota_path = format!("{}/ota", OTA_DIR);
        let config = Arc::new(Config {
            ota: Ota { enabled: true, path: ota_path.clone() },
            ..Default::default()
        });

        // Create channels to forward and push action_status on
        let (stx, srx) = flume::bounded(1);
        let (btx, brx) = flume::bounded(1);
        let action_status = Stream::dynamic_with_size("actions_status", "", "", 1, stx);
        let (ota_tx, downloader) = OtaDownloader::new(config, action_status, btx).unwrap();

        // Start OtaDownloader in separate thread
        std::thread::spawn(|| downloader.start().unwrap());

        // Create a firmware update action
        let ota_update = FirmwareUpdate {
            url: "https://github.com/bytebeamio/uplink/raw/main/docs/logo.png".to_string(),
            version: "1.0".to_string(),
            ota_path: None,
        };
        let mut expected_forward = ota_update.clone();
        expected_forward.ota_path = Some(ota_path + "/1.0/logo.png");
        let ota_action = Action {
            action_id: "1".to_string(),
            kind: "firmware_update".to_string(),
            name: "firmware_update".to_string(),
            payload: json!(ota_update).to_string(),
        };

        // Send action to OtaDownloader with OneTx<Action>
        ota_tx.send(ota_action.clone()).unwrap();

        // Collect action_status and forwarded Action
        let status: Vec<ActionResponse> =
            serde_json::from_slice(&srx.recv().unwrap().serialize()).unwrap();
        let forward: FirmwareUpdate = serde_json::from_str(&brx.recv().unwrap().payload).unwrap();
        // Ensure received action_status and forwarded action contains expected info
        assert_eq!(status[0].state, "Downloading");
        assert_eq!(forward, expected_forward);
    }
}
