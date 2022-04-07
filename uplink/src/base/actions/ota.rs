use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use log::{debug, error, info};
use reqwest::{Certificate, Client, ClientBuilder, Identity, Response};
use serde::{Deserialize, Serialize};

use std::fs::{create_dir_all, File};
use std::{io::Write, path::PathBuf, sync::Arc};

use super::{Action, ActionResponse};
use crate::base::{Config, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error from reqwest {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("File io Error {0}")]
    IoError(#[from] std::io::Error),
    #[error("Error forwarding to Bridge {0}")]
    TrySendError(#[from] flume::TrySendError<Action>),
    #[error("Error receiving action {0}")]
    Recv(#[from] flume::RecvError),
    // #[error("Download failed, content length none")]
    // NoContentLen,
    // #[error("Download failed, content length zero")]
    // ContentLenZero,
}

pub struct OtaDownloader {
    config: Arc<Config>,
    ota_rx: Receiver<Action>,
    action_id: String,
    status_bucket: Stream<ActionResponse>,
    bridge_tx: Sender<Action>,
    client: Client,
    sequence: u32,
}

impl OtaDownloader {
    pub fn new(
        config: Arc<Config>,
        ota_rx: Receiver<Action>,
        status_bucket: Stream<ActionResponse>,
        bridge_tx: Sender<Action>,
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
            config,
            client,
            ota_rx,
            status_bucket,
            bridge_tx,
            sequence: 0,
            action_id: String::default(),
        })
    }

    /// Spawn a thread to handle downloading OTA updates as per "update_firmware" actions and for
    /// forwarding updated actions to bridge for further processing, i.e. update installation.
    #[tokio::main(flavor = "current_thread")]
    pub async fn start(mut self) -> Result<(), Error> {
        loop {
            info!("Dowloading firmware");
            self.sequence = 0;
            let Action { action_id, kind, name, payload } = self.ota_rx.recv_async().await?;
            self.action_id = action_id.clone();
            // Extract url and add ota_path in payload before recreating action to be sent to bridge
            let mut update = serde_json::from_str::<FirmwareUpdate>(&payload)?;
            let url = update.url.clone();
            let ota_path = self.config.ota.path.clone();
            update.ota_path = Some(ota_path.clone());
            let payload = serde_json::to_string(&update)?;
            let action = Action { action_id, kind, name, payload };
            let ota_path = PathBuf::from(ota_path);

            info!("Dowloading from {}", url);
            match self.run(&ota_path, action, url).await {
                Ok(_) => {
                    let status =
                        ActionResponse::success(&self.action_id).set_sequence(self.sequence());
                    self.send_status(status).await;
                }
                Err(e) => {
                    let status = ActionResponse::failure(&self.action_id, e.to_string())
                        .set_sequence(self.sequence());
                    self.send_status(status).await;
                }
            }
        }
    }

    async fn run(&mut self, ota_path: &PathBuf, action: Action, url: String) -> Result<(), Error> {
        // Update action status for process initiated
        let status = ActionResponse::progress(&self.action_id, "Downloading", 0)
            .set_sequence(self.sequence());
        self.send_status(status).await;

        // Create file to download files into
        let file = self.create_file(ota_path).await?;

        // Create handler to perform download from URL
        let resp = self.client.get(url).send().await?;

        self.download(resp, file).await?;

        // Forward Action packet through bridge
        self.bridge_tx.try_send(action)?;

        Ok(())
    }

    /// Ensure that directory for downloading file into, exists
    async fn create_file(&mut self, ota_path: &PathBuf) -> Result<File, Error> {
        let dir = PathBuf::from("./");
        let ota_dir = ota_path.parent().unwrap_or(dir.as_path());
        create_dir_all(ota_dir)?;
        let file = File::create(ota_path)?;

        Ok(file)
    }

    /// Downloads from server and stores into file
    async fn download(&mut self, resp: Response, mut file: File) -> Result<(), Error> {
        // NOTE: Commented code for download's content length and completion estimation
        // Supposing content length is defined in bytes
        // let content_length = match resp.content_length() {
        //     None => return Err(Error::NoContentLen),
        //     Some(0) => return Err(Error::ContentLenZero),
        //     Some(l) => l as usize,
        // };
        // let mut downloaded = 0;
        let mut percentage = 0;
        let mut stream = resp.bytes_stream();

        // Download and store to disk by streaming as chunks
        while let Some(item) = stream.next().await {
            let chunk = item?;
            // downloaded += chunk.len();
            file.write_all(&chunk)?;

            // NOTE: ensure lesser frequency of action responses
            if file.metadata()?.len() % 1024 * 1024 == 0 {
                // let percentage = (100 * downloaded / content_length) as u8;
                percentage += 1;
                let status = ActionResponse::progress(&self.action_id, "Downloading", percentage)
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

#[derive(Serialize, Deserialize)]
struct FirmwareUpdate {
    url: String,
    version: String,
    /// Path to location in fs where download will be stored
    ota_path: Option<String>,
}
