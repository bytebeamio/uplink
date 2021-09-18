use async_channel::Sender;
use bytes::BytesMut;
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
}

struct OtaDownloader {
    status_bucket: Stream<ActionResponse>,
    action_id: String,
    ota_path: PathBuf,
    bridge_tx: Sender<Action>,
    client: Client,
}

impl OtaDownloader {
    async fn run(&mut self, action: Action, url: String) {
        // Create file to download files into
        let mut file = match self.create_file().await {
            Ok(file) => file,
            Err(e) => {
                self.send_status(ActionResponse::failure(
                    &self.action_id,
                    format!("Failed to create file: {}", e),
                ))
                .await;
                return;
            }
        };

        // Create handler to perform download from URL
        let resp = match self.client.get(url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                self.send_status(ActionResponse::failure(
                    &self.action_id,
                    format!("Couldn't download OTA update: {}", e),
                ))
                .await;
                return;
            }
        };

        if let Err(e) = self.download(resp, &mut file).await {
            self.send_status(ActionResponse::failure(
                &self.action_id,
                format!("Error while downloading: {}", e),
            ))
            .await;
            return;
        }

        // Forward Action packet through bridge
        match self.bridge_tx.try_send(action) {
            Ok(()) => {
                self.send_status(ActionResponse::success(&self.action_id)).await;
            }
            Err(e) => {
                self.send_status(ActionResponse::failure(
                    &self.action_id,
                    format!("Failed forwarding to bridge | Error: {}", e),
                ))
                .await;
            }
        }
    }

    /// Ensure that directory for downloading file into, exists
    async fn create_file(&mut self) -> Result<File, Error> {
        let curr_dir = PathBuf::from("./");
        let ota_dir = self.ota_path.parent().unwrap_or(curr_dir.as_path());
        create_dir_all(ota_dir)?;
        let file = File::create(self.ota_path.clone())?;

        Ok(file)
    }

    /// Downloads from server and stores into file
    async fn download(&mut self, resp: Response, file: &mut File) -> Result<(), Error> {
        // Supposing content length is defined in bytes
        let content_length = resp.content_length().unwrap_or(0) as usize;
        let mut downloaded = 0;
        let mut stream = resp.bytes_stream();

        // Download and store to disk by streaming as chunks
        while let Some(item) = stream.next().await {
            let chunk = item?;
            downloaded += chunk.len();
            file.write(&chunk)?;

            self.send_status(ActionResponse::progress(
                &self.action_id,
                (downloaded / content_length) as u8 * 100,
            ))
            .await;
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
}

#[derive(Serialize, Deserialize)]
struct FirmwareUpdate {
    url: String,
    version: String,
    /// Path to location in fs where download will be stored
    ota_path: Option<String>,
}

/// Download contents of the OTA update if action is named "update_firmware"
pub async fn firmware_downloader(
    status_bucket: Stream<ActionResponse>,
    action: Action,
    config: Arc<Config>,
    bridge_tx: Sender<Action>,
) -> Result<(), Error> {
    info!("Dowloading firmware");
    let Action { id, kind, name, payload } = action;
    // Extract url and add ota_path in payload before recreating action to be sent to bridge
    let mut update = serde_json::from_str::<FirmwareUpdate>(&payload)?;
    let url = update.url.clone();
    let ota_path = config.ota.path.clone();
    update.ota_path = Some(ota_path.clone());
    let payload = serde_json::to_string(&update)?;
    let action_id = id.clone();
    let action = Action { id, kind, name, payload };
    let ota_path = PathBuf::from(ota_path);

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
    let mut downloader = OtaDownloader { status_bucket, action_id, ota_path, bridge_tx, client };

    info!("Dowloading from {}", url);
    tokio::task::spawn(async move { downloader.run(action, url).await });

    Ok(())
}
