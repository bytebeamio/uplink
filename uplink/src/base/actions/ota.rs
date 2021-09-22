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
    #[error("Error forwarding to Bridge {0}")]
    TrySendError(#[from] async_channel::TrySendError<Action>),
}

struct OtaDownloader {
    action_id: String,
    status_bucket: Stream<ActionResponse>,
    bridge_tx: Sender<Action>,
}

impl OtaDownloader {
    async fn run(
        &mut self,
        ota_path: &PathBuf,
        client: Client,
        action: Action,
        url: String,
    ) -> Result<(), Error> {
        // Create file to download files into
        let file = self.create_file(ota_path).await?;

        // Create handler to perform download from URL
        let resp = client.get(url).send().await?;

        self.download(resp, file).await?;

        // Forward Action packet through bridge
        self.bridge_tx.try_send(action)?;

        Ok(())
    }

    /// Ensure that directory for downloading file into, exists
    async fn create_file(&mut self, ota_path: &PathBuf) -> Result<File, Error> {
        let curr_dir = PathBuf::from("./");
        let ota_dir = ota_path.parent().unwrap_or(curr_dir.as_path());
        create_dir_all(ota_dir)?;
        let file = File::create(ota_path)?;

        Ok(file)
    }

    /// Downloads from server and stores into file
    async fn download(&mut self, resp: Response, mut file: File) -> Result<(), Error> {
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

/// Spawn a task to download and forward "update_firmware" actions
pub async fn spawn_firmware_downloader(
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
    let mut downloader = OtaDownloader { status_bucket, action_id, bridge_tx };

    info!("Dowloading from {}", url);
    // TODO: Spawned task may fail to execute as expected and status may not be forwarded to cloud
    tokio::task::spawn(async move {
        match downloader.run(&ota_path, client, action, url).await {
            Ok(_) => downloader.send_status(ActionResponse::success(&downloader.action_id)).await,
            Err(e) => {
                downloader
                    .send_status(ActionResponse::failure(&downloader.action_id, e.to_string()))
                    .await
            }
        }
    });

    Ok(())
}
