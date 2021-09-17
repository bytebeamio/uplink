use async_channel::Sender;
use bytes::BytesMut;
use futures_util::StreamExt;
use log::{debug, error, info};
use reqwest::{Certificate, ClientBuilder, Identity};
use serde::{Deserialize, Serialize};

use std::fs::{create_dir_all, File};
use std::{io::Write, path::PathBuf, sync::Arc};

use super::{Action, ActionResponse};
use crate::base::{Config, Stream};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Error from reqwest")]
    ReqwestError(#[from] reqwest::Error),
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
    mut status_bucket: Stream<ActionResponse>,
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

    info!("Dowloading from {}", url);
    tokio::task::spawn(async move {
        // Ensure that directory for downloading updates into, exists
        let ota_dir = ota_path.parent().unwrap();
        if let Err(e) = create_dir_all(ota_dir) {
            send_status(
                &mut status_bucket,
                ActionResponse::failure(&action_id, format!("Failed to create file: {}", e)),
            )
            .await;
            return;
        }

        // Create file to download files into
        let mut file = match File::create(ota_path) {
            Ok(file) => file,
            Err(e) => {
                send_status(
                    &mut status_bucket,
                    ActionResponse::failure(&action_id, format!("Failed to create file: {}", e)),
                )
                .await;
                return;
            }
        };

        // Create handler to perform download from URL
        let resp = match client.get(url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                send_status(
                    &mut status_bucket,
                    ActionResponse::failure(
                        &action_id,
                        format!("Couldn't download OTA update: {}", e),
                    ),
                )
                .await;
                return;
            }
        };

        // Supposing content length is defined in bytes
        let content_length = resp.content_length().unwrap_or(0) as usize;
        let mut downloaded = 0;
        let mut stream = resp.bytes_stream();

        // Download and store to disk by streaming as chunks
        while let Some(item) = stream.next().await {
            let chunk = match item {
                Ok(chunk) => chunk,
                Err(e) => {
                    send_status(
                        &mut status_bucket,
                        ActionResponse::failure(
                            &action_id,
                            format!("Error while downloading: {}", e),
                        ),
                    )
                    .await;
                    return;
                }
            };
            downloaded += chunk.len();

            if let Err(e) = file.write(&chunk) {
                send_status(
                    &mut status_bucket,
                    ActionResponse::failure(
                        &action_id,
                        format!("Error while writing to file: {}", e),
                    ),
                )
                .await;
                return;
            }

            send_status(
                &mut status_bucket,
                ActionResponse::progress(&action_id, (downloaded / content_length) as u8 * 100),
            )
            .await;
        }

        info!("Firmware dowloaded sucessfully");

        // Forward Action packet through bridge
        match bridge_tx.try_send(action) {
            Ok(()) => {
                send_status(&mut status_bucket, ActionResponse::success(&action_id)).await;
            }
            Err(e) => {
                send_status(
                    &mut status_bucket,
                    ActionResponse::failure(
                        &action_id,
                        format!("Failed forwarding to bridge | Error: {}", e),
                    ),
                )
                .await;
            }
        }
    });

    Ok(())
}

async fn send_status(status_bucket: &mut Stream<ActionResponse>, status: ActionResponse) {
    debug!("Action status: {:?}", status);
    if let Err(e) = status_bucket.fill(status).await {
        error!("Failed to send downloader status. Error = {:?}", e);
    }
}
