use log::{error, info};
use reqwest::{Error, Response};
use serde::Deserialize;
use serde_json::json;
use std::cmp::{max, min};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use structopt::StructOpt;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use u3::config::{AuthConfig, HttpCreds, parse_auth_file, parse_config};
use u3::utils::{clock, num_cores};
use u3::{DataRow, PublishItem, Uplink};
use u3::core::actions::Action;
use u3::core::serializer::ConnectionManager;

fn main() {
    let args = Cli::from_args();
    let uplink_exe_path = std::env::current_exe().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(max(4, num_cores()))
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");

    runtime.block_on(async move {
        let (actions_tx, actions_rx) = flume::bounded::<Action>(8);
        let (data_tx, data_rx) = flume::bounded(128);
        initialize_logging(args.verbosity, args.log_filters_file_path);

        let cfg = match parse_config(&args.config) {
            Ok(r) => r,
            Err(e) => {
                error!("{e}");
                return;
            }
        };
        let mut auth = match parse_auth_file(&args.authentication) {
            Ok(r) => r,
            Err(e) => {
                error!("{e}");
                return;
            }
        };

        let connection_manager = Arc::new(ConnectionManager::new(auth.http_credentials.clone()));
        let mut uplink = Uplink::spawn(cfg.clone(), connection_manager.clone(), data_rx.clone());
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigint = Box::pin(tokio::signal::ctrl_c());
        loop {
            select! {
                _ = async {
                    select! {
                        _ = &mut sigint => {}
                        _ = sigterm.recv() => {}
                    }
                } => {
                    uplink.terminate().await;
                    break;
                }
                action = connection_manager.await_action("renew_cert") => {
                    let now = clock();
                    let new_credentials = match serde_json::from_value::<AuthConfig>(action.params) {
                        Ok(p) => p,
                        Err(_) => {
                            submit_action_response(&auth.http_credentials, action.action_id.clone(), "Failed", 100, vec!["invalid action payload".into()]).await;
                            continue;
                        }
                    };
                    info!("received new certificated from server. project_id = {}, device_id = {}", new_credentials.project_id, new_credentials.device_id);
                    // TODO: validate that we can connect using new credentials
                    if let Err(e) = OpenOptions::new()
                        .write(true)
                        .open(&args.authentication)
                        .and_then(|mut auth_file_handle| auth_file_handle.write_all(serde_json::to_string_pretty(&new_credentials).unwrap().as_bytes())) {
                        submit_action_response(&auth.http_credentials, action.action_id.clone(), "Failed", 100, vec![format!("cannot write auth file: {e:?}")]).await;
                        continue;
                    }
                    submit_action_response(&auth.http_credentials, action.action_id.clone(), "Completed", 100, vec![]).await;
                    auth = new_credentials;
                    info!("saved new certificates, reconnecting to the server...");
                    connection_manager.update_credentials(auth.http_credentials.clone());
                }
                action = connection_manager.await_action("update_uplink") => {
                    // check if we can update uplink exe file
                    // download uplink to same directory
                    // unlink the old file and rename the new file
                    // submit action response
                    break;
                }
            }
        }
    });
}

async fn submit_action_response(
    auth: &HttpCreds,
    action_id: String,
    status: &'static str,
    progress: u8,
    errors: Vec<String>,
) {
    let auth = auth.clone();
    let errors = errors.clone();
    // issues are unlikely to happen because we just received an action over the internet, but just in case
    if !try_submit_action_response(&auth, &action_id, status, progress, &errors).await {
        tokio::spawn(async move {
            for _ in 0..10 {
                if try_submit_action_response(&auth, &action_id, status, progress, &errors).await {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
}

async fn try_submit_action_response(
    auth: &HttpCreds,
    action_id: &str,
    status: &'static str,
    progress: u8,
    errors: &[String],
) -> bool {
    match reqwest::Client::new()
        .post(format!("{}/v1/streams/action_status/submit", &auth.api_url))
        .header("x-bytebeam-device-identity", &auth.api_key)
        .json(&json!([
            {"action_id": action_id, "state": status, "progress": progress, "errors": errors}
        ]))
        .send().await
    {
        Ok(resp) => {
            if !resp.status().is_success() {
                log::error!("action response upload failed. status = {}, body = {:?}", resp.status(), resp.text().await);
                false
            } else {
                true
            }
        }
        Err(e) => {
            log::error!("action response upload failed: {e:?}");
            false
        }
    }
}


async fn reqwest_error_for_status(resp: Response) -> Result<String, (reqwest::StatusCode, String)> {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.is_success() { Ok(body) } else { Err((status, body)) }
}

#[derive(Deserialize)]
struct ReprovisionParams {
    tenant: String,
    server: String,
    api_key: String,
}

#[derive(StructOpt)]
struct Cli {
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub verbosity: u8,
    #[structopt(long)]
    pub log_filters_file_path: Option<String>,
    #[structopt(short, long)]
    pub config: String,
    #[structopt(short, long)]
    pub authentication: String,
}

pub fn initialize_logging(verbosity: u8, file_path: Option<String>) {
    let filter_str = match verbosity {
        0 => "info,u3=warn",
        1 => "info,u3=info",
        2 => "info,u3=debug",
        _ => "info,u3=trace",
    };
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .compact()
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(filter_str)
        .with_filter_reloading();
    let reload_handle = builder.reload_handle();
    builder.try_init().unwrap();

    if let Some(file_path) = file_path {
        tokio::spawn(async move {
            if let Err(e) = std::fs::write(&file_path, filter_str) {
                println!("Couldn't write log filters file, log level cannot be reloaded: {e}");
                return;
            }

            let mut last_modified = SystemTime::now();
            loop {
                match std::fs::metadata(&file_path) {
                    Ok(metadata) => {
                        let modified = match metadata.modified() {
                            Ok(md) => md,
                            Err(e) => {
                                println!("couldn't read {file_path}: {e:?}");
                                continue;
                            }
                        };
                        if modified > last_modified {
                            last_modified = modified;
                            match std::fs::read_to_string(&file_path) {
                                Ok(new_filter_str) => {
                                    let new_filter =
                                        tracing_subscriber::EnvFilter::new(new_filter_str.trim());
                                    println!("reloading {file_path}");
                                    if let Err(e) = reload_handle.reload(new_filter) {
                                        println!("Failed to reload filter: {e:?}");
                                    }
                                }
                                Err(e) => {
                                    println!("Failed to read {file_path}: {e:?}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to get file metadata: {e:?}");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
