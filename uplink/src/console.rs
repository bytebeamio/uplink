use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{post, put},
    Router,
};
use log::info;
use uplink::base::CtrlTx;

use crate::ReloadHandle;

#[derive(Debug, Clone)]
struct StateHandle {
    reload_handle: ReloadHandle,
    ctrl_tx: CtrlTx,
    downloader_disable: Arc<Mutex<bool>>,
}

#[tokio::main]
pub async fn start(
    port: u16,
    reload_handle: ReloadHandle,
    ctrl_tx: CtrlTx,
    downloader_disable: Arc<Mutex<bool>>,
) {
    let address = format!("0.0.0.0:{port}");
    info!("Starting uplink console server: {address}");
    let state = StateHandle { reload_handle, ctrl_tx, downloader_disable };
    let app = Router::new()
        .route("/logs", post(reload_loglevel))
        .route("/shutdown", post(shutdown))
        .route("/disable_downloader", put(disable_downloader))
        .route("/enable_downloader", put(enable_downloader))
        .with_state(state);

    axum::Server::bind(&address.parse().unwrap()).serve(app.into_make_service()).await.unwrap();
}

async fn reload_loglevel(State(state): State<StateHandle>, filter: String) -> impl IntoResponse {
    info!("Reloading tracing filter: {filter}");
    if state.reload_handle.reload(&filter).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}

async fn shutdown(State(state): State<StateHandle>) -> impl IntoResponse {
    info!("Shutting down uplink");
    state.ctrl_tx.trigger_shutdown().await;

    StatusCode::OK
}

// Stops downloader from downloading even if it was already stopped
async fn disable_downloader(State(state): State<StateHandle>) -> impl IntoResponse {
    info!("Downloader stopped");
    let mut is_disabled = state.downloader_disable.lock().unwrap();
    if *is_disabled {
        StatusCode::ACCEPTED
    } else {
        *is_disabled = true;
        StatusCode::OK
    }
}

// Start downloader back up even if it was already not stopped
async fn enable_downloader(State(state): State<StateHandle>) -> impl IntoResponse {
    info!("Downloader started");
    let mut is_disabled = state.downloader_disable.lock().unwrap();
    if *is_disabled {
        *is_disabled = false;
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    }
}
