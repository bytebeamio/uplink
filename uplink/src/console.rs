use std::sync::atomic::Ordering;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{post, put},
    Router,
};
use log::info;
use uplink::{base::CtrlTx, collector::downloader::DOWNLOADER_DISABLED};

use crate::ReloadHandle;

#[derive(Debug, Clone)]
struct StateHandle {
    reload_handle: ReloadHandle,
    ctrl_tx: CtrlTx,
}

#[tokio::main]
pub async fn start(port: u16, reload_handle: ReloadHandle, ctrl_tx: CtrlTx) {
    let address = format!("0.0.0.0:{port}");
    info!("Starting uplink console server: {address}");
    let state = StateHandle { reload_handle, ctrl_tx };
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
async fn disable_downloader() -> impl IntoResponse {
    info!("Downloader stopped");
    // Shouldn't panic as always sets to true
    if DOWNLOADER_DISABLED
        .fetch_update(Ordering::Release, Ordering::Acquire, |_| Some(true))
        .unwrap()
    {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    }
}

// Start downloader back up even if it was already not stopped
async fn enable_downloader() -> impl IntoResponse {
    info!("Downloader started");
    // Shouldn't panic as always sets to true
    if DOWNLOADER_DISABLED
        .fetch_update(Ordering::Release, Ordering::Acquire, |_| Some(false))
        .unwrap()
    {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    }
}
