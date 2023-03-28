use axum::{http::Request, routing::post, Router};
use log::info;
use reqwest::StatusCode;

use std::sync::Arc;

use crate::ReloadHandle;

pub fn start(port: u16, handle: ReloadHandle) {
    let address = format!("0.0.0.0:{port}");
    info!("Starting tracing server: {address}");

    let handle = Arc::new(handle);

    Router::<(), String>::new().route(
        "/logs",
        post({
            let handle = Arc::clone(&handle);
            move |body| reload_loglevel(handle, body)
        }),
    );
}

async fn reload_loglevel(handle: Arc<ReloadHandle>, request: Request<String>) -> StatusCode {
    if handle.reload(request.body()).is_err() {
        return StatusCode::OK;
    }

    StatusCode::INTERNAL_SERVER_ERROR
}
