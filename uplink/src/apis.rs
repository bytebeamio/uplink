use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Router};
use log::info;

use std::sync::Arc;

use crate::ReloadHandle;

struct ApiState {
    handle: ReloadHandle,
}

pub async fn start(port: u16, handle: ReloadHandle) {
    let address = format!("0.0.0.0:{port}");
    info!("Starting tracing server: {address}");

    let state = ApiState { handle };
    let app = Router::new().route("/", post(reload_loglevel)).with_state(Arc::new(state));

    axum::Server::bind(&address.parse().unwrap()).serve(app.into_make_service()).await.unwrap();
}

fn reload_loglevel(State(state): State<Arc<ApiState>>, filter: String) -> impl IntoResponse {
    info!("Reloading tracing filter: {filter}");
    if state.handle.reload(&filter).is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}
