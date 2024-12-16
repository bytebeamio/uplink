use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{
    extract::State,
    http::{response::Builder, StatusCode},
    response::IntoResponse,
    routing::{get, post, put},
    Router,
};
use log::info;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uplink::base::CtrlTx;
use uplink::base::events::pusher::CREATE_EVENTS_TABLE;
use crate::ReloadHandle;

#[derive(Debug, Clone)]
struct StateHandle {
    reload_handle: ReloadHandle,
    ctrl_tx: CtrlTx,
    downloader_disable: Arc<Mutex<bool>>,
    network_up: Arc<Mutex<bool>>,
    events_db_conn: Option<Arc<Mutex<Connection>>>,
}

#[tokio::main]
pub async fn start(
    port: u16,
    reload_handle: ReloadHandle,
    ctrl_tx: CtrlTx,
    downloader_disable: Arc<Mutex<bool>>,
    network_up: Arc<Mutex<bool>>,
    events_db_path: Option<PathBuf>,
) {
    let address = format!("0.0.0.0:{port}");
    info!("Starting uplink console server: {address}");

    let events_db_conn = events_db_path
        .and_then(|path| {
            Connection::open(path)
                .map_err(|e| {
                    log::error!("couldn't connect to events database: {e}");
                    e
                })
                .and_then(|conn| {
                    match conn.execute_batch(CREATE_EVENTS_TABLE) {
                        Ok(_) => Ok(conn),
                        Err(e) => {
                            log::error!("couldn't create events table : {e}");
                            Err(e)
                        }
                    }
                })
                .map(|c| {
                    Arc::new(Mutex::new(c))
                })
                .ok()
        });

    let app = Router::new()
        .route("/logs", post(reload_loglevel))
        .route("/shutdown", post(shutdown))
        .route("/disable_downloader", put(disable_downloader))
        .route("/enable_downloader", put(enable_downloader))
        .route("/status", get(status))
        .route("/events", post(save_events));

    let state = StateHandle { reload_handle, ctrl_tx, downloader_disable, network_up, events_db_conn };
    let app = app.with_state(state);

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

// Pushes uplink status as JSON text
async fn status(State(state): State<StateHandle>) -> impl IntoResponse {
    Builder::new()
        .body(
            json!({
                "connected": *state.network_up.lock().unwrap(),
            })
            .to_string(),
        )
        .unwrap()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PayloadWithStream {
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: Value,
}

// language=sqlite
const INSERT_EVENT: &str = "INSERT INTO events(payload) VALUES (?)";
async fn save_events(State(state): State<StateHandle>, axum::Json(payload): axum::Json<PayloadWithStream>) -> (StatusCode, axum::Json<Value>) {
    match state.events_db_conn {
        None => {
            (StatusCode::BAD_REQUEST, axum::Json(json!({ "error": "events feature is disabled in uplink config" })))
        }
        Some(conn) => {
            let payload = serde_json::to_string(&payload).unwrap();
            match conn.lock().unwrap().execute(INSERT_EVENT, (payload,)) {
                Ok(_) => {
                    (StatusCode::OK, axum::Json(json!({ "result": "ok" })))
                }
                Err(e) => {
                    log::error!("couldn't save event to local storage: {e:?}");
                    (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(json!({ "error": "couldn't save event to local storage" })))
                }
            }
        }
    }
}