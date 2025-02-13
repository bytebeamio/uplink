use std::future::Future;
use std::pin::Pin;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use flume::{Receiver, SendError};
use rumqttc::{AsyncClient, Publish, QoS, Request};
use rusqlite::{Connection, Row};
use crate::base::bridge::Payload;
use crate::utils::SendOnce;

pub struct EventsPusher {
    pubacks: Receiver<u16>,
    publisher: AsyncClient,
    tenant_id: String,
    device_id: String,
    reserved_pkids: [u16; 2],
    db_path: PathBuf,
}

enum EventsPusherState {
    /// If queue has messages
    ///   initiate push to rumqtt and go to next step
    ///   otherwise wait for some time
    Init,
    /// Wait on rumqtt push
    ///   If it succeeds, go to next step
    ///   If it fails with rumqtt error, log error and stop
    SendingEvent(i64, Pin<Box<dyn Future<Output=Result<(), SendError<Request>>> + Send>>),
    /// Wait for the puback for some time
    ///   If the correct puback arrives, pop message and go to first step
    ///   If the step times out, go to first step
    WaitingForAck(i64),
}

impl EventsPusher {
    pub fn new(pubacks: Receiver<u16>, publisher: AsyncClient, tenant_id: String, device_id: String, reserved_pkids: [u16; 2], db_path: PathBuf) -> Self {
        Self { pubacks, publisher, tenant_id, device_id, db_path, reserved_pkids }
    }

    pub async fn start(self) {
        use EventsPusherState::*;

        // let _span = tracing::trace_span!("event_pusher_thread").entered();

        let conn = match Connection::open(&self.db_path) {
            Ok(c) => Mutex::new(c),
            Err(e) => {
                log::error!("couldn't connect to events database: {e}");
                return;
            }
        };

        if let Err(e) = do_initialization(&conn) {
            log::error!("sqlite error : {e}");
            return;
        }

        let mut current_pkid_idx = 0;
        let mut state = Init;
        loop {
            match &mut state {
                Init => {
                    let result = conn.lock().unwrap().query_row(FETCH_ONE_EVENT, (), EventOrm::create);
                    match result {
                        Ok(event_o) => {
                            match self.generate_publish(&event_o, self.reserved_pkids[current_pkid_idx]) {
                                Ok(event) => {
                                    log::info!("found an event, writing to network");
                                    state = SendingEvent(
                                        event_o.id,
                                        Box::pin(SendOnce(self.publisher.request_tx.clone()).send_async(event)),
                                    );
                                }
                                Err(e) => {
                                    log::error!("invalid event in database : {event_o:?} : {e}");
                                    if let Err(e) = conn.lock().unwrap().execute(POP_EVENT, (event_o.id,)) {
                                        log::error!("unexpected error: couldn't pop event from queue: {e}");
                                        return;
                                    }
                                }
                            }
                        }
                        Err(rusqlite::Error::QueryReturnedNoRows) => {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(e) => {
                            log::error!("unexpected error when fetching event: {e}");
                            tokio::time::sleep(Duration::from_secs(60)).await;
                        }
                    }
                }
                SendingEvent(event_id, fut) => {
                    match fut.await {
                        Ok(_) => {
                            log::info!("event written to network, waiting for acknowledgement");
                            self.pubacks.drain();
                            state = WaitingForAck(*event_id);
                        }
                        Err(e) => {
                            log::error!("Rumqtt send error, aborting : {e}");
                            return;
                        }
                    }
                }
                WaitingForAck(event_id) => {
                    let end = tokio::time::Instant::now() + Duration::from_secs(60);
                    loop {
                        match tokio::time::timeout_at(end, self.pubacks.recv_async()).await {
                            Ok(Ok(pkid)) => {
                                if pkid == self.reserved_pkids[current_pkid_idx] {
                                    log::info!("received acknowledgement");
                                    if let Err(e) = conn.lock().unwrap().execute(POP_EVENT, (*event_id,)) {
                                        log::error!("unexpected error: couldn't pop event from queue: {e}");
                                        return;
                                    }
                                    current_pkid_idx += 1;
                                    current_pkid_idx %= 2;
                                    state = Init;
                                    break;
                                }
                            }
                            Ok(Err(e)) => {
                                log::warn!("PubAcks stream ended, aborting : {e}");
                                return;
                            }
                            Err(_) => {
                                log::error!("Timed out waiting for PubAck, retrying.");
                                state = Init;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    fn generate_publish(&self, event: &EventOrm, pkid: u16) -> anyhow::Result<Request> {
        let payload = serde_json::from_str::<Payload>(event.payload.as_str())?;
        let mut result = Publish::new(
            format!("/tenants/{}/devices/{}/events/{}/jsonarray", self.tenant_id, self.device_id, payload.stream),
            QoS::AtLeastOnce,
            serde_json::to_string(&[payload]).unwrap(),
        );
        result.pkid = pkid;
        result.retain = false;
        Ok(Request::Publish(result))
    }
}

fn do_initialization(conn: &Mutex<Connection>) -> anyhow::Result<()> {
    conn.lock().unwrap()
        .execute_batch(CREATE_EVENTS_TABLE)?;

    let count = conn.lock().unwrap().query_row(FETCH_EVENTS_COUNT, (), |row| row.get::<_, u64>(0))?;
    if count != 0 {
        log::info!("found {count} events saved in storage");
    }
    Ok(())
}

// language=sqlite
const POP_EVENT: &str = "DELETE FROM events WHERE id = ?";

// language=sqlite
pub const CREATE_EVENTS_TABLE: &str = "CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT); VACUUM;";

#[derive(Debug)]
struct EventOrm {
    id: i64,
    payload: String,
}

// language=sqlite
const FETCH_ONE_EVENT: &str = "SELECT id, payload FROM events ORDER BY id LIMIT 1";

// language=sqlite
const FETCH_EVENTS_COUNT: &str = "SELECT COUNT(*) FROM events";

impl EventOrm {
    pub fn create(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            payload: row.get(1)?,
        })
    }
}
