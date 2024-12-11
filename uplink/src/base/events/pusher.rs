use std::future::Future;
use std::pin::Pin;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use flume::{Receiver, SendError, Sender};
use rumqttc::{AsyncClient, Publish, QoS, Request};
use rusqlite::{Connection, Row};
use crate::base::bridge::Payload;
use crate::utils::SendOnce;

pub struct EventsPusher {
    pubacks: Receiver<u16>,
    publisher: AsyncClient,
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
    SendingEvent(Pin<Box<dyn Future<Output=Result<(), SendError<Request>>> + Send>>),
    /// Wait for the puback for some time
    ///   If the correct puback arrives, pop message and go to first step
    ///   If the step times out, go to S1
    WaitingForAck,
}

impl EventsPusher {
    pub fn new(pubacks: Receiver<u16>, publisher: AsyncClient, reserved_pkids: [u16; 2], db_path: PathBuf) -> Self {
        Self { pubacks, publisher, db_path, reserved_pkids }
    }

    pub async fn start(self) {
        use EventsPusherState::*;

        // let _span = tracing::trace_span!("event_pusher_thread").entered();

        let mut conn = match Connection::open(&self.db_path) {
            Ok(c) => Mutex::new(c),
            Err(e) => {
                log::error!("couldn't connect to events database: {e}");
                return;
            }
        };
        match conn.lock().unwrap().query_row(FETCH_EVENTS_COUNT, (), |row| row.get::<_, u64>(0)) {
            Ok(count) => {
                log::info!("found {count} events saved in storage");
            }
            Err(e) => {
                log::error!("sqlite error : {e}");
                return;
            }
        }

        let mut current_pkid_idx = 0;
        let mut state = Init;
        loop {
            match &mut state {
                Init => {
                    match conn.lock().unwrap().query_row(FETCH_ONE_EVENT, (), EventOrm::create) {
                        Ok(event) => {
                            log::info!("found an event, writing to network");
                            state = SendingEvent(
                                Box::pin(SendOnce(self.publisher.request_tx.clone()).send_async(
                                    self.generate_publish(event, self.reserved_pkids[current_pkid_idx]),
                                )),
                            );
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
                SendingEvent(fut) => {
                    match fut.await {
                        Ok(_) => {
                            log::info!("event written to network, waiting for acknowledgement");
                            self.pubacks.drain();
                            state = WaitingForAck;
                        }
                        Err(e) => {
                            log::error!("Rumqtt send error, aborting : {e}");
                            return;
                        }
                    }
                }
                WaitingForAck => {
                    let end = tokio::time::Instant::now() + Duration::from_secs(60);
                    loop {
                        match tokio::time::timeout_at(end, self.pubacks.recv_async()).await {
                            Ok(Ok(pkid)) => {
                                if pkid == self.reserved_pkids[current_pkid_idx] {
                                    log::info!("received acknowledgement");
                                    if let Err(e) = conn.lock().unwrap().execute(POP_EVENT, ()) {
                                        log::error!("unexpected error: couldn't pop event from queue: {e}");
                                        return;
                                    }
                                    current_pkid_idx += 1;
                                    current_pkid_idx %= 2;
                                    state = Init;
                                }
                            }
                            Ok(Err(e)) => {
                                log::warn!("PubAcks stream ended, aborting : {e}");
                                return;
                            }
                            Err(_) => {
                                log::error!("Timed out waiting for PubAck, retrying.");
                                state = Init;
                            }
                        }
                    }
                }
            }
        }
    }

    fn generate_publish(&self, event: EventOrm, pkid: u16) -> Request {
        let payload = serde_json::from_str::<Payload>(event.payload.as_str()).unwrap();
        let mut result = Publish::new(
            format!("/tenants/{}/devices/{}/events/{}/jsonarray", "", "", payload.stream),
            QoS::AtLeastOnce,
            serde_json::to_string(&[payload]).unwrap(),
        );
        result.pkid = pkid;
        result.retain = false;
        Request::Publish(result)
    }
}

// language=sqlite
const POP_EVENT: &str = "
DELETE FROM events
WHERE id = (SELECT id FROM events ORDER BY id LIMIT 1)";

// language=sqlite
const CREATE_EVENTS_TABLE: &str = "CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)";

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