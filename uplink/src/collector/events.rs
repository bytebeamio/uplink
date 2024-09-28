use std::{fs::metadata, sync::Arc, time::Duration};

use axum::{extract::State, routing::post, Json, Router};
use log::{debug, error, info, warn};
use reqwest::StatusCode;
use rumqttc::{AsyncClient, Event, Outgoing, Packet, PubAck, QoS};
use sqlx::{migrate::MigrateDatabase, Connection, Sqlite, SqliteConnection};
use tokio::{spawn, sync::Mutex, time::sleep};

use crate::{
    base::{bridge::Payload, mqtt::mqttoptions},
    config::{Config, DeviceConfig},
};

type StreamName = String;
type RawPayload = String;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Sql {0}")]
    Sql(#[from] sqlx::Error),
    #[error("Serde {0}")]
    Serde(#[from] serde_json::Error),
}

struct Queue {
    conn: SqliteConnection,
}

impl Queue {
    /// Construct an sqlite database and configure the payloads table
    pub async fn new(path: &str) -> Result<Self, Error> {
        // NOTE: create db if file doesn't exist
        if metadata(path).is_err() {
            Sqlite::create_database(path).await?;
        }

        let mut conn = SqliteConnection::connect(path).await?;

        sqlx::query!(
            r#"CREATE TABLE IF NOT EXISTS payloads
        (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            stream  TEXT NOT NULL,
            raw     TEXT NOT NULL
        );"#,
        )
        .execute(&mut conn)
        .await?;

        Ok(Self { conn })
    }

    /// Write into payloads table
    pub async fn push(&mut self, payload: &Payload) -> Result<(), Error> {
        let raw = serde_json::to_string(payload)?;
        sqlx::query!(
            r#"INSERT INTO payloads ( stream, raw )
            VALUES ( ?1, ?2 );"#,
            payload.stream,
            raw,
        )
        .execute(&mut self.conn)
        .await?;

        Ok(())
    }

    /// Read out one from the payloads table
    pub async fn peek(&mut self) -> Result<(StreamName, RawPayload), Error> {
        let row = sqlx::query!("SELECT stream, raw FROM payloads ORDER BY id ASC LIMIT 1")
            .fetch_one(&mut self.conn)
            .await?;

        let stream = row.stream;
        let raw = row.raw;

        Ok((stream, raw))
    }

    /// Forget messages acked by the broker
    pub async fn pop(&mut self) -> Result<(), Error> {
        sqlx::query!("DELETE FROM payloads WHERE id = (SELECT MIN(id) FROM payloads);")
            .execute(&mut self.conn)
            .await?;

        Ok(())
    }
}

#[tokio::main]
pub async fn start(port: u16, path: &str, config: Arc<Config>, device_config: Arc<DeviceConfig>) {
    let address = format!("0.0.0.0:{port}");
    info!("Starting uplink event server: {address}");

    let queue = match Queue::new(path).await {
        Ok(q) => q,
        Err(e) => {
            error!("{e}");
            return;
        }
    };
    let state = Arc::new(Mutex::new(queue));

    spawn(push_to_broker_on_ack(config, device_config, state.clone()));

    let app = Router::new().route("/event", post(event)).with_state(state);

    axum::Server::bind(&address.parse().unwrap()).serve(app.into_make_service()).await.unwrap();
}

async fn event(State(queue): State<Arc<Mutex<Queue>>>, Json(payload): Json<Payload>) -> StatusCode {
    info!("Event received on stream: {}", payload.stream);

    let mut queue = queue.lock().await;
    if let Err(e) = queue.push(&payload).await {
        error!("{e}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}

async fn push_to_broker_on_ack(
    config: Arc<Config>,
    device_config: Arc<DeviceConfig>,
    queue: Arc<Mutex<Queue>>,
) {
    // create a new eventloop and use it only for event data push
    let options = mqttoptions(&config, &device_config);
    let (client, mut eventloop) = AsyncClient::new(options, 1);
    eventloop.network_options.set_connection_timeout(config.mqtt.network_timeout);

    'outer: loop {
        let mut guard = queue.lock().await;
        let (stream, text) = match guard.peek().await {
            Ok(q) => q,
            Err(Error::Sql(sqlx::Error::RowNotFound)) => {
                debug!("Looks like event queue is handled for the time being, check again in 5s");
                // Wait 5 seconds before asking for next
                sleep(Duration::from_secs(5)).await;
                continue 'outer;
            }
            Err(e) => {
                error!("{e}");
                return;
            }
        };
        drop(guard);

        let topic = format!(
            "/tenants/{}/devices/{}/events/{stream}/jsonarry",
            device_config.project_id, device_config.device_id
        );

        if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, false, format!("[{text}]")).await {
            error!("{e}");
        }

        let mut expected_pkid = None;
        loop {
            match eventloop.poll().await {
                Ok(Event::Outgoing(Outgoing::Publish(pkid))) => expected_pkid = Some(pkid),
                Ok(Event::Incoming(Packet::PubAck(PubAck { pkid }))) => {
                    if expected_pkid.map_or(false, |p| p == pkid) {
                        expected_pkid.take();
                        let mut guard = queue.lock().await;
                        if let Err(e) = guard.pop().await {
                            error!("{e}");
                            return;
                        }

                        debug!("Event has reached broker on topic: {topic}");
                        continue 'outer;
                    } else {
                        warn!("Unexpected pkid");
                    }
                }
                Err(e) => {
                    error!("error={e}; trying again in 5s");
                    // Wait a second before trying again
                    sleep(Duration::from_secs(1)).await;
                }
                _ => {}
            }
        }
    }
}
