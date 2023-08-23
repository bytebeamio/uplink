use crate::base::bridge::{BridgeTx, Payload};
use crate::base::{clock, SimulatorConfig};
use crate::Action;
use data::{Bms, DeviceData, DeviceShadow, Gps, Imu, Motor, PeripheralState};
use flume::{bounded, Sender};
use log::{error, info};
use rand::Rng;
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use tokio::time::interval;
use tokio::{select, spawn};

use std::time::Duration;
use std::{fs, io, sync::Arc};

mod data;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Recv error {0}")]
    Recv(#[from] flume::RecvError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

#[derive(Serialize)]
pub struct ActionResponse {
    action_id: String,
    state: String,
    progress: u8,
    errors: Vec<String>,
}

impl ActionResponse {
    pub async fn simulate(action: Action, tx: Sender<Payload>) {
        let action_id = action.action_id;
        info!("Generating action events for action: {action_id}");
        let mut sequence = 0;
        let mut interval = interval(Duration::from_secs(1));

        // Action response, 10% completion per second
        for i in 1..10 {
            let response = ActionResponse {
                action_id: action_id.clone(),
                progress: i * 10 + rand::thread_rng().gen_range(0..10),
                state: String::from("in_progress"),
                errors: vec![],
            };
            sequence += 1;
            if let Err(e) = tx
                .send_async(Payload {
                    stream: "action_status".to_string(),
                    sequence,
                    payload: json!(response),
                    timestamp: clock() as u64,
                })
                .await
            {
                error!("{e}");
                break;
            }

            interval.tick().await;
        }

        let response = ActionResponse {
            action_id,
            progress: 100,
            state: String::from("Completed"),
            errors: vec![],
        };
        sequence += 1;
        if let Err(e) = tx
            .send_async(Payload {
                stream: "action_status".to_string(),
                sequence,
                payload: json!(response),
                timestamp: clock() as u64,
            })
            .await
        {
            error!("{e}");
        }
        info!("Successfully sent all action responses");
    }
}

pub fn read_gps_path(paths_dir: &str) -> Arc<Vec<Gps>> {
    let i = rand::thread_rng().gen_range(0..10);
    let file_name: String = format!("{}/path{}.json", paths_dir, i);

    let contents = fs::read_to_string(file_name).expect("Oops, failed ot read path");

    let parsed: Vec<Gps> = serde_json::from_str(&contents).unwrap();

    Arc::new(parsed)
}

pub fn new_device_data(path: Arc<Vec<Gps>>) -> DeviceData {
    let mut rng = rand::thread_rng();

    let path_index = rng.gen_range(0..path.len()) as u32;

    DeviceData { path, path_offset: path_index }
}

pub fn spawn_data_simulators(device: DeviceData, tx: Sender<Payload>) {
    spawn(Gps::simulate(tx.clone(), device));
    spawn(Bms::simulate(tx.clone()));
    spawn(Imu::simulate(tx.clone()));
    spawn(Motor::simulate(tx.clone()));
    spawn(PeripheralState::simulate(tx.clone()));
    spawn(DeviceShadow::simulate(tx.clone()));
}

#[tokio::main(flavor = "current_thread")]
pub async fn start(config: SimulatorConfig, bridge_tx: BridgeTx) -> Result<(), Error> {
    let path = read_gps_path(&config.gps_paths);
    let device = new_device_data(path);

    let actions_rx = bridge_tx.register_action_routes(&config.actions).await;

    let (tx, rx) = bounded(10);
    spawn_data_simulators(device, tx.clone());

    loop {
        select! {
            action = actions_rx.as_ref().unwrap().recv_async(), if actions_rx.is_some() => {
                let action = action?;
                spawn(ActionResponse::simulate(action,  tx.clone()));
            }
            p = rx.recv_async() => {
                let payload = match p {
                    Ok(p) => p,
                    Err(_) => {
                        error!("All generators have stopped!");
                        return Ok(())
                    }
                };

                bridge_tx.send_payload(payload).await;
            }
        }
    }
}
