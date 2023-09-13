use crate::base::bridge::{BridgeTx, Payload};
use crate::base::SimulatorConfig;
use crate::{Action, ActionResponse};
use data::{Bms, DeviceData, DeviceShadow, Gps, Imu, Motor, PeripheralState};
use flume::{bounded, Receiver, Sender};
use log::{error, info};
use rand::Rng;
use thiserror::Error;
use tokio::time::interval;
use tokio::{select, spawn};

use std::time::Duration;
use std::{fs, io, sync::Arc};

mod data;

pub enum Event {
    Data(Payload),
    ActionResponse(ActionResponse),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] io::Error),
    #[error("Recv error {0}")]
    Recv(#[from] flume::RecvError),
    #[error("Serde error {0}")]
    Json(#[from] serde_json::error::Error),
}

impl ActionResponse {
    pub async fn simulate(action: Action, tx: Sender<Event>) {
        let action_id = action.action_id;
        info!("Generating action events for action: {action_id}");
        let mut sequence = 0;
        let mut interval = interval(Duration::from_secs(1));

        // Action response, 10% completion per second
        for i in 1..10 {
            let progress = i * 10 + rand::thread_rng().gen_range(0..10);
            sequence += 1;
            let response = ActionResponse::progress(&action_id, "in_progress", progress)
                .set_sequence(sequence);
            if let Err(e) = tx.send_async(Event::ActionResponse(response)).await {
                error!("{e}");
                break;
            }

            interval.tick().await;
        }

        sequence += 1;
        let response =
            ActionResponse::progress(&action_id, "Completed", 100).set_sequence(sequence);
        if let Err(e) = tx.send_async(Event::ActionResponse(response)).await {
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

pub fn spawn_data_simulators(device: DeviceData, tx: Sender<Event>) {
    spawn(Gps::simulate(tx.clone(), device));
    spawn(Bms::simulate(tx.clone()));
    spawn(Imu::simulate(tx.clone()));
    spawn(Motor::simulate(tx.clone()));
    spawn(PeripheralState::simulate(tx.clone()));
    spawn(DeviceShadow::simulate(tx));
}

#[tokio::main(flavor = "current_thread")]
pub async fn start(
    config: SimulatorConfig,
    bridge_tx: BridgeTx,
    actions_rx: Option<Receiver<Action>>,
) -> Result<(), Error> {
    let path = read_gps_path(&config.gps_paths);
    let device = new_device_data(path);

    let (tx, rx) = bounded(10);
    spawn_data_simulators(device, tx.clone());

    loop {
        select! {
            action = actions_rx.as_ref().unwrap().recv_async(), if actions_rx.is_some() => {
                let action = action?;
                spawn(ActionResponse::simulate(action,  tx.clone()));
            }
            event = rx.recv_async() => {
                match event {
                    Ok(Event::ActionResponse(status)) => bridge_tx.send_action_response(status).await,
                    Ok(Event::Data(payload)) => bridge_tx.send_payload(payload).await,
                    Err(_) => {
                        error!("All generators have stopped!");
                        return Ok(())
                    }
                };
            }
        }
    }
}
